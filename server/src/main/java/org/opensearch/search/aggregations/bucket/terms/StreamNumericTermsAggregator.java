/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.IntroSelector;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.Numbers;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.IntArray;
import org.opensearch.index.fielddata.FieldData;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalMultiBucketAggregation;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.bucket.LocalBucketCountThresholds;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.streaming.collection.BatchedLongTermsLeafCollector;
import org.opensearch.search.streaming.collection.ColumnSinkFactory;
import org.opensearch.search.streaming.collection.LongColumnSink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.opensearch.search.aggregations.InternalOrder.isKeyOrder;

/**
 * Aggregate all docs that contain numeric terms through streaming
 *
 * @opensearch.internal
 */
public class StreamNumericTermsAggregator extends TermsAggregator {
    private static final Logger logger = LogManager.getLogger(StreamNumericTermsAggregator.class);
    private final ResultStrategy<?, ?> resultStrategy;
    private final ValuesSource.Numeric valuesSource;
    private final IncludeExclude.LongFilter longFilter;
    private LongKeyedBucketOrds bucketOrds;
    private final CardinalityUpperBound cardinality;
    private final int segmentTopN;
    // Non-null while the current segment collects through the batched/columnar path (POC).
    // Flushed via finish() before buildAggregations so the tail batch is folded in.
    private BatchedLongTermsLeafCollector batchedCollector;

    public StreamNumericTermsAggregator(
        String name,
        AggregatorFactories factories,
        Function<StreamNumericTermsAggregator, ResultStrategy<?, ?>> resultStrategy,
        ValuesSource.Numeric valuesSource,
        DocValueFormat format,
        BucketOrder order,
        BucketCountThresholds bucketCountThresholds,
        SearchContext aggregationContext,
        Aggregator parent,
        SubAggCollectionMode subAggCollectMode,
        IncludeExclude.LongFilter longFilter,
        CardinalityUpperBound cardinality,
        int segmentTopN,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, aggregationContext, parent, bucketCountThresholds, order, format, subAggCollectMode, metadata);
        this.resultStrategy = resultStrategy.apply(this); // ResultStrategy needs a reference to the Aggregator to do its job.
        this.valuesSource = valuesSource;
        this.longFilter = longFilter;
        this.cardinality = cardinality;
        this.segmentTopN = segmentTopN;
    }

    @Override
    public void doReset() {
        super.doReset();
        Releasables.close(bucketOrds, batchedCollector);
        bucketOrds = null;
        batchedCollector = null;
    }

    // Streaming flushes buildAggregations() once per segment; the deferring collector's
    // prepareSelectedBuckets() can only be invoked once, so deferral breaks across flushes.
    @Override
    protected boolean shouldDefer(Aggregator aggregator) {
        return false;
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (bucketOrds != null) {
            bucketOrds.close();
        }
        bucketOrds = LongKeyedBucketOrds.build(context.bigArrays(), cardinality);
        SortedNumericDocValues values = resultStrategy.getValues(ctx);

        // Batched/columnar collection (POC): buffer docids, bulk-decode the group-by field via
        // NumericDocValues.longValues, fold each batch in one tight loop, and optionally
        // materialize the segment's key column into an Arrow sink. Root-level single-valued
        // unfiltered shapes only; everything else takes the classic per-doc collector below.
        if (batchedCollector != null) {
            Releasables.close(batchedCollector);
            batchedCollector = null;
        }
        // Gate: root-level, no include/exclude filter, and nothing downstream needs scores
        // (batching defers sub.collect() to flush time, when the scorer is positioned elsewhere).
        if (ColumnSinkFactory.isCollectionEnabled() && longFilter == null && parent() == null && scoreMode().needsScores() == false) {
            LongColumnSink sink = ColumnSinkFactory.newLongSink(name + "#" + ctx.ord, BatchedLongTermsLeafCollector.BATCH_SIZE);
            // Whether each doc counts exactly 1 (no _doc_count field): required for run-batched
            // doc-count increments below. Checked per segment.
            final boolean unitDocCounts = docCountProvider.alwaysOne();
            // Scratch for dispatching a run of docs that share one bucket to the sub-agg chain
            // via the batch entry point (one virtual call per run instead of per doc).
            final int[] runScratch = new int[BatchedLongTermsLeafCollector.BATCH_SIZE];
            BatchedLongTermsLeafCollector batched = BatchedLongTermsLeafCollector.tryCreate(values, sub, (docs, vals, count) -> {
                // Run-length grouping: docids arrive in index order, so on index-sorted or
                // low-cardinality fields equal keys cluster into runs. A run of N equal keys
                // costs 1 hash probe (ReorganizingLongHash is >50% of node CPU here), one
                // doc-count increment, and ONE batched sub-aggregator dispatch — sub-aggs
                // with bulk overrides (sum/avg/count/cardinality via longValues) then decode
                // the run's values in bulk instead of N megamorphic collect() calls.
                long lastVal = 0;
                long lastOrd = -1;
                int runStart = 0;
                for (int i = 0; i < count; i++) {
                    long val = vals[i];
                    if (lastOrd >= 0 && val == lastVal) {
                        continue;
                    }
                    if (lastOrd >= 0) {
                        flushRun(sub, docs, runStart, i - runStart, lastOrd, unitDocCounts, runScratch);
                    }
                    long bucketOrdinal = bucketOrds.add(0, val);
                    if (bucketOrdinal < 0) {
                        bucketOrdinal = -1 - bucketOrdinal;
                    } else {
                        grow(bucketOrdinal + 1);
                    }
                    lastVal = val;
                    lastOrd = bucketOrdinal;
                    runStart = i;
                }
                if (lastOrd >= 0) {
                    flushRun(sub, docs, runStart, count - runStart, lastOrd, unitDocCounts, runScratch);
                }
                // One breaker check per batch instead of one LongAdder increment per doc.
                checkBucketMemory();
            }, sink);
            if (batched != null) {
                batchedCollector = batched;
                return resultStrategy.wrapCollector(batched);
            } else if (sink != null) {
                sink.close();
            }
        }

        return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, values) {
            private int sinceMemoryCheck = 0;

            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (values.advanceExact(doc)) {
                    int valuesCount = values.docValueCount();
                    long previous = Long.MAX_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        long val = values.nextValue();
                        if (previous != val || i == 0) {
                            if ((longFilter == null) || (longFilter.accept(val))) {
                                long bucketOrdinal = bucketOrds.add(owningBucketOrd, val);
                                if (bucketOrdinal < 0) { // already seen
                                    bucketOrdinal = -1 - bucketOrdinal;
                                } else {
                                    grow(bucketOrdinal + 1);
                                }
                                collectExistingBucketQuiet(sub, doc, bucketOrdinal);
                                if (++sinceMemoryCheck >= 8192) {
                                    sinceMemoryCheck = 0;
                                    checkBucketMemory();
                                }
                            }
                            previous = val;
                        }
                    }
                }
            }
        });
    }

    /**
     * Dispatch a run of {@code len} docs (all in {@code bucketOrd}) to the sub-agg chain.
     * With unit doc counts the whole run folds via one increment + one batched sub dispatch;
     * otherwise fall back to per-doc accounting (honors _doc_count).
     */
    private void flushRun(LeafBucketCollector sub, int[] docs, int start, int len, long bucketOrd, boolean unitDocCounts, int[] scratch)
        throws IOException {
        if (unitDocCounts) {
            if (start == 0) {
                collectExistingBucketBatch(sub, docs, len, bucketOrd);
            } else {
                System.arraycopy(docs, start, scratch, 0, len);
                collectExistingBucketBatch(sub, scratch, len, bucketOrd);
            }
        } else {
            for (int i = start; i < start + len; i++) {
                collectExistingBucketQuiet(sub, docs[i], bucketOrd);
            }
        }
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        if (batchedCollector != null) {
            batchedCollector.finish();
        }
        return resultStrategy.buildAggregationsBatch(owningBucketOrds);
    }

    /**
     * Shard-side columnar emit gate: the Arrow transport will write columns, this is a root-level
     * agg (single owning ordinal 0), and every sub-agg is a
     * {@link org.opensearch.search.aggregations.metrics.ColumnarMetricSink}. Ineligible
     * shapes fall through to the object path. Key-type eligibility is decided per result strategy
     * (only LongTermsResults emits columnar).
     */
    private boolean columnarEmitEligible(long[] owningBucketOrds) {
        return ColumnSinkFactory.isArrowColumnarTransportEnabled()
            && parent() == null
            && owningBucketOrds.length == 1
            && owningBucketOrds[0] == 0
            && ColumnarTermsShardResult.subAggsEligible(subAggregators);
    }

    /**
     * Strategy for building results.
     */
    public abstract class ResultStrategy<R extends InternalAggregation, B extends InternalMultiBucketAggregation.InternalBucket>
        implements
            Releasable {
        protected IntArray reusableIndices;
        protected Aggregator.BucketComparator ordinalComparator;
        protected B tempBucket1;
        protected B tempBucket2;

        private InternalAggregation[] buildAggregationsBatch(long[] owningBucketOrds) throws IOException {
            if (bucketOrds == null) { // no data collected
                InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
                for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
                    results[ordIdx] = buildEmptyResult();
                }
                return results;
            }
            LocalBucketCountThresholds localBucketCountThresholds = context.asLocalBucketCountThresholds(bucketCountThresholds);
            B[][] topBucketsPerOrd = buildTopBucketsPerOrd(owningBucketOrds.length);
            long[] otherDocCount = new long[owningBucketOrds.length];

            for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
                checkCancelled();
                collectZeroDocEntriesIfNeeded(owningBucketOrds[ordIdx]);
                LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = bucketOrds.ordsEnum(owningBucketOrds[ordIdx]);
                long bucketsInOrd = bucketOrds.bucketsInOrd(owningBucketOrds[ordIdx]);
                logger.debug("Cardinality post collection for ordIdx {}: {}", ordIdx, bucketsInOrd);

                SelectionResult<B> selectionResult = selectTopBuckets(
                    ordsEnum,
                    bucketsInOrd,
                    segmentTopN,
                    bucketCountThresholds,
                    owningBucketOrds[ordIdx]
                );

                otherDocCount[ordIdx] = selectionResult.otherDocCount;
                topBucketsPerOrd[ordIdx] = buildBuckets(selectionResult.buckets.size());
                for (int i = 0; i < topBucketsPerOrd[ordIdx].length; i++) {
                    topBucketsPerOrd[ordIdx][i] = selectionResult.buckets.get(i);
                }
            }

            // Columnar emit: if the Arrow transport will write columns and every sub-agg is a
            // ColumnarMetricSink, build the emit-only carrier straight from ordinal-indexed metric
            // state instead of materializing per-bucket metric objects (which the writer would then
            // read back into vectors). Only LongTermsResults overrides buildColumnarResult; Double/
            // UnsignedLong return null and fall through to the object path unchanged.
            if (columnarEmitEligible(owningBucketOrds)) {
                InternalAggregation[] carriers = new InternalAggregation[owningBucketOrds.length];
                boolean allColumnar = true;
                for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
                    carriers[ordIdx] = buildColumnarResult(owningBucketOrds[ordIdx], otherDocCount[ordIdx], topBucketsPerOrd[ordIdx]);
                    if (carriers[ordIdx] == null) {
                        allColumnar = false;
                        break;
                    }
                }
                if (allColumnar) {
                    return carriers;
                }
            }

            buildSubAggs(topBucketsPerOrd);
            InternalAggregation[] result = new InternalAggregation[owningBucketOrds.length];
            for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
                result[ordIdx] = buildResult(owningBucketOrds[ordIdx], otherDocCount[ordIdx], topBucketsPerOrd[ordIdx]);
            }
            return result;
        }

        /**
         * Build the emit-only columnar carrier for one owning ordinal, or {@code null} if this
         * result type can't emit columnar (Double/UnsignedLong keys — the wire LONG column is a
         * true long). Default: not columnar-capable.
         */
        InternalAggregation buildColumnarResult(long owningBucketOrd, long otherDocCount, B[] topBuckets) throws IOException {
            return null;
        }

        private void prepareIndicesArray(long valueCount) {
            if (reusableIndices == null) {
                reusableIndices = context.bigArrays().newIntArray(valueCount, false);
            } else if (reusableIndices.size() < valueCount) {
                reusableIndices = context.bigArrays().grow(reusableIndices, valueCount);
            }
        }

        protected void ensureOrdinalComparator() {
            // Override in subclasses if needed
        }

        abstract B createTempBucket();

        private static class SelectionResult<B> {
            final List<B> buckets;
            final long otherDocCount;

            SelectionResult(List<B> buckets, long otherDocCount) {
                this.buckets = buckets;
                this.otherDocCount = otherDocCount;
            }
        }

        private SelectionResult<B> selectTopBuckets(
            LongKeyedBucketOrds.BucketOrdsEnum ordsEnum,
            long totalBuckets,
            int segmentSize,
            BucketCountThresholds thresholds,
            long owningBucketOrd
        ) throws IOException {
            prepareIndicesArray(totalBuckets);

            int candidateCount = 0;
            long totalDocCount = 0;
            while (ordsEnum.next()) {
                long docCount = StreamNumericTermsAggregator.this.bucketDocCount(ordsEnum.ord());
                totalDocCount += docCount;
                if (docCount >= thresholds.getMinDocCount()) {
                    reusableIndices.set(candidateCount++, (int) ordsEnum.ord());
                }
            }

            segmentSize = Math.min(segmentSize, candidateCount);

            if (candidateCount <= segmentSize) {
                ordsEnum = bucketOrds.ordsEnum(owningBucketOrd);
                List<B> result = new ArrayList<>(candidateCount);
                long selectedDocCount = 0;
                while (ordsEnum.next()) {
                    long docCount = StreamNumericTermsAggregator.this.bucketDocCount(ordsEnum.ord());
                    if (docCount >= thresholds.getMinDocCount()) {
                        result.add(buildFinalBucket(ordsEnum.ord(), ordsEnum.value(), docCount, owningBucketOrd));
                        selectedDocCount += docCount;
                    }
                }
                return new SelectionResult<>(result, totalDocCount - selectedDocCount);
            }

            ensureOrdinalComparator();

            IntroSelector selector = new IntroSelector() {
                int pivotOrdinal;

                @Override
                protected void swap(int i, int j) {
                    int temp = reusableIndices.get(i);
                    reusableIndices.set(i, reusableIndices.get(j));
                    reusableIndices.set(j, temp);
                }

                @Override
                protected void setPivot(int i) {
                    pivotOrdinal = reusableIndices.get(i);
                }

                @Override
                protected int comparePivot(int j) {
                    long leftOrd = reusableIndices.get(j);
                    long rightOrd = pivotOrdinal;
                    if (ordinalComparator != null) {
                        return -ordinalComparator.compare(leftOrd, rightOrd);
                    }
                    // Fallback to doc count for _count ordering
                    long leftDocCount = StreamNumericTermsAggregator.this.bucketDocCount(leftOrd);
                    long rightDocCount = StreamNumericTermsAggregator.this.bucketDocCount(rightOrd);
                    return Long.compare(leftDocCount, rightDocCount);
                }
            };

            selector.select(0, candidateCount, segmentSize);

            // Build result directly from selected ordinals (O(segmentSize) instead of O(totalBuckets * segmentSize))
            List<B> result = new ArrayList<>(segmentSize);
            long selectedDocCount = 0;
            for (int i = 0; i < segmentSize; i++) {
                int selectedOrd = reusableIndices.get(i);
                long value = bucketOrds.get(selectedOrd);
                long docCount = StreamNumericTermsAggregator.this.bucketDocCount(selectedOrd);
                result.add(buildFinalBucket(selectedOrd, value, docCount, owningBucketOrd));
                selectedDocCount += docCount;
            }

            return new SelectionResult<>(result, totalDocCount - selectedDocCount);
        }

        @Override
        public final void close() {
            Releasables.close(reusableIndices);
            reusableIndices = null;
        }

        /**
         * Short description of the collection mechanism added to the profile
         * output to help with debugging.
         */
        abstract String describe();

        /**
         * Resolve the doc values to collect results of this type.
         */
        abstract SortedNumericDocValues getValues(LeafReaderContext ctx) throws IOException;

        /**
         * Wrap the "standard" numeric terms collector to collect any more
         * information that this result type may need.
         */
        abstract LeafBucketCollector wrapCollector(LeafBucketCollector primary);

        /**
         * Build an array to hold the "top" buckets for each ordinal.
         */
        abstract B[][] buildTopBucketsPerOrd(int size);

        /**
         * Build an array of buckets for a particular ordinal. These arrays
         * are asigned to the value returned by {@link #buildTopBucketsPerOrd}.
         */
        abstract B[] buildBuckets(int size);

        /**
         * Build the sub-aggregations into the buckets. This will usually
         * delegate to {@link #buildSubAggsForAllBuckets}.
         */
        abstract void buildSubAggs(B[][] topBucketsPerOrd) throws IOException;

        /**
         * Collect extra entries for "zero" hit documents if they were requested
         * and required.
         */
        abstract void collectZeroDocEntriesIfNeeded(long owningBucketOrd) throws IOException;

        /**
         * Turn the buckets into an aggregation result.
         */
        abstract R buildResult(long owningBucketOrd, long otherDocCounts, B[] topBuckets);

        /**
         * Build an "empty" result. Only called if there isn't any data on this
         * shard.
         */
        abstract R buildEmptyResult();

        /**
         * Build a final bucket directly with the provided data, skipping temporary bucket creation.
         */
        abstract B buildFinalBucket(long ord, long value, long docCount, long owningBucketOrd) throws IOException;
    }

    abstract class StandardTermsResultStrategy<R extends InternalMappedTerms<R, B>, B extends InternalTerms.Bucket<B>> extends
        ResultStrategy<R, B> {
        protected final boolean showTermDocCountError;

        StandardTermsResultStrategy(boolean showTermDocCountError) {
            this.showTermDocCountError = showTermDocCountError;
        }

        @Override
        protected void ensureOrdinalComparator() {
            if (ordinalComparator == null) {
                if (isKeyOrder(order)) {
                    throw new IllegalArgumentException(
                        "Streaming aggregation does not support key-based ordering for numeric fields. "
                            + "Use traditional aggregation approach instead."
                    );
                } else if (partiallyBuiltBucketComparator != null) {
                    tempBucket1 = createTempBucket();
                    tempBucket2 = createTempBucket();
                    ordinalComparator = (leftOrd, rightOrd) -> {
                        tempBucket1.bucketOrd = leftOrd;
                        tempBucket1.docCount = StreamNumericTermsAggregator.this.bucketDocCount(leftOrd);
                        tempBucket2.bucketOrd = rightOrd;
                        tempBucket2.docCount = StreamNumericTermsAggregator.this.bucketDocCount(rightOrd);
                        return partiallyBuiltBucketComparator.compare(tempBucket1, tempBucket2);
                    };
                }
            }
        }

        @Override
        final LeafBucketCollector wrapCollector(LeafBucketCollector primary) {
            return primary;
        }

        @Override
        final void buildSubAggs(B[][] topBucketsPerOrd) throws IOException {
            buildSubAggsForAllBuckets(topBucketsPerOrd, b -> b.bucketOrd, (b, aggs) -> b.aggregations = aggs);
        }

        @Override
        final void collectZeroDocEntriesIfNeeded(long owningBucketOrd) throws IOException {
            if (bucketCountThresholds.getMinDocCount() != 0) {
                return;
            }
            if (InternalOrder.isCountDesc(order) && bucketOrds.bucketsInOrd(owningBucketOrd) >= bucketCountThresholds.getRequiredSize()) {
                return;
            }
            // we need to fill-in the blanks
            for (LeafReaderContext ctx : context.searcher().getTopReaderContext().leaves()) {
                SortedNumericDocValues values = getValues(ctx);
                for (int docId = 0; docId < ctx.reader().maxDoc(); ++docId) {
                    if (values.advanceExact(docId)) {
                        int valueCount = values.docValueCount();
                        for (int v = 0; v < valueCount; ++v) {
                            long value = values.nextValue();
                            if (longFilter == null || longFilter.accept(value)) {
                                bucketOrds.add(owningBucketOrd, value);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * LongTermsResults for numeric terms
     *
     * @opensearch.internal
     */
    public class LongTermsResults extends StandardTermsResultStrategy<LongTerms, LongTerms.Bucket> {
        public LongTermsResults(boolean showTermDocCountError) {
            super(showTermDocCountError);
        }

        @Override
        LongTerms.Bucket createTempBucket() {
            return new LongTerms.Bucket(0, 0, null, showTermDocCountError, 0, format) {
                @Override
                public int compareKey(LongTerms.Bucket other) {
                    // For tie-breaking when sub-aggregation values are equal, compare actual bucket values
                    // instead of ordinals. Ordinals are assigned dynamically and don't guarantee numeric order.
                    long thisValue = bucketOrds.get(this.bucketOrd);
                    long otherValue = bucketOrds.get(other.bucketOrd);
                    return Long.compare(thisValue, otherValue);
                }
            };
        }

        @Override
        String describe() {
            return "stream_long_terms";
        }

        @Override
        SortedNumericDocValues getValues(LeafReaderContext ctx) throws IOException {
            return valuesSource.longValues(ctx);
        }

        @Override
        LongTerms.Bucket[][] buildTopBucketsPerOrd(int size) {
            return new LongTerms.Bucket[size][];
        }

        @Override
        LongTerms.Bucket[] buildBuckets(int size) {
            return new LongTerms.Bucket[size];
        }

        @Override
        LongTerms buildResult(long owningBucketOrd, long otherDocCount, LongTerms.Bucket[] topBuckets) {
            final BucketOrder reduceOrder;
            if (isKeyOrder(order) == false) {
                reduceOrder = InternalOrder.key(true);
                Arrays.sort(topBuckets, reduceOrder.comparator());
            } else {
                reduceOrder = order;
            }
            return new LongTerms(
                name,
                reduceOrder,
                order,
                metadata(),
                format,
                bucketCountThresholds.getShardSize(),
                showTermDocCountError,
                otherDocCount,
                List.of(topBuckets),
                0,
                bucketCountThresholds
            );
        }

        @Override
        LongTerms buildEmptyResult() {
            return new LongTerms(
                name,
                order,
                order,
                metadata(),
                format,
                bucketCountThresholds.getShardSize(),
                showTermDocCountError,
                0,
                emptyList(),
                0,
                bucketCountThresholds
            );
        }

        @Override
        LongTerms.Bucket buildFinalBucket(long ord, long value, long docCount, long owningBucketOrd) {
            LongTerms.Bucket result = new LongTerms.Bucket(value, docCount, null, showTermDocCountError, 0, format);
            result.bucketOrd = ord;
            result.setDocCountError(0);
            return result;
        }

        @Override
        InternalAggregation buildColumnarResult(long owningBucketOrd, long otherDocCount, LongTerms.Bucket[] topBuckets)
            throws IOException {
            // Mirror buildResult's reduceOrder + sort so the emitted key column is in the order the
            // coordinator reader/folder expects (KEY_ASC unless the request order is key-based).
            final BucketOrder reduceOrder;
            if (isKeyOrder(order) == false) {
                reduceOrder = InternalOrder.key(true);
                Arrays.sort(topBuckets, reduceOrder.comparator());
            } else {
                reduceOrder = order;
            }
            int rowCount = topBuckets.length;
            long[] longKeys = new long[rowCount];
            long[] docCounts = new long[rowCount];
            long[] bucketErrors = showTermDocCountError ? new long[rowCount] : null;
            long[] bucketOrdsForMetrics = new long[rowCount];
            for (int i = 0; i < rowCount; i++) {
                LongTerms.Bucket b = topBuckets[i];
                longKeys[i] = b.term;
                docCounts[i] = b.docCount;
                bucketOrdsForMetrics[i] = b.bucketOrd;
                if (bucketErrors != null) {
                    bucketErrors[i] = b.getDocCountError();
                }
            }
            List<ColumnarTermsShardResult.MetricColumn> metricColumns = ColumnarTermsShardResult.buildMetricColumns(
                subAggregators,
                bucketOrdsForMetrics,
                rowCount
            );
            return new ColumnarTermsShardResult(
                name,
                metadata(),
                reduceOrder,
                order,
                bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(),
                format,
                bucketCountThresholds.getShardSize(),
                showTermDocCountError,
                otherDocCount,
                0L,
                longKeys,
                null,
                docCounts,
                bucketErrors,
                metricColumns,
                rowCount
            );
        }
    }

    /**
     * DoubleTermsResults for numeric terms
     *
     * @opensearch.internal
     */
    public class DoubleTermsResults extends StandardTermsResultStrategy<DoubleTerms, DoubleTerms.Bucket> {

        public DoubleTermsResults(boolean showTermDocCountError) {
            super(showTermDocCountError);
        }

        @Override
        DoubleTerms.Bucket createTempBucket() {
            return new DoubleTerms.Bucket(0.0, 0, null, showTermDocCountError, 0, format) {
                @Override
                public int compareKey(DoubleTerms.Bucket other) {
                    // For tie-breaking when sub-aggregation values are equal, compare actual bucket values
                    // instead of ordinals. Ordinals are assigned dynamically and don't guarantee numeric order.
                    long thisValue = bucketOrds.get(this.bucketOrd);
                    long otherValue = bucketOrds.get(other.bucketOrd);
                    return Double.compare(NumericUtils.sortableLongToDouble(thisValue), NumericUtils.sortableLongToDouble(otherValue));
                }
            };
        }

        @Override
        String describe() {
            return "stream_double_terms";
        }

        @Override
        SortedNumericDocValues getValues(LeafReaderContext ctx) throws IOException {
            return FieldData.toSortableLongBits(valuesSource.doubleValues(ctx));
        }

        @Override
        DoubleTerms.Bucket[][] buildTopBucketsPerOrd(int size) {
            return new DoubleTerms.Bucket[size][];
        }

        @Override
        DoubleTerms.Bucket[] buildBuckets(int size) {
            return new DoubleTerms.Bucket[size];
        }

        @Override
        DoubleTerms buildResult(long owningBucketOrd, long otherDocCount, DoubleTerms.Bucket[] topBuckets) {
            final BucketOrder reduceOrder;
            if (isKeyOrder(order) == false) {
                reduceOrder = InternalOrder.key(true);
                Arrays.sort(topBuckets, reduceOrder.comparator());
            } else {
                reduceOrder = order;
            }
            return new DoubleTerms(
                name,
                reduceOrder,
                order,
                metadata(),
                format,
                bucketCountThresholds.getShardSize(),
                showTermDocCountError,
                otherDocCount,
                List.of(topBuckets),
                0,
                bucketCountThresholds
            );
        }

        @Override
        DoubleTerms buildEmptyResult() {
            return new DoubleTerms(
                name,
                order,
                order,
                metadata(),
                format,
                bucketCountThresholds.getShardSize(),
                showTermDocCountError,
                0,
                emptyList(),
                0,
                bucketCountThresholds
            );
        }

        @Override
        DoubleTerms.Bucket buildFinalBucket(long ord, long value, long docCount, long owningBucketOrd) {
            DoubleTerms.Bucket result = new DoubleTerms.Bucket(
                NumericUtils.sortableLongToDouble(value),
                docCount,
                null,
                showTermDocCountError,
                0,
                format
            );
            result.bucketOrd = ord;
            result.setDocCountError(0);
            return result;
        }
    }

    /**
     * UnsignedLongTermsResults for numeric terms
     *
     * @opensearch.internal
     */
    public class UnsignedLongTermsResults extends StandardTermsResultStrategy<UnsignedLongTerms, UnsignedLongTerms.Bucket> {
        public UnsignedLongTermsResults(boolean showTermDocCountError) {
            super(showTermDocCountError);
        }

        @Override
        UnsignedLongTerms.Bucket createTempBucket() {
            return new UnsignedLongTerms.Bucket(Numbers.toUnsignedBigInteger(0), 0, null, showTermDocCountError, 0, format) {
                @Override
                public int compareKey(UnsignedLongTerms.Bucket other) {
                    // For tie-breaking when sub-aggregation values are equal, compare actual bucket values
                    // instead of ordinals. Ordinals are assigned dynamically and don't guarantee numeric order.
                    long thisValue = bucketOrds.get(this.bucketOrd);
                    long otherValue = bucketOrds.get(other.bucketOrd);
                    return Long.compareUnsigned(thisValue, otherValue);
                }
            };
        }

        @Override
        String describe() {
            return "stream_unsigned_long_terms";
        }

        @Override
        SortedNumericDocValues getValues(LeafReaderContext ctx) throws IOException {
            return valuesSource.longValues(ctx);
        }

        @Override
        UnsignedLongTerms.Bucket[][] buildTopBucketsPerOrd(int size) {
            return new UnsignedLongTerms.Bucket[size][];
        }

        @Override
        UnsignedLongTerms.Bucket[] buildBuckets(int size) {
            return new UnsignedLongTerms.Bucket[size];
        }

        @Override
        UnsignedLongTerms buildResult(long owningBucketOrd, long otherDocCount, UnsignedLongTerms.Bucket[] topBuckets) {
            final BucketOrder reduceOrder;
            if (isKeyOrder(order) == false) {
                reduceOrder = InternalOrder.key(true);
                Arrays.sort(topBuckets, reduceOrder.comparator());
            } else {
                reduceOrder = order;
            }
            return new UnsignedLongTerms(
                name,
                reduceOrder,
                order,
                metadata(),
                format,
                bucketCountThresholds.getShardSize(),
                showTermDocCountError,
                otherDocCount,
                List.of(topBuckets),
                0,
                bucketCountThresholds
            );
        }

        @Override
        UnsignedLongTerms buildEmptyResult() {
            return new UnsignedLongTerms(
                name,
                order,
                order,
                metadata(),
                format,
                bucketCountThresholds.getShardSize(),
                showTermDocCountError,
                0,
                emptyList(),
                0,
                bucketCountThresholds
            );
        }

        @Override
        UnsignedLongTerms.Bucket buildFinalBucket(long ord, long value, long docCount, long owningBucketOrd) {
            UnsignedLongTerms.Bucket result = new UnsignedLongTerms.Bucket(
                Numbers.toUnsignedBigInteger(value),
                docCount,
                null,
                showTermDocCountError,
                0,
                format
            );
            result.bucketOrd = ord;
            result.setDocCountError(0);
            return result;
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return resultStrategy.buildEmptyResult();
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        add.accept("result_strategy", resultStrategy.describe());
        add.accept("total_buckets", bucketOrds == null ? 0 : bucketOrds.size());
        if (batchedCollector != null) {
            add.accept("batched_collection", true);
            add.accept("bulk_batches", batchedCollector.bulkBatches());
            add.accept("sparse_batches", batchedCollector.sparseBatches());
            add.accept("direct_sink_batches", batchedCollector.directSinkBatches());
            add.accept("copy_sink_batches", batchedCollector.copySinkBatches());
        }
    }

    @Override
    public void doClose() {
        Releasables.close(super::doClose, bucketOrds, resultStrategy, batchedCollector);
    }
}
