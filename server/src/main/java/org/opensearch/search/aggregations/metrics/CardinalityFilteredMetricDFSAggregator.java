/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.LongArray;
import org.opensearch.common.util.LongHash;
import org.opensearch.common.util.ObjectArray;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorBase;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;

/**
 * Single-pass (DFS) filtered_metric for terms + cardinality.
 * No BFS replay — collects doc counts and count ordinals in one pass.
 * Groups meeting min_doc_count get Roaring bitmaps. Groups exceeding
 * shard_pass_value are hashed into HLL and freed immediately.
 *
 * @opensearch.internal
 */
public class CardinalityFilteredMetricDFSAggregator extends AggregatorBase {

    private final ValuesSource.Bytes.WithOrdinals groupSource;
    private final ValuesSource.Bytes.WithOrdinals countSource;
    private final int threshold;
    private final int minDocCount;
    private final int shardPassValue;
    private final int minBorderlineCount;
    private final int precision;
    private final BigArrays bigArrays;

    // Shard-scoped hash-of-group-BytesRef → stable bucketOrd. Avoids building
    // a cross-segment OrdinalMap for the group field (typically high-card).
    // NOTE: countSource still uses globalOrdinalsValues — its ordinals are stored
    // in per-group Roaring bitmaps and compared across segments. Porting the
    // count dimension would require hashing per count doc. Kept as-is because
    // count fields are typically low-card (~10 distinct).
    private final LongHash bucketOrds;

    // Per parent bucket
    private ObjectArray<BucketState> states;

    CardinalityFilteredMetricDFSAggregator(
        String name,
        ValuesSource.Bytes.WithOrdinals groupSource,
        ValuesSource.Bytes.WithOrdinals countSource,
        int threshold,
        int minDocCount,
        int shardPassValue,
        int minBorderlineCount,
        int precision,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, AggregatorFactories.EMPTY, context, parent, CardinalityUpperBound.NONE, metadata);
        this.groupSource = groupSource;
        this.countSource = countSource;
        this.threshold = threshold;
        this.minDocCount = minDocCount;
        this.shardPassValue = shardPassValue;
        this.minBorderlineCount = minBorderlineCount;
        this.precision = precision;
        this.bigArrays = context.bigArrays();
        this.bucketOrds = new LongHash(1, bigArrays);
        this.states = bigArrays.newObjectArray(1);
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        final SortedSetDocValues groupOrds = groupSource.ordinalsValues(ctx);
        final SortedSetDocValues countOrds = countSource.globalOrdinalsValues(ctx);
        final int segOrdCount = Math.toIntExact(groupOrds.getValueCount());
        // Per-segment caches: segOrd → (bucketOrd, groupHash). -1 bucketOrd means unseen.
        final long[] segOrdToBucketOrd = new long[segOrdCount];
        final long[] segOrdToHash = new long[segOrdCount];
        Arrays.fill(segOrdToBucketOrd, -1L);
        final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();

        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long parentBucket) throws IOException {
                if (groupOrds.advanceExact(doc) == false) return;
                long segOrd = groupOrds.nextOrd();
                if (segOrd == SortedSetDocValues.NO_MORE_DOCS) return;

                long bucketOrd = segOrdToBucketOrd[(int) segOrd];
                long groupHash;
                if (bucketOrd == -1L) {
                    BytesRef gv = groupOrds.lookupOrd(segOrd);
                    MurmurHash3.hash128(gv.bytes, gv.offset, gv.length, 0, hash);
                    groupHash = hash.h1;
                    long added = bucketOrds.add(groupHash);
                    bucketOrd = added < 0 ? -1 - added : added;
                    segOrdToBucketOrd[(int) segOrd] = bucketOrd;
                    segOrdToHash[(int) segOrd] = groupHash;
                } else {
                    groupHash = segOrdToHash[(int) segOrd];
                }

                states = bigArrays.grow(states, parentBucket + 1);
                BucketState state = states.get(parentBucket);
                if (state == null) {
                    state = new BucketState(bigArrays, precision);
                    states.set(parentBucket, state);
                }

                if (state.hasPassed(bucketOrd)) return;

                long docCount = state.incrementDocCount(bucketOrd);

                // Always collect count ordinals — min_doc_count only gates eligibility tracking
                if (docCount == minDocCount) state.groupsEligible++;
                if (countOrds.advanceExact(doc)) {
                    int c = countOrds.docValueCount();
                    for (int i = 0; i < c; i++) {
                        state.addCountOrd(bucketOrd, (int) countOrds.nextOrd());
                    }

                    // Check if group just exceeded pass threshold
                    if (state.getDistinctCount(bucketOrd) > shardPassValue) {
                        state.markPassed(bucketOrd, groupHash);
                    }
                }
            }
        };
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
        for (int i = 0; i < owningBucketOrds.length; i++) {
            results[i] = buildSingle(owningBucketOrds[i]);
        }
        return results;
    }

    private InternalAggregation buildSingle(long owningBucketOrd) throws IOException {
        if (owningBucketOrd >= states.size()) return buildEmptyAggregation();
        BucketState state = states.get(owningBucketOrd);
        if (state == null) return buildEmptyAggregation();

        AbstractHyperLogLogPlusPlus passedCopy = state.clonePassedHLL();

        // Group identity comes from LongHash (already hashed at collect). Count dimension
        // is still keyed by global ord so we do resolve countGlobalOrds once for borderline.
        SortedSetDocValues countGlobalOrds = countSource.globalOrdinalsValues(context.searcher().getIndexReader().leaves().get(0));
        MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
        Map<Long, Object> borderline = new HashMap<>();

        state.forEachBorderline(minBorderlineCount, minDocCount, (bucketOrd, countBitmap) -> {
            long groupHash = bucketOrds.get(bucketOrd);
            Set<Long> countHashes = new HashSet<>();
            PeekableIntIterator it = countBitmap.getIntIterator();
            while (it.hasNext()) {
                BytesRef cv = countGlobalOrds.lookupOrd(it.next());
                MurmurHash3.hash128(cv.bytes, cv.offset, cv.length, 0, hash);
                countHashes.add(hash.h1);
            }
            borderline.put(groupHash, countHashes);
        });

        return new InternalFilteredMetric(name, passedCopy, borderline, threshold, precision, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalFilteredMetric(name, null, new HashMap<>(), threshold, precision, metadata());
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        add.accept("execution_hint", "dfs");
        add.accept("threshold", threshold);
        add.accept("min_doc_count", minDocCount);
        add.accept("shard_pass_value", shardPassValue);
        add.accept("min_borderline_count", minBorderlineCount);
        long totalGroups = 0, eligible = 0, passed = 0, borderlineSent = 0, borderlineDropped = 0;
        if (states != null) {
            for (long i = 0; i < states.size(); i++) {
                BucketState s = states.get(i);
                if (s != null) {
                    totalGroups += s.totalGroups;
                    eligible += s.groupsEligible;
                    passed += s.groupsPassed;
                    borderlineSent += s.groupsBorderlineSent;
                    borderlineDropped += s.groupsBorderlineDropped;
                }
            }
        }
        add.accept("total_groups", totalGroups);
        add.accept("groups_eligible", eligible);
        add.accept("groups_passed", passed);
        add.accept("groups_borderline_sent", borderlineSent);
        add.accept("groups_borderline_dropped", borderlineDropped);
    }

    @Override
    protected void doClose() {
        if (states != null) {
            for (long i = 0; i < states.size(); i++) {
                BucketState s = states.get(i);
                if (s != null) s.close();
            }
        }
        Releasables.close(states, bucketOrds);
    }

    /**
     * Per-parent-bucket state: doc counts, Roaring bitmaps, passed HLL.
     */
    static class BucketState implements Releasable {
        private final BigArrays bigArrays;
        private LongArray docCounts;
        private ObjectArray<RoaringBitmap> countBitmaps;
        private RoaringBitmap passedGroups;
        private HyperLogLogPlusPlus passedHLL;
        private long cachedGroupOrd = -1;
        private RoaringBitmap cachedBitmap;

        // Counters for debug
        long totalGroups;
        long groupsEligible;
        long groupsPassed;
        long groupsBorderlineSent;
        long groupsBorderlineDropped;

        BucketState(BigArrays bigArrays, int precision) {
            this.bigArrays = bigArrays;
            this.docCounts = bigArrays.newLongArray(1, true);
            this.countBitmaps = bigArrays.newObjectArray(1);
            this.passedGroups = new RoaringBitmap();
            this.passedHLL = new HyperLogLogPlusPlus(precision, bigArrays, 1);
        }

        boolean hasPassed(long g) {
            return passedGroups.contains((int) g);
        }

        long incrementDocCount(long g) {
            docCounts = bigArrays.grow(docCounts, g + 1);
            long count = docCounts.increment(g, 1);
            if (count == 1) totalGroups++;
            return count;
        }

        void addCountOrd(long g, int countOrd) {
            if (g != cachedGroupOrd) {
                countBitmaps = bigArrays.grow(countBitmaps, g + 1);
                cachedBitmap = countBitmaps.get(g);
                if (cachedBitmap == null) {
                    cachedBitmap = new RoaringBitmap();
                    countBitmaps.set(g, cachedBitmap);
                }
                cachedGroupOrd = g;
            }
            cachedBitmap.add(countOrd);
        }

        int getDistinctCount(long g) {
            return cachedGroupOrd == g && cachedBitmap != null ? cachedBitmap.getCardinality() : 0;
        }

        void markPassed(long g, long groupHash) {
            passedGroups.add((int) g);
            passedHLL.collect(0, groupHash);
            groupsPassed++;
            if (g < countBitmaps.size()) countBitmaps.set(g, null);
            if (g == cachedGroupOrd) {
                cachedBitmap = null;
                cachedGroupOrd = -1;
            }
        }

        AbstractHyperLogLogPlusPlus clonePassedHLL() {
            return passedHLL.cardinality(0) > 0 ? passedHLL.clone(0, BigArrays.NON_RECYCLING_INSTANCE) : null;
        }

        interface BorderlineConsumer {
            void accept(long bucketOrd, RoaringBitmap countBitmap) throws IOException;
        }

        void forEachBorderline(int minCount, int minDocCount, BorderlineConsumer consumer) throws IOException {
            for (long i = 0; i < countBitmaps.size(); i++) {
                RoaringBitmap bm = countBitmaps.get(i);
                if (bm != null && passedGroups.contains((int) i) == false) {
                    long dc = i < docCounts.size() ? docCounts.get(i) : 0;
                    if (dc >= minDocCount && bm.getCardinality() >= minCount) {
                        groupsBorderlineSent++;
                        consumer.accept(i, bm);
                    } else {
                        groupsBorderlineDropped++;
                    }
                }
            }
        }

        @Override
        public void close() {
            Releasables.close(docCounts, countBitmaps, passedHLL);
        }
    }
}
