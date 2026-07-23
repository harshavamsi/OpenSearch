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
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.ObjectArray;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;

import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;

/**
 * Streaming cardinality aggregator using per-segment Roaring bitmaps with segment-local ordinals.
 * Each segment is an independent unit: collect ordinals, rank by ordinal cardinality, materialize
 * HLL only for top-N buckets in buildAggregation, then reset for the next segment.
 *
 * @opensearch.internal
 */
public class StreamCardinalityAggregator extends CardinalityAggregator {

    private SegmentCollector segmentCollector;

    public StreamCardinalityAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        int precision,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata,
        CardinalityAggregatorFactory.ExecutionMode executionMode
    ) throws IOException {
        super(name, valuesSourceConfig, precision, context, parent, metadata, executionMode);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        if (segmentCollector != null) {
            segmentCollector.close();
            segmentCollector = null;
        }

        if (valuesSource == null) {
            emptyCollectorsUsed++;
            return new EmptyCollector();
        }

        if (valuesSource instanceof ValuesSource.Bytes.WithOrdinals == false) {
            throw new IllegalStateException("StreamCardinalityAggregator only supports ordinal value sources");
        }

        ValuesSource.Bytes.WithOrdinals source = (ValuesSource.Bytes.WithOrdinals) valuesSource;
        SortedSetDocValues segmentOrds = source.ordinalsValues(ctx);
        segmentCollector = new SegmentCollector(segmentOrds, context.bigArrays());
        deferredOrdinalsCollectorsUsed++;
        return segmentCollector;
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (segmentCollector != null) {
            return segmentCollector.ordinalCardinality(owningBucketOrd);
        }
        return 0;
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        if (segmentCollector == null) {
            return buildEmptyAggregation();
        }
        if (segmentCollector.ordinalCardinality(owningBucketOrdinal) == 0) {
            return buildEmptyAggregation();
        }
        // Create single-bucket HLL, hash ordinals on-the-fly from segment values
        try (HyperLogLogPlusPlus singleHLL = new HyperLogLogPlusPlus(precision, context.bigArrays(), 1)) {
            segmentCollector.materializeBucket(singleHLL, 0, owningBucketOrdinal);
            AbstractHyperLogLogPlusPlus copy = singleHLL.clone(0, BigArrays.NON_RECYCLING_INSTANCE);
            return new InternalCardinality(name, copy, metadata());
        }
    }

    @Override
    public void doReset() {
        super.doReset();
        if (segmentCollector != null) {
            segmentCollector.close();
            segmentCollector = null;
        }
    }

    @Override
    protected void doPostCollection() {
        // no-op — bitmaps stay alive for buildAggregation
    }

    @Override
    protected void doClose() {
        super.doClose();
        if (segmentCollector != null) {
            segmentCollector.close();
            segmentCollector = null;
        }
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
    }

    /**
     * Per-segment collector: stores ordinals in Roaring bitmaps, materializes HLL per bucket on demand.
     */
    static class SegmentCollector extends Collector {
        private final SortedSetDocValues values;
        private ObjectArray<RoaringBitmap> visitedOrds;
        private final BigArrays bigArrays;
        private long cachedBucket = -1;
        private RoaringBitmap cachedBitmap;

        SegmentCollector(SortedSetDocValues values, BigArrays bigArrays) {
            this.values = values;
            this.bigArrays = bigArrays;
            this.visitedOrds = bigArrays.newObjectArray(1);
        }

        @Override
        public void collect(int doc, long bucketOrd) throws IOException {
            if (values.advanceExact(doc) == false) return;
            if (bucketOrd != cachedBucket) {
                visitedOrds = bigArrays.grow(visitedOrds, bucketOrd + 1);
                cachedBitmap = visitedOrds.get(bucketOrd);
                if (cachedBitmap == null) {
                    cachedBitmap = new RoaringBitmap();
                    visitedOrds.set(bucketOrd, cachedBitmap);
                }
                cachedBucket = bucketOrd;
            }
            int count = values.docValueCount();
            long ord;
            while ((count-- > 0) && (ord = values.nextOrd()) != SortedSetDocValues.NO_MORE_DOCS) {
                cachedBitmap.add((int) ord);
            }
        }

        @Override
        public void postCollect() {
            // no-op
        }

        @Override
        public void close() {
            Releasables.close(visitedOrds);
            visitedOrds = null;
            cachedBitmap = null;
            cachedBucket = -1;
        }

        long ordinalCardinality(long bucketOrd) {
            if (visitedOrds == null || bucketOrd >= visitedOrds.size()) return 0;
            RoaringBitmap bitmap = visitedOrds.get(bucketOrd);
            return bitmap == null ? 0 : bitmap.getLongCardinality();
        }

        /**
         * Hash one bucket's ordinals into the target HLL at targetBucket.
         */
        void materializeBucket(HyperLogLogPlusPlus targetHLL, long targetBucket, long sourceBucket) throws IOException {
            if (visitedOrds == null || sourceBucket >= visitedOrds.size()) return;
            RoaringBitmap bitmap = visitedOrds.get(sourceBucket);
            if (bitmap == null) return;
            final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
            PeekableIntIterator it = bitmap.getIntIterator();
            while (it.hasNext()) {
                final BytesRef value = values.lookupOrd(it.next());
                MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, hash);
                targetHLL.collect(targetBucket, hash.h1);
            }
        }
    }
}
