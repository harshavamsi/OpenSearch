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
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * A streaming aggregator that computes approximate counts of unique values.
 * Uses per-segment {@link CardinalityAggregator.DeferredOrdinalsCollector} with
 * segment-local ordinals, materializing HLL before each segment flush.
 *
 * @opensearch.internal
 */
public class StreamCardinalityAggregator extends CardinalityAggregator {

    private Collector streamCollector;
    private SegmentDeferredCollector segmentCollector;

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
        // Materialize and clean up previous segment's collector
        materializeAndCleanup();

        if (valuesSource == null) {
            emptyCollectorsUsed++;
            streamCollector = new EmptyCollector();
            return streamCollector;
        }

        if (!(valuesSource instanceof ValuesSource.Bytes.WithOrdinals)) {
            throw new IllegalStateException("StreamCardinalityAggregator only supports ordinal value sources");
        }

        // Create per-segment collector with segment-local ordinals
        ValuesSource.Bytes.WithOrdinals source = (ValuesSource.Bytes.WithOrdinals) valuesSource;
        SortedSetDocValues segmentOrds = source.ordinalsValues(ctx);
        segmentCollector = new SegmentDeferredCollector(segmentOrds, context);
        streamCollector = segmentCollector;
        deferredOrdinalsCollectorsUsed++;
        return streamCollector;
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (segmentCollector != null) {
            return segmentCollector.ordinalCardinality(owningBucketOrd);
        }
        return counts == null ? 0 : counts.cardinality(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        // Materialize current segment before building
        materializeAndCleanup();
        // Use the base class path which reads from counts (now populated)
        if (counts == null || owningBucketOrdinal >= counts.maxOrd() || counts.cardinality(owningBucketOrdinal) == 0) {
            return buildEmptyAggregation();
        }
        AbstractHyperLogLogPlusPlus copy = counts.clone(owningBucketOrdinal, org.opensearch.common.util.BigArrays.NON_RECYCLING_INSTANCE);
        return new InternalCardinality(name, copy, metadata());
    }

    @Override
    public void doReset() {
        super.doReset();
        materializeAndCleanup();
        if (counts != null) {
            counts.close();
            counts = valuesSource == null ? null : new HyperLogLogPlusPlus(precision, context.bigArrays(), 1);
        }
    }

    @Override
    protected void doPostCollection() throws IOException {
        materializeAndCleanup();
    }

    @Override
    protected void doClose() {
        super.doClose();
        if (segmentCollector != null) {
            segmentCollector.close();
            segmentCollector = null;
        }
    }

    private void materializeAndCleanup() {
        if (segmentCollector != null) {
            try {
                segmentCollector.materializeIntoHLL(counts);
            } catch (IOException e) {
                throw new RuntimeException("Failed to materialize HLL", e);
            } finally {
                segmentCollector.close();
                segmentCollector = null;
                streamCollector = null;
            }
        }
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
    }

    /**
     * Per-segment collector using segment-local ordinals and Roaring bitmaps.
     * Materializes into the shared HLL before the segment is flushed.
     */
    private static class SegmentDeferredCollector extends Collector {
        private final SortedSetDocValues values;
        private final SearchContext context;
        private org.opensearch.common.util.ObjectArray<org.roaringbitmap.RoaringBitmap> visitedOrds;
        private long cachedBucket = -1;
        private org.roaringbitmap.RoaringBitmap cachedBitmap;

        SegmentDeferredCollector(SortedSetDocValues values, SearchContext context) {
            this.values = values;
            this.context = context;
            this.visitedOrds = context.bigArrays().newObjectArray(1);
        }

        @Override
        public void collect(int doc, long bucketOrd) throws IOException {
            if (values.advanceExact(doc) == false) return;
            if (bucketOrd != cachedBucket) {
                visitedOrds = context.bigArrays().grow(visitedOrds, bucketOrd + 1);
                cachedBitmap = visitedOrds.get(bucketOrd);
                if (cachedBitmap == null) {
                    cachedBitmap = new org.roaringbitmap.RoaringBitmap();
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
            // no-op — materialization happens in materializeIntoHLL
        }

        @Override
        public void close() {
            org.opensearch.common.lease.Releasables.close(visitedOrds);
            visitedOrds = null;
            cachedBitmap = null;
            cachedBucket = -1;
        }

        long ordinalCardinality(long bucketOrd) {
            if (visitedOrds == null || bucketOrd >= visitedOrds.size()) return 0;
            org.roaringbitmap.RoaringBitmap bitmap = visitedOrds.get(bucketOrd);
            return bitmap == null ? 0 : bitmap.getLongCardinality();
        }

        /**
         * Hash segment ordinals into the shared HLL using segment-local lookupOrd.
         */
        void materializeIntoHLL(HyperLogLogPlusPlus counts) throws IOException {
            if (visitedOrds == null) return;
            final org.opensearch.common.hash.MurmurHash3.Hash128 hash = new org.opensearch.common.hash.MurmurHash3.Hash128();
            for (long bucket = visitedOrds.size() - 1; bucket >= 0; --bucket) {
                org.roaringbitmap.RoaringBitmap bitmap = visitedOrds.get(bucket);
                if (bitmap == null) continue;
                org.roaringbitmap.PeekableIntIterator it = bitmap.getIntIterator();
                while (it.hasNext()) {
                    final org.apache.lucene.util.BytesRef value = values.lookupOrd(it.next());
                    org.opensearch.common.hash.MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, hash);
                    counts.collect(bucket, hash.h1);
                }
            }
        }
    }
}
