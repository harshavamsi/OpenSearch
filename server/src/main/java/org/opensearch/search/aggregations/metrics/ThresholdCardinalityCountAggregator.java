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
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.LongArray;
import org.opensearch.common.util.ObjectArray;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;

/**
 * Two-pass BFS aggregator:
 * Pass 1: count docs per group, record (doc, groupOrd, parentBucket) for replay.
 * Pass 2 (postCollection): replay only groups with docCount > threshold, collecting count ordinals.
 *
 * @opensearch.internal
 */
public class ThresholdCardinalityCountAggregator extends MetricsAggregator {

    private final ValuesSource.Bytes.WithOrdinals groupSource;
    private final ValuesSource.Bytes.WithOrdinals countSource;
    private final int threshold;
    private final int minDocCount;
    private final int minBorderlineCount;
    private final int precision;
    private final BigArrays bigArrays;

    // Pass 1: doc count per group — single flat array indexed by (parentBucket * maxGroupOrd + groupOrd)
    private LongArray docCounts;
    private long maxGroupOrd;

    // Pass 1: recorded entries for replay
    private final List<SegmentEntry> segmentEntries = new ArrayList<>();

    // Pass 2 results per parent bucket
    private ObjectArray<GroupTracker> trackers;

    // Eligible groups (docCount > threshold)
    private ObjectArray<RoaringBitmap> eligibleGroups; // parentBucket → eligible group ords

    private boolean collected = false;

    ThresholdCardinalityCountAggregator(
        String name,
        ValuesSource.Bytes.WithOrdinals groupSource,
        ValuesSource.Bytes.WithOrdinals countSource,
        int threshold,
        int minDocCount,
        int minBorderlineCount,
        int precision,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        this.groupSource = groupSource;
        this.countSource = countSource;
        this.threshold = threshold;
        this.minDocCount = minDocCount;
        this.minBorderlineCount = minBorderlineCount;
        this.precision = precision;
        this.bigArrays = context.bigArrays();
        this.docCounts = bigArrays.newLongArray(1, true);
        this.maxGroupOrd = 0;
        this.trackers = bigArrays.newObjectArray(1);
        this.eligibleGroups = bigArrays.newObjectArray(1);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        final SortedSetDocValues groupOrds = groupSource.globalOrdinalsValues(ctx);
        final long groupOrdCount = groupOrds.getValueCount();

        // Pre-size docCounts if needed (parentBuckets are small, e.g. 3 platforms)
        // We'll grow lazily for parentBucket dimension but pre-size for groupOrd dimension
        if (groupOrdCount > maxGroupOrd) {
            maxGroupOrd = groupOrdCount;
        }

        // Record this segment for replay
        final PackedLongValues.Builder docDeltaBuilder = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
        final PackedLongValues.Builder groupOrdBuilder = PackedLongValues.packedBuilder(PackedInts.COMPACT);
        final PackedLongValues.Builder parentBucketBuilder = PackedLongValues.packedBuilder(PackedInts.COMPACT);
        final SegmentEntry entry = new SegmentEntry(ctx, docDeltaBuilder, groupOrdBuilder, parentBucketBuilder);
        segmentEntries.add(entry);

        return new LeafBucketCollector() {
            int lastDoc = 0;

            @Override
            public void collect(int doc, long parentBucket) throws IOException {
                if (groupOrds.advanceExact(doc) == false) return;
                long groupOrd = groupOrds.nextOrd();
                if (groupOrd == SortedSetDocValues.NO_MORE_DOCS) return;

                docDeltaBuilder.add(doc - lastDoc);
                groupOrdBuilder.add(groupOrd);
                parentBucketBuilder.add(parentBucket);
                lastDoc = doc;
                entry.count++;

                long flatIdx = parentBucket * maxGroupOrd + groupOrd;
                docCounts = bigArrays.grow(docCounts, flatIdx + 1);
                docCounts.increment(flatIdx, 1);
            }
        };
    }

    @Override
    public void postCollection() throws IOException {
        if (collected) return;
        collected = true;
        // Determine eligible groups (docCount > threshold)
        long numParentBuckets = (docCounts.size() + maxGroupOrd - 1) / maxGroupOrd;
        for (long pb = 0; pb < numParentBuckets; pb++) {
            eligibleGroups = bigArrays.grow(eligibleGroups, pb + 1);
            RoaringBitmap eligible = new RoaringBitmap();
            eligibleGroups.set(pb, eligible);

            for (long g = 0; g < maxGroupOrd; g++) {
                long flatIdx = pb * maxGroupOrd + g;
                if (flatIdx < docCounts.size() && docCounts.get(flatIdx) >= minDocCount) {
                    eligible.add((int) g);
                }
            }
        }

        // Free doc counts
        Releasables.close(docCounts);
        docCounts = null;

        // Replay: only collect count ordinals for eligible groups
        for (SegmentEntry entry : segmentEntries) {
            if (entry.count == 0) continue;

            SortedSetDocValues countOrds = countSource.globalOrdinalsValues(entry.ctx);
            PackedLongValues docDeltas = entry.docDeltaBuilder.build();
            PackedLongValues groupOrds = entry.groupOrdBuilder.build();
            PackedLongValues parentBuckets = entry.parentBucketBuilder.build();

            PackedLongValues.Iterator docIt = docDeltas.iterator();
            PackedLongValues.Iterator groupIt = groupOrds.iterator();
            PackedLongValues.Iterator parentIt = parentBuckets.iterator();

            int doc = 0;
            for (long i = 0; i < entry.count; i++) {
                doc += (int) docIt.next();
                long groupOrd = groupIt.next();
                long parentBucket = parentIt.next();

                RoaringBitmap eligible = (parentBucket < eligibleGroups.size()) ? eligibleGroups.get(parentBucket) : null;
                if (eligible == null || eligible.contains((int) groupOrd) == false) continue;

                trackers = bigArrays.grow(trackers, parentBucket + 1);
                GroupTracker tracker = trackers.get(parentBucket);
                if (tracker == null) {
                    tracker = new GroupTracker(bigArrays, threshold);
                    trackers.set(parentBucket, tracker);
                }
                if (tracker.hasPassed(groupOrd)) continue;

                if (countOrds.advanceExact(doc)) {
                    int countCount = countOrds.docValueCount();
                    boolean exceeded = false;
                    for (int c = 0; c < countCount; c++) {
                        exceeded = tracker.addCountOrdAndCheck(groupOrd, (int) countOrds.nextOrd());
                        if (exceeded) break;
                    }
                    if (exceeded) tracker.markPassed(groupOrd);
                }
            }
        }

        // Free replay data
        segmentEntries.clear();
        collected = true;
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrd) throws IOException {
        if (collected == false) postCollection();

        if (owningBucketOrd >= trackers.size()) return buildEmptyAggregation();
        GroupTracker tracker = trackers.get(owningBucketOrd);
        if (tracker == null) return buildEmptyAggregation();

        SortedSetDocValues groupGlobalOrds = groupSource.globalOrdinalsValues(context.searcher().getIndexReader().leaves().get(0));
        SortedSetDocValues countGlobalOrds = countSource.globalOrdinalsValues(context.searcher().getIndexReader().leaves().get(0));
        MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();

        try (HyperLogLogPlusPlus singleHLL = new HyperLogLogPlusPlus(precision, bigArrays, 1)) {
            PeekableIntIterator passedIt = tracker.getPassedGroups().getIntIterator();
            while (passedIt.hasNext()) {
                BytesRef groupValue = groupGlobalOrds.lookupOrd(passedIt.next());
                MurmurHash3.hash128(groupValue.bytes, groupValue.offset, groupValue.length, 0, hash);
                singleHLL.collect(0, hash.h1);
            }

            AbstractHyperLogLogPlusPlus passedCopy = singleHLL.cardinality(0) > 0
                ? singleHLL.clone(0, BigArrays.NON_RECYCLING_INSTANCE)
                : null;

            Map<Long, Set<Long>> borderline = new HashMap<>();
            tracker.forEachBorderline((groupOrd, countBitmap) -> {
                if (countBitmap.getCardinality() < minBorderlineCount) return; // skip low-count borderline
                BytesRef groupValue = groupGlobalOrds.lookupOrd(groupOrd);
                MurmurHash3.hash128(groupValue.bytes, groupValue.offset, groupValue.length, 0, hash);
                long groupHash = hash.h1;
                Set<Long> countHashes = new HashSet<>();
                PeekableIntIterator countIt = countBitmap.getIntIterator();
                while (countIt.hasNext()) {
                    BytesRef countValue = countGlobalOrds.lookupOrd(countIt.next());
                    MurmurHash3.hash128(countValue.bytes, countValue.offset, countValue.length, 0, hash);
                    countHashes.add(hash.h1);
                }
                borderline.put(groupHash, countHashes);
            });

            return new InternalThresholdCardinalityCount(name, passedCopy, borderline, threshold, precision, metadata());
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalThresholdCardinalityCount(name, null, new HashMap<>(), threshold, precision, metadata());
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        add.accept("threshold", threshold);
        add.accept("min_doc_count", minDocCount);
        add.accept("min_borderline_count", minBorderlineCount);
        long totalPassed = 0;
        long totalBorderlineSent = 0;
        long totalBorderlineSkipped = 0;
        long totalEligible = 0;
        if (trackers != null) {
            for (long i = 0; i < trackers.size(); i++) {
                GroupTracker t = trackers.get(i);
                if (t != null) {
                    totalPassed += t.getPassedGroups().getCardinality();
                    long[] counts = t.getBorderlineCounts(minBorderlineCount);
                    totalBorderlineSent += counts[0];
                    totalBorderlineSkipped += counts[1];
                }
            }
        }
        if (eligibleGroups != null) {
            for (long i = 0; i < eligibleGroups.size(); i++) {
                RoaringBitmap e = eligibleGroups.get(i);
                if (e != null) totalEligible += e.getLongCardinality();
            }
        }
        add.accept("groups_passed", totalPassed);
        add.accept("groups_borderline_sent", totalBorderlineSent);
        add.accept("groups_borderline_skipped", totalBorderlineSkipped);
        add.accept("groups_eligible", totalEligible);
    }

    @Override
    protected void doClose() {
        if (trackers != null) {
            for (long i = 0; i < trackers.size(); i++) {
                GroupTracker t = trackers.get(i);
                if (t != null) t.close();
            }
        }
        Releasables.close(docCounts, trackers, eligibleGroups);
    }

    static class SegmentEntry {
        final LeafReaderContext ctx;
        final PackedLongValues.Builder docDeltaBuilder;
        final PackedLongValues.Builder groupOrdBuilder;
        final PackedLongValues.Builder parentBucketBuilder;
        long count;

        SegmentEntry(
            LeafReaderContext ctx,
            PackedLongValues.Builder docDeltaBuilder,
            PackedLongValues.Builder groupOrdBuilder,
            PackedLongValues.Builder parentBucketBuilder
        ) {
            this.ctx = ctx;
            this.docDeltaBuilder = docDeltaBuilder;
            this.groupOrdBuilder = groupOrdBuilder;
            this.parentBucketBuilder = parentBucketBuilder;
        }
    }

    static class GroupTracker implements Releasable {
        private final BigArrays bigArrays;
        private final int threshold;
        private ObjectArray<RoaringBitmap> countBitmaps;
        private RoaringBitmap passedGroups;
        private long cachedGroupOrd = -1;
        private RoaringBitmap cachedBitmap;

        GroupTracker(BigArrays bigArrays, int threshold) {
            this.bigArrays = bigArrays;
            this.threshold = threshold;
            this.countBitmaps = bigArrays.newObjectArray(1);
            this.passedGroups = new RoaringBitmap();
        }

        boolean hasPassed(long groupOrd) {
            return passedGroups.contains((int) groupOrd);
        }

        boolean addCountOrdAndCheck(long groupOrd, int countOrd) {
            if (groupOrd != cachedGroupOrd) {
                countBitmaps = bigArrays.grow(countBitmaps, groupOrd + 1);
                cachedBitmap = countBitmaps.get(groupOrd);
                if (cachedBitmap == null) {
                    cachedBitmap = new RoaringBitmap();
                    countBitmaps.set(groupOrd, cachedBitmap);
                }
                cachedGroupOrd = groupOrd;
            }
            cachedBitmap.add(countOrd);
            return cachedBitmap.getCardinality() > threshold;
        }

        void markPassed(long groupOrd) {
            passedGroups.add((int) groupOrd);
            if (groupOrd < countBitmaps.size()) countBitmaps.set(groupOrd, null);
            if (groupOrd == cachedGroupOrd) {
                cachedBitmap = null;
                cachedGroupOrd = -1;
            }
        }

        RoaringBitmap getPassedGroups() {
            return passedGroups;
        }

        long getBorderlineCount() {
            long c = 0;
            for (long i = 0; i < countBitmaps.size(); i++) {
                if (countBitmaps.get(i) != null && passedGroups.contains((int) i) == false) c++;
            }
            return c;
        }

        /**
         * Returns [sent, skipped] counts based on minBorderlineCount filter.
         */
        long[] getBorderlineCounts(int minBorderlineCount) {
            long sent = 0;
            long skipped = 0;
            for (long i = 0; i < countBitmaps.size(); i++) {
                RoaringBitmap bm = countBitmaps.get(i);
                if (bm != null && bm.isEmpty() == false && passedGroups.contains((int) i) == false) {
                    if (bm.getCardinality() >= minBorderlineCount) {
                        sent++;
                    } else {
                        skipped++;
                    }
                }
            }
            return new long[] { sent, skipped };
        }

        interface BorderlineConsumer {
            void accept(int groupOrd, RoaringBitmap countBitmap) throws IOException;
        }

        void forEachBorderline(BorderlineConsumer consumer) throws IOException {
            for (long i = 0; i < countBitmaps.size(); i++) {
                RoaringBitmap bm = countBitmaps.get(i);
                if (bm != null && bm.isEmpty() == false && passedGroups.contains((int) i) == false) {
                    consumer.accept((int) i, bm);
                }
            }
        }

        @Override
        public void close() {
            Releasables.close(countBitmaps);
            countBitmaps = null;
            passedGroups = null;
            cachedBitmap = null;
            cachedGroupOrd = -1;
        }
    }
}
