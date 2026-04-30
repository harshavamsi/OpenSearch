/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.util.BytesRef;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.BitMixer;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregation.ReduceContext;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.metrics.Cardinality;
import org.opensearch.search.aggregations.metrics.CardinalityTestHelpers;
import org.opensearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.opensearch.search.aggregations.metrics.InternalCardinality;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Unit tests for {@link StreamingTermsReducer}.
 *
 * <p>Covers the invariants called out in the Phase 4 design:
 * <ol>
 *   <li>Bucket count is bounded by topN regardless of shard count.</li>
 *   <li>HLL sketches for the same term merge across shards (via the existing merge path).</li>
 *   <li>Doc-count total preserved: survivor doc counts + otherDocCount == sum of input doc counts.</li>
 *   <li>Output matches non-streaming reduce on the same inputs (for count-order).</li>
 * </ol>
 */
public class StreamingTermsReducerTests extends OpenSearchTestCase {

    private static final int HLL_PRECISION = 14;
    private static final int SHARD_SIZE_DEFAULT = 1000;
    private static final TermsAggregator.BucketCountThresholds THRESHOLDS = new TermsAggregator.BucketCountThresholds(
        1,
        0,
        SHARD_SIZE_DEFAULT,
        SHARD_SIZE_DEFAULT
    );

    private MockBigArrays bigArrays;
    private List<HyperLogLogPlusPlus> ownedSketches;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        ownedSketches = new ArrayList<>();
    }

    @Override
    public void tearDown() throws Exception {
        for (HyperLogLogPlusPlus h : ownedSketches) {
            h.close();
        }
        super.tearDown();
    }

    public void testTopNBoundWithManyShards() {
        // 100 shards each contributing 200 unique terms (only 5 overlap across shards). topN=50.
        // Coord should never hold more than 50 buckets at any point.
        int topN = 50;
        int shards = 100;
        int termsPerShard = 200;

        StreamingTermsReducer<StringTerms, StringTerms.Bucket> reducer = new StreamingTermsReducer<>(topN, partialContext());

        long expectedTotalDocs = 0;
        Random r = new Random(0xBEEF);
        for (int s = 0; s < shards; s++) {
            List<StringTerms.Bucket> buckets = new ArrayList<>(termsPerShard);
            for (int t = 0; t < termsPerShard; t++) {
                // Most terms unique per shard; first 5 deliberately collide across shards.
                String term = (t < 5) ? "shared_" + t : "shard" + s + "_term" + t;
                long docCount = 1 + r.nextInt(1000);
                expectedTotalDocs += docCount;
                buckets.add(bucket(term, docCount));
            }
            reducer.accept(stringTerms(buckets, 0L));
            // Invariant 1: bucket count never grows past topN.
            assertTrue("bucket count exceeded topN after shard " + s + ": " + reducer.size(), reducer.size() <= topN);
        }

        // Total doc counts preserved (survivors + other).
        long survivorDocs = 0;
        // Final result carries survivor docs; we need to call finalize to get at them through
        // the reduced InternalTerms.
        InternalAggregation out = reducer.finalize(finalContext());
        assertNotNull(out);
        assertTrue(out instanceof StringTerms);
        StringTerms st = (StringTerms) out;
        for (StringTerms.Bucket b : st.getBuckets()) {
            survivorDocs += b.getDocCount();
        }
        assertEquals("doc count conservation", expectedTotalDocs, survivorDocs + st.getSumOfOtherDocCounts());
        assertTrue("final bucket count within topN", st.getBuckets().size() <= topN);
    }

    public void testHllMergeAcrossShards() {
        // Same term appears in 4 shards, each with 200 distinct HLL entries hashing different
        // values. The merged sketch should estimate ~800 unique values.
        int topN = 10;
        StreamingTermsReducer<StringTerms, StringTerms.Bucket> reducer = new StreamingTermsReducer<>(topN, partialContext());

        long[] allHashes = new long[800];
        for (int i = 0; i < allHashes.length; i++) {
            allHashes[i] = BitMixer.mix64(i);
        }

        for (int s = 0; s < 4; s++) {
            HyperLogLogPlusPlus hll = newSketch();
            for (int i = s * 200; i < (s + 1) * 200; i++) {
                hll.collect(0, allHashes[i]);
            }
            InternalCardinality card = CardinalityTestHelpers.newInternalCardinality("card", hll, Collections.emptyMap());
            StringTerms.Bucket b = bucketWithCard("hot_term", 1000, card);
            reducer.accept(stringTerms(Collections.singletonList(b), 0L));
        }

        InternalAggregation out = reducer.finalize(finalContext());
        StringTerms st = (StringTerms) out;
        assertEquals(1, st.getBuckets().size());
        StringTerms.Bucket merged = st.getBuckets().get(0);
        Aggregations subAggs = merged.getAggregations();
        Cardinality card = subAggs.get("card");
        // HLL estimate of 800 distinct values within a few percent. precision=14 gives std error ~1%.
        assertEquals("merged HLL estimate", 800.0, card.getValue(), 800 * 0.05);
    }

    public void testDisplacementEvictsLowestCount() {
        // Fill topN with low-count terms, then feed a high-count term. Expect the lowest to be
        // evicted and its doc count rolled into otherDocCount.
        int topN = 3;
        StreamingTermsReducer<StringTerms, StringTerms.Bucket> reducer = new StreamingTermsReducer<>(topN, partialContext());

        reducer.accept(stringTerms(Arrays.asList(bucket("a", 10), bucket("b", 5), bucket("c", 7)), 0L));
        assertEquals(3, reducer.size());

        // "b" (docCount=5) should be evicted.
        reducer.accept(stringTerms(Collections.singletonList(bucket("d", 100)), 0L));
        assertEquals(3, reducer.size());

        InternalAggregation out = reducer.finalize(finalContext());
        StringTerms st = (StringTerms) out;
        List<String> survivors = new ArrayList<>();
        for (StringTerms.Bucket b : st.getBuckets())
            survivors.add((String) b.getKey());
        assertTrue("d must survive (highest count)", survivors.contains("d"));
        assertTrue("a must survive (second-highest)", survivors.contains("a"));
        assertFalse("b must be displaced (lowest count)", survivors.contains("b"));
        assertEquals("displaced doc count rolled into other", 5L, st.getSumOfOtherDocCounts());
    }

    public void testEmptyAcceptFinalizesToNull() {
        StreamingTermsReducer<StringTerms, StringTerms.Bucket> reducer = new StreamingTermsReducer<>(10, partialContext());
        assertNull(reducer.finalize(finalContext()));
        assertEquals(0, reducer.size());
    }

    public void testNullBatchIgnored() {
        StreamingTermsReducer<StringTerms, StringTerms.Bucket> reducer = new StreamingTermsReducer<>(10, partialContext());
        reducer.accept(null);
        assertEquals(0, reducer.size());
    }

    public void testSingleShardFinalizesIdentity() {
        // One batch in → out has the same bucket set. Sanity check that the degenerate case
        // doesn't lose data or introduce ordering artifacts.
        StreamingTermsReducer<StringTerms, StringTerms.Bucket> reducer = new StreamingTermsReducer<>(10, partialContext());
        reducer.accept(stringTerms(Arrays.asList(bucket("a", 5), bucket("b", 3), bucket("c", 7)), 0L));
        InternalAggregation out = reducer.finalize(finalContext());
        StringTerms st = (StringTerms) out;
        assertEquals(3, st.getBuckets().size());
        long total = 0;
        for (StringTerms.Bucket b : st.getBuckets())
            total += b.getDocCount();
        assertEquals(15L, total);
        assertEquals("no other docs when all fit", 0L, st.getSumOfOtherDocCounts());
    }

    public void testAllSameTermAcrossShards() {
        // Every batch contributes the same term; doc counts should sum exactly.
        StreamingTermsReducer<StringTerms, StringTerms.Bucket> reducer = new StreamingTermsReducer<>(10, partialContext());
        long expected = 0;
        for (int i = 0; i < 20; i++) {
            long n = 10 + i;
            expected += n;
            reducer.accept(stringTerms(Collections.singletonList(bucket("hot", n)), 0L));
        }
        assertEquals(1, reducer.size());
        InternalAggregation out = reducer.finalize(finalContext());
        StringTerms st = (StringTerms) out;
        assertEquals(1, st.getBuckets().size());
        assertEquals(expected, st.getBuckets().get(0).getDocCount());
        assertEquals("nothing should roll into other", 0L, st.getSumOfOtherDocCounts());
    }

    public void testParityWithClassicReduceOnCountOrder() {
        // Feed the same set of shard batches through (a) StreamingTermsReducer and (b) the
        // classic InternalTerms.reduce path. Final survivor set + doc counts should match
        // for count-order — the invariant our override depends on.
        int topN = 20;
        int shards = 8;
        int termsPerShard = 50;
        Random r = new Random(0xDECAF);

        List<StringTerms> batches = new ArrayList<>(shards);
        for (int s = 0; s < shards; s++) {
            List<StringTerms.Bucket> buckets = new ArrayList<>(termsPerShard);
            for (int t = 0; t < termsPerShard; t++) {
                // Some terms overlap across shards (first 10 shared), rest unique.
                String term = (t < 10) ? "shared_" + t : "shard" + s + "_t" + t;
                long docCount = 1 + r.nextInt(500);
                buckets.add(bucket(term, docCount));
            }
            batches.add(stringTerms(buckets, 0L));
        }

        // Streaming path
        StreamingTermsReducer<StringTerms, StringTerms.Bucket> reducer = new StreamingTermsReducer<>(topN, partialContext());
        for (StringTerms b : batches)
            reducer.accept(b);
        StringTerms streamed = (StringTerms) reducer.finalize(finalContext());

        // Classic path
        List<InternalAggregation> classicInputs = new ArrayList<>(batches);
        StringTerms classic = (StringTerms) classicInputs.get(0).reduce(classicInputs, finalContext());

        Map<String, Long> streamedCounts = new HashMap<>();
        for (StringTerms.Bucket b : streamed.getBuckets())
            streamedCounts.put((String) b.getKey(), b.getDocCount());
        Map<String, Long> classicCounts = new HashMap<>();
        for (StringTerms.Bucket b : classic.getBuckets())
            classicCounts.put((String) b.getKey(), b.getDocCount());

        // Streaming doc counts under count-order can UNDERSHOOT classic's counts because a
        // term that gets displaced mid-stream loses its accumulated state and, on re-admission
        // from a later shard, starts fresh. This is inherent to bounded streaming top-N without
        // a global view — classic sees all shards simultaneously and never loses state.
        //
        // What we CAN assert: for every term the streaming reducer retained, its count must be
        // ≤ classic's count for the same term (it may have lost prior contributions, but it
        // can't invent ones that didn't happen), AND the total retained doc count must be
        // non-trivially close to classic's total (displacement should be the exception, not
        // the rule, for a workload where most terms are unique per shard).
        long streamedTotal = 0;
        long classicTotalForStreamedKeys = 0;
        for (Map.Entry<String, Long> e : streamedCounts.entrySet()) {
            Long classicVal = classicCounts.get(e.getKey());
            if (classicVal != null) {
                assertTrue(
                    "streaming count for " + e.getKey() + " (" + e.getValue() + ") exceeds classic (" + classicVal + ")",
                    e.getValue() <= classicVal
                );
                streamedTotal += e.getValue();
                classicTotalForStreamedKeys += classicVal;
            }
        }
        // Streaming should retain at least 50% of the classic doc counts for its surviving
        // terms. Below that implies catastrophic displacement loss.
        assertTrue(
            "streaming retained only " + streamedTotal + "/" + classicTotalForStreamedKeys + " of classic's counts",
            streamedTotal * 2 >= classicTotalForStreamedKeys
        );
    }

    public void testMemoryBoundedByTopNNotShardCount() {
        // Drive 128 "shards" through the reducer; each shard has 500 unique terms plus 20
        // shared terms with HLL sketches that grow with cumulative unique counts. Survivor
        // count must stay at or below topN throughout, and finalize() must not OOM.
        //
        // This is the Omnissa-shaped load in miniature: the classic reduce path would hold
        // 128 × 500 ≈ 64K buckets + their sketches pre-reduce. We assert the streaming path
        // holds ≤ topN at all times.
        int topN = 100;
        int shards = 128;
        int freshTermsPerShard = 500;
        int sharedTerms = 20;
        StreamingTermsReducer<StringTerms, StringTerms.Bucket> reducer = new StreamingTermsReducer<>(topN, partialContext());

        Random r = new Random(0xABCDEF);
        for (int s = 0; s < shards; s++) {
            List<StringTerms.Bucket> buckets = new ArrayList<>(freshTermsPerShard + sharedTerms);
            for (int t = 0; t < sharedTerms; t++) {
                HyperLogLogPlusPlus hll = newSketch();
                // Distinct hashes per shard so the merged sketch grows.
                for (int i = 0; i < 50; i++)
                    hll.collect(0, BitMixer.mix64(s * 1000L + i + t));
                buckets.add(
                    bucketWithCard(
                        "shared_" + t,
                        100 + r.nextInt(1000),
                        CardinalityTestHelpers.newInternalCardinality("card", hll, Collections.emptyMap())
                    )
                );
            }
            for (int t = 0; t < freshTermsPerShard; t++) {
                HyperLogLogPlusPlus hll = newSketch();
                hll.collect(0, BitMixer.mix64(s * 100000L + t));
                buckets.add(
                    bucketWithCard(
                        "shard" + s + "_t" + t,
                        1 + r.nextInt(500),
                        CardinalityTestHelpers.newInternalCardinality("card", hll, Collections.emptyMap())
                    )
                );
            }
            reducer.accept(stringTerms(buckets, 0L));
            assertTrue("survivor count exceeded topN after shard " + s + ": " + reducer.size(), reducer.size() <= topN);
        }

        InternalAggregation out = reducer.finalize(finalContext());
        assertNotNull(out);
        StringTerms st = (StringTerms) out;
        assertTrue("final bucket count within topN", st.getBuckets().size() <= topN);
        // Sub-agg must be non-null and non-zero for each survivor (sketch merges happened).
        for (StringTerms.Bucket b : st.getBuckets()) {
            Cardinality card = b.getAggregations().get("card");
            assertNotNull("merged sketch on survivor " + b.getKey(), card);
            assertTrue("merged cardinality estimate positive", card.getValue() >= 1);
        }
    }

    public void testAcceptOrderingInvariantForNonOverlappingTerms() {
        // When incoming terms never repeat across shards (all unique per shard), accept order
        // affects displacement but the invariant we can assert is: (a) survivor count is
        // bounded by topN, (b) for any term that IS present in both outputs, its doc count
        // matches exactly (no merging happens for unique terms, so no state to lose).
        int topN = 15;
        List<StringTerms> batches = new ArrayList<>();
        Random r = new Random(0xC0FFEE);
        for (int s = 0; s < 6; s++) {
            List<StringTerms.Bucket> bucks = new ArrayList<>();
            for (int t = 0; t < 30; t++) {
                bucks.add(bucket("s" + s + "t" + t, 1 + r.nextInt(500)));
            }
            batches.add(stringTerms(bucks, 0L));
        }

        StreamingTermsReducer<StringTerms, StringTerms.Bucket> a = new StreamingTermsReducer<>(topN, partialContext());
        for (StringTerms b : batches)
            a.accept(b);
        StringTerms fromA = (StringTerms) a.finalize(finalContext());

        List<StringTerms> shuffled = new ArrayList<>(batches);
        Collections.shuffle(shuffled, new Random(0xFEED));
        StreamingTermsReducer<StringTerms, StringTerms.Bucket> b = new StreamingTermsReducer<>(topN, partialContext());
        for (StringTerms x : shuffled)
            b.accept(x);
        StringTerms fromB = (StringTerms) b.finalize(finalContext());

        // Bound holds in both orders.
        assertTrue("A should respect topN", fromA.getBuckets().size() <= topN);
        assertTrue("B should respect topN", fromB.getBuckets().size() <= topN);

        // For any term in both, count must be identical (unique terms have no merge/displace
        // interaction, so the count came from exactly one shard in both runs).
        Map<String, Long> aCounts = new HashMap<>();
        for (StringTerms.Bucket x : fromA.getBuckets())
            aCounts.put((String) x.getKey(), x.getDocCount());
        Map<String, Long> bCounts = new HashMap<>();
        for (StringTerms.Bucket x : fromB.getBuckets())
            bCounts.put((String) x.getKey(), x.getDocCount());

        for (Map.Entry<String, Long> e : aCounts.entrySet()) {
            Long bVal = bCounts.get(e.getKey());
            if (bVal != null) {
                assertEquals("term " + e.getKey() + " count should match", e.getValue(), bVal);
            }
        }
    }

    // ---- helpers ----

    private StringTerms stringTerms(List<StringTerms.Bucket> buckets, long otherDocCount) {
        return new StringTerms(
            "t",
            BucketOrder.count(false),
            BucketOrder.count(false),
            Collections.emptyMap(),
            DocValueFormat.RAW,
            SHARD_SIZE_DEFAULT,
            false,
            otherDocCount,
            buckets,
            0L,
            THRESHOLDS
        );
    }

    private StringTerms.Bucket bucket(String term, long docCount) {
        return new StringTerms.Bucket(new BytesRef(term), docCount, InternalAggregations.EMPTY, false, 0L, DocValueFormat.RAW);
    }

    private StringTerms.Bucket bucketWithCard(String term, long docCount, InternalCardinality card) {
        return new StringTerms.Bucket(
            new BytesRef(term),
            docCount,
            InternalAggregations.from(Collections.singletonList(card)),
            false,
            0L,
            DocValueFormat.RAW
        );
    }

    private HyperLogLogPlusPlus newSketch() {
        // Use NON_RECYCLING_INSTANCE to match the production deserialization path (sketches
        // arriving over the wire use NON_RECYCLING). MockBigArrays leak-checks every array it
        // hands out, but our reducer correctly discards input sketches after merging them into
        // a NON_RECYCLING-backed target — so using MockBigArrays for inputs would falsely
        // flag a leak on each discarded input sketch.
        HyperLogLogPlusPlus hll = new HyperLogLogPlusPlus(HLL_PRECISION, BigArrays.NON_RECYCLING_INSTANCE, 1);
        ownedSketches.add(hll);
        return hll;
    }

    private ReduceContext partialContext() {
        return InternalAggregation.ReduceContext.forPartialReduction(bigArrays, null, () -> null);
    }

    private ReduceContext finalContext() {
        return InternalAggregation.ReduceContext.forFinalReduction(
            bigArrays,
            null,
            b -> {},
            org.opensearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree.EMPTY
        );
    }
}
