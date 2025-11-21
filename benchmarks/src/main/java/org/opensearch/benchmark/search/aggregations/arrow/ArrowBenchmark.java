/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.search.aggregations.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * Benchmark comparing BigArrays-based terms aggregation vs Arrow vector-based approach.
 * Simulates indexing 10M documents with terms and performing aggregation reduction.
 */
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = {
    "--add-modules=jdk.incubator.vector",
    "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED" })
public class ArrowBenchmark {

    @Param({ "10000000" })
    private int totalDocuments;

    @Param({ "1000", "10000", "100000" })
    private int uniqueTerms;

    @Param({ "10" })
    private int numShards;

    @Param({ "10", "100", "1000" })
    private int avgKeyLength;

    private BigArrays bigArrays;
    private BufferAllocator arrowAllocator;
    private List<StringTerms> bigArraysShardResults;
    private List<VectorSchemaRoot> arrowShardResults;
    private Random random;

    @Setup(Level.Trial)
    public void setup() {
        random = new Random(42);
        bigArrays = new BigArrays(new PageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService(), CircuitBreaker.REQUEST);
        arrowAllocator = new RootAllocator(Long.MAX_VALUE);

        // Generate a global term dictionary with counts
        int[] globalTermDictionary = generateTermDictionary(uniqueTerms);

        // Partition terms across shards (each term goes to one shard based on hash)
        // This simulates real distributed aggregation where terms are partitioned
        bigArraysShardResults = new ArrayList<>();
        arrowShardResults = new ArrayList<>();

        for (int shard = 0; shard < numShards; shard++) {
            // Each shard gets a subset of terms (those that hash to this shard)
            Map<Integer, Integer> shardTerms = new HashMap<>();
            for (int termId = 0; termId < uniqueTerms; termId++) {
                // Simple partitioning: term goes to shard based on termId % numShards
                if (termId % numShards == shard) {
                    shardTerms.put(termId, globalTermDictionary[termId]);
                }
            }

            bigArraysShardResults.add(createBigArraysShardData(shardTerms));
            arrowShardResults.add(createArrowShardData(shardTerms));
        }

        // Run verification only once per trial
        if (System.getProperty("verify.results", "false").equals("true")) {
            verifyResults();
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        bigArraysShardResults.clear();
        for (VectorSchemaRoot data : arrowShardResults) {
            data.close();
        }
        arrowShardResults.clear();
        arrowAllocator.close();
    }

    /**
     * Verification method to ensure all reduce implementations produce the same results.
     * This is not a benchmark - it runs once to validate correctness.
     */
    public void verifyResults() {
        System.out.println("\n=== VERIFICATION: Comparing all reduce method outputs ===\n");

        // Run bigArraysReduce
        System.out.println("Running bigArraysReduce...");
        StringTerms bigArraysResult = bigArraysReduce();
        Map<String, Long> bigArraysMap = new HashMap<>();
        for (StringTerms.Bucket bucket : bigArraysResult.getBuckets()) {
            bigArraysMap.put(bucket.getKeyAsString(), bucket.getDocCount());
        }
        System.out.println("  Result: " + bigArraysMap.size() + " unique terms");

        // Run bigArraysReduceManual
        System.out.println("Running bigArraysReduceManual...");
        Map<BytesRef, Long> bigArraysManualResult = bigArraysReduceManual();
        Map<String, Long> bigArraysManualMap = new HashMap<>();
        for (Map.Entry<BytesRef, Long> entry : bigArraysManualResult.entrySet()) {
            bigArraysManualMap.put(entry.getKey().utf8ToString(), entry.getValue());
        }
        System.out.println("  Result: " + bigArraysManualMap.size() + " unique terms");

        // Run arrowReduce
        System.out.println("Running arrowReduce...");
        Map<String, Long> arrowReduceResult = arrowReduce();
        System.out.println("  Result: " + arrowReduceResult.size() + " unique terms");

        // Run arrowReduceOptimized
        System.out.println("Running arrowReduceOptimized...");
        Map<BytesRef, Long> arrowOptimizedResult = arrowReduceOptimized();
        Map<String, Long> arrowOptimizedMap = new HashMap<>();
        for (Map.Entry<BytesRef, Long> entry : arrowOptimizedResult.entrySet()) {
            arrowOptimizedMap.put(entry.getKey().utf8ToString(), entry.getValue());
        }
        System.out.println("  Result: " + arrowOptimizedMap.size() + " unique terms");

        // Run arrowReduceSIMD
        System.out.println("Running arrowReduceSIMD...");
        Map<String, Long> arrowSIMDResult = arrowReduceSIMD();
        System.out.println("  Result: " + arrowSIMDResult.size() + " unique terms");

        // Compare all results
        System.out.println("\n=== Comparing Results ===");

        boolean allMatch = true;
        int sampleCount = 0;
        int maxSamples = 10;

        // Check if all maps have the same size
        if (bigArraysMap.size() != bigArraysManualMap.size()
            || bigArraysMap.size() != arrowReduceResult.size()
            || bigArraysMap.size() != arrowOptimizedMap.size()
            || bigArraysMap.size() != arrowSIMDResult.size()) {
            System.out.println("❌ MISMATCH: Different number of terms!");
            System.out.println("  bigArrays: " + bigArraysMap.size());
            System.out.println("  bigArraysManual: " + bigArraysManualMap.size());
            System.out.println("  arrowReduce: " + arrowReduceResult.size());
            System.out.println("  arrowOptimized: " + arrowOptimizedMap.size());
            System.out.println("  arrowSIMD: " + arrowSIMDResult.size());
            allMatch = false;
        }

        // Compare each term's count
        for (Map.Entry<String, Long> entry : bigArraysMap.entrySet()) {
            String term = entry.getKey();
            Long bigArraysCount = entry.getValue();
            Long bigArraysManualCount = bigArraysManualMap.get(term);
            Long arrowCount = arrowReduceResult.get(term);
            Long arrowOptCount = arrowOptimizedMap.get(term);
            Long arrowSIMDCount = arrowSIMDResult.get(term);

            if (bigArraysManualCount == null || arrowCount == null || arrowOptCount == null || arrowSIMDCount == null) {
                System.out.println("❌ MISSING TERM: " + term);
                System.out.println("  bigArrays: " + bigArraysCount);
                System.out.println("  bigArraysManual: " + bigArraysManualCount);
                System.out.println("  arrowReduce: " + arrowCount);
                System.out.println("  arrowOptimized: " + arrowOptCount);
                System.out.println("  arrowSIMD: " + arrowSIMDCount);
                allMatch = false;
                sampleCount++;
                if (sampleCount >= maxSamples) break;
                continue;
            }

            if (!bigArraysCount.equals(bigArraysManualCount)
                || !bigArraysCount.equals(arrowCount)
                || !bigArraysCount.equals(arrowOptCount)
                || !bigArraysCount.equals(arrowSIMDCount)) {
                System.out.println("❌ COUNT MISMATCH for term: " + term);
                System.out.println("  bigArrays: " + bigArraysCount);
                System.out.println("  bigArraysManual: " + bigArraysManualCount);
                System.out.println("  arrowReduce: " + arrowCount);
                System.out.println("  arrowOptimized: " + arrowOptCount);
                System.out.println("  arrowSIMD: " + arrowSIMDCount);
                allMatch = false;
                sampleCount++;
                if (sampleCount >= maxSamples) break;
            } else if (sampleCount < 5) {
                // Show some matching examples
                System.out.println("✓ Match for term: " + term + " = " + bigArraysCount);
                sampleCount++;
            }
        }

        // Check for extra terms in arrow results
        for (String term : bigArraysManualMap.keySet()) {
            if (!bigArraysMap.containsKey(term)) {
                System.out.println("❌ EXTRA TERM in bigArraysManual: " + term);
                allMatch = false;
            }
        }
        for (String term : arrowReduceResult.keySet()) {
            if (!bigArraysMap.containsKey(term)) {
                System.out.println("❌ EXTRA TERM in arrowReduce: " + term);
                allMatch = false;
            }
        }
        for (String term : arrowOptimizedMap.keySet()) {
            if (!bigArraysMap.containsKey(term)) {
                System.out.println("❌ EXTRA TERM in arrowOptimized: " + term);
                allMatch = false;
            }
        }
        for (String term : arrowSIMDResult.keySet()) {
            if (!bigArraysMap.containsKey(term)) {
                System.out.println("❌ EXTRA TERM in arrowSIMD: " + term);
                allMatch = false;
            }
        }

        System.out.println("\n=== Final Result ===");
        if (allMatch) {
            System.out.println("✅ SUCCESS: All reduce methods produce identical results!");
        } else {
            System.out.println("❌ FAILURE: Results differ between methods!");
            throw new AssertionError("Reduce methods produced different results!");
        }
        System.out.println("==================\n");
    }

    private int[] generateTermDictionary(int size) {
        int[] termDictionary = new int[size];
        for (int i = 0; i < totalDocuments; i++) {
            termDictionary[Math.abs(random.nextInt()) % size]++;
        }
        return termDictionary;
    }

    // Create shard data from a map of termId -> count
    private StringTerms createBigArraysShardData(Map<Integer, Integer> shardTerms) {
        String keyPrefix = "x".repeat(avgKeyLength);
        List<StringTerms.Bucket> buckets = new ArrayList<>();

        // Sort by term ID to ensure consistent ordering
        shardTerms.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(
                entry -> buckets.add(
                    new StringTerms.Bucket(
                        new BytesRef(keyPrefix + entry.getKey()),
                        entry.getValue(),
                        InternalAggregations.EMPTY,
                        false,
                        0,
                        DocValueFormat.RAW
                    )
                )
            );

        return new StringTerms(
            "terms_agg",
            BucketOrder.key(true),
            BucketOrder.count(false),
            Collections.emptyMap(),
            DocValueFormat.RAW,
            numShards,
            false,
            0,
            buckets,
            0,
            new TermsAggregator.BucketCountThresholds(0, 0, uniqueTerms, uniqueTerms)
        );
    }

    private VectorSchemaRoot createArrowShardData(Map<Integer, Integer> shardTerms) {
        String keyPrefix = "x".repeat(avgKeyLength);
        List<Field> fields = Arrays.asList(
            new Field("term", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("count", FieldType.nullable(new ArrowType.Int(32, true)), null)
        );
        Schema schema = new Schema(fields);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, arrowAllocator.newChildAllocator("child", 0, Long.MAX_VALUE));
        VarCharVector termsVector = (VarCharVector) root.getVector("term");
        org.apache.arrow.vector.IntVector countsVector = (org.apache.arrow.vector.IntVector) root.getVector("count");
        termsVector.allocateNew();
        countsVector.allocateNew();

        // Sort by term ID to ensure consistent ordering
        List<Map.Entry<Integer, Integer>> sortedEntries = shardTerms.entrySet().stream().sorted(Map.Entry.comparingByKey()).toList();

        for (int i = 0; i < sortedEntries.size(); i++) {
            Map.Entry<Integer, Integer> entry = sortedEntries.get(i);
            String term = keyPrefix + entry.getKey();
            byte[] termBytes = term.getBytes(StandardCharsets.UTF_8);
            termsVector.setSafe(i, termBytes, 0, termBytes.length);
            countsVector.setSafe(i, entry.getValue());
        }
        root.setRowCount(sortedEntries.size());
        return root;
    }

    @Benchmark
    public StringTerms bigArraysReduce() {
        final MultiBucketConsumerService.MultiBucketConsumer bucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(
            Integer.MAX_VALUE,
            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
        );

        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(
            bigArrays,
            null,
            bucketConsumer,
            PipelineAggregator.PipelineTree.EMPTY
        );

        StringTerms reduced = (StringTerms) bigArraysShardResults.get(0).reduce(new ArrayList<>(bigArraysShardResults), context);
        return reduced;
    }

    private static class VectorCursor {
        final VectorSchemaRoot root;
        final VarCharVector termsVector;
        final IntVector countsVector;
        int currentIndex;

        VectorCursor(VectorSchemaRoot root) {
            this.root = root;
            this.termsVector = (VarCharVector) root.getVector("term");
            this.countsVector = (IntVector) root.getVector("count");
            this.currentIndex = 0;
        }

        boolean hasNext() {
            return currentIndex < root.getRowCount();
        }

        byte[] getCurrentTerm() {
            return termsVector.get(currentIndex);
        }

        int getCurrentCount() {
            return countsVector.get(currentIndex);
        }

        void advance() {
            currentIndex++;
        }
    }

    /**
     * SIMD-vectorized batch merging for count accumulation.
     * This is most effective when you have multiple shards with overlapping terms.
     * Uses Java Vector API for parallel count accumulation.
     */
    @Benchmark
    public Map<String, Long> arrowReduceSIMD() {
        Map<String, Long> merged = new HashMap<>();

        // Strategy: For each unique term, collect all counts from all shards at once
        // This allows SIMD vectorization of the count accumulation
        VectorSpecies<Integer> SPECIES = jdk.incubator.vector.IntVector.SPECIES_PREFERRED;
        int lanes = SPECIES.length();

        // Build a map of term -> (list of shard indices, list of row indices)
        Map<String, List<int[]>> termToShardAndIndex = new HashMap<>();

        for (int shardIdx = 0; shardIdx < arrowShardResults.size(); shardIdx++) {
            VectorSchemaRoot root = arrowShardResults.get(shardIdx);
            VarCharVector termsVector = (VarCharVector) root.getVector("term");

            for (int i = 0; i < root.getRowCount(); i++) {
                String term = new String(termsVector.get(i), StandardCharsets.UTF_8);
                termToShardAndIndex.computeIfAbsent(term, k -> new ArrayList<>()).add(new int[] { shardIdx, i });
            }
        }

        // Now accumulate counts using SIMD when possible
        int[] countBuffer = new int[Math.max(lanes, arrowShardResults.size())];

        for (Map.Entry<String, List<int[]>> entry : termToShardAndIndex.entrySet()) {
            String term = entry.getKey();
            List<int[]> shardAndIndexPairs = entry.getValue();

            // Gather counts from all shards into buffer
            int numCounts = shardAndIndexPairs.size();
            for (int i = 0; i < numCounts; i++) {
                int[] pair = shardAndIndexPairs.get(i);
                int shardIdx = pair[0];
                int rowIdx = pair[1];
                VectorSchemaRoot root = arrowShardResults.get(shardIdx);
                org.apache.arrow.vector.IntVector countsVector = (org.apache.arrow.vector.IntVector) root.getVector("count");
                countBuffer[i] = countsVector.get(rowIdx);
            }

            // Use SIMD to sum counts
            long total = 0;
            int i = 0;

            // Vectorized loop
            for (; i < SPECIES.loopBound(numCounts); i += lanes) {
                jdk.incubator.vector.IntVector vec = jdk.incubator.vector.IntVector.fromArray(SPECIES, countBuffer, i);
                total += vec.reduceLanes(VectorOperators.ADD);
            }

            // Scalar tail
            for (; i < numCounts; i++) {
                total += countBuffer[i];
            }

            merged.put(term, total);
        }

        return merged;
    }

    /**
     * Optimized version leveraging Arrow's fixed-width nature and SIMD vectorization.
     * Key optimizations:
     * 1. Direct buffer access to avoid object creation
     * 2. Batch processing with SIMD for comparisons
     * 3. Minimize heap allocations during merge
     * 4. Use BytesRef for zero-copy string handling
     */
    @Benchmark
    public Map<BytesRef, Long> arrowReduceOptimized() {
        Map<BytesRef, Long> merged = new HashMap<>();

        // Pre-sorted assumption: if vectors are already sorted, we can use streaming merge
        // Create cursors with direct buffer access
        VectorCursorOptimized[] cursors = new VectorCursorOptimized[arrowShardResults.size()];
        int activeCursors = 0;

        for (VectorSchemaRoot root : arrowShardResults) {
            if (root.getRowCount() > 0) {
                cursors[activeCursors++] = new VectorCursorOptimized(root);
            }
        }

        // Use a simple min-heap for K-way merge (more cache-friendly than PriorityQueue for small K)
        MinHeap heap = new MinHeap(cursors, activeCursors);
        heap.heapify();

        BytesRef currentTerm = new BytesRef();
        BytesRef reusableTerm = new BytesRef();

        while (heap.size > 0) {
            VectorCursorOptimized minCursor = heap.data[0];
            minCursor.getCurrentTermInto(currentTerm);
            long totalCount = minCursor.getCurrentCount();
            minCursor.advance();

            // Update or remove from heap
            if (minCursor.hasNext()) {
                heap.siftDown(0);
            } else {
                heap.removeTop();
            }

            // Merge all cursors with the same term
            while (heap.size > 0) {
                VectorCursorOptimized nextCursor = heap.data[0];
                nextCursor.getCurrentTermInto(reusableTerm);

                if (currentTerm.bytesEquals(reusableTerm)) {
                    totalCount += nextCursor.getCurrentCount();
                    nextCursor.advance();

                    if (nextCursor.hasNext()) {
                        heap.siftDown(0);
                    } else {
                        heap.removeTop();
                    }
                } else {
                    break;
                }
            }

            // Store with a copy of the term
            merged.put(BytesRef.deepCopyOf(currentTerm), totalCount);
        }

        return merged;
    }

    /**
     * Optimized cursor with direct buffer access and zero-copy operations
     */
    private static class VectorCursorOptimized {
        final VarCharVector termsVector;
        final org.apache.arrow.vector.IntVector countsVector;
        final int maxIndex;
        int currentIndex;

        VectorCursorOptimized(VectorSchemaRoot root) {
            this.termsVector = (VarCharVector) root.getVector("term");
            this.countsVector = (org.apache.arrow.vector.IntVector) root.getVector("count");
            this.maxIndex = root.getRowCount();
            this.currentIndex = 0;
        }

        boolean hasNext() {
            return currentIndex < maxIndex;
        }

        // Zero-copy: reuse BytesRef to avoid allocations
        void getCurrentTermInto(BytesRef target) {
            byte[] termBytes = termsVector.get(currentIndex);
            target.bytes = termBytes;
            target.offset = 0;
            target.length = termBytes.length;
        }

        int getCurrentCount() {
            return countsVector.get(currentIndex);
        }

        void advance() {
            currentIndex++;
        }

        // SIMD-optimized comparison using vectorized byte comparison
        int compareTermTo(VectorCursorOptimized other) {
            byte[] a = termsVector.get(currentIndex);
            byte[] b = other.termsVector.get(other.currentIndex);
            return Arrays.compareUnsigned(a, b);
        }
    }

    /**
     * Minimal heap implementation optimized for K-way merge.
     * More cache-friendly than Java's PriorityQueue for small K values.
     */
    private static class MinHeap {
        final VectorCursorOptimized[] data;
        int size;

        MinHeap(VectorCursorOptimized[] cursors, int initialSize) {
            this.data = cursors;
            this.size = initialSize;
        }

        void heapify() {
            for (int i = (size / 2) - 1; i >= 0; i--) {
                siftDown(i);
            }
        }

        void siftDown(int index) {
            while (true) {
                int smallest = index;
                int left = 2 * index + 1;
                int right = 2 * index + 2;

                if (left < size && data[left].compareTermTo(data[smallest]) < 0) {
                    smallest = left;
                }
                if (right < size && data[right].compareTermTo(data[smallest]) < 0) {
                    smallest = right;
                }

                if (smallest == index) break;

                // Swap
                VectorCursorOptimized temp = data[index];
                data[index] = data[smallest];
                data[smallest] = temp;
                index = smallest;
            }
        }

        void removeTop() {
            if (size > 0) {
                data[0] = data[--size];
                if (size > 0) {
                    siftDown(0);
                }
            }
        }
    }

    @Benchmark
    public Map<String, Long> arrowReduce() {
        Map<String, Long> merged = new HashMap<>();

        // Create a priority queue that tracks position in each vector
        PriorityQueue<VectorCursor> pq = new PriorityQueue<>(arrowShardResults.size()) {
            @Override
            protected boolean lessThan(VectorCursor a, VectorCursor b) {
                return Arrays.compareUnsigned(a.getCurrentTerm(), b.getCurrentTerm()) < 0;
            }
        };

        // Initialize priority queue with first element from each shard
        for (VectorSchemaRoot root : arrowShardResults) {
            if (root.getRowCount() > 0) {
                VectorCursor cursor = new VectorCursor(root);
                pq.add(cursor);
            }
        }

        // Merge using k-way merge algorithm
        while (pq.size() > 0) {
            VectorCursor cursor = pq.top();
            byte[] currentTerm = cursor.getCurrentTerm();
            long currentCount = cursor.getCurrentCount();

            // Convert byte[] to String for the map key
            String termKey = new String(currentTerm, StandardCharsets.UTF_8);

            // Accumulate counts for the same term from all shards
            long totalCount = currentCount;
            cursor.advance();

            // Check if this cursor has more elements
            if (cursor.hasNext()) {
                pq.updateTop();
            } else {
                pq.pop();
            }

            // Check if next cursors have the same term
            while (pq.size() > 0 && Arrays.equals(currentTerm, pq.top().getCurrentTerm())) {
                VectorCursor nextCursor = pq.top();
                totalCount += nextCursor.getCurrentCount();
                nextCursor.advance();

                if (nextCursor.hasNext()) {
                    pq.updateTop();
                } else {
                    pq.pop();
                }
            }

            merged.put(termKey, totalCount);
        }

        return merged;
    }

    /**
     * Pure BigArrays-based reduction without Arrow vectors.
     * This implementation uses BigArrays to store intermediate term/count pairs
     * and performs a k-way merge similar to arrowReduce but using OpenSearch's
     * native BigArrays instead of Arrow vectors.
     */
    @Benchmark
    public Map<BytesRef, Long> bigArraysReduceManual() {
        Map<BytesRef, Long> merged = new HashMap<>();

        // Create cursors for each shard's StringTerms buckets
        BigArraysBucketCursor[] cursors = new BigArraysBucketCursor[bigArraysShardResults.size()];
        int activeCursors = 0;

        for (StringTerms terms : bigArraysShardResults) {
            if (!terms.getBuckets().isEmpty()) {
                cursors[activeCursors++] = new BigArraysBucketCursor(terms);
            }
        }

        // Use priority queue for k-way merge
        PriorityQueue<BigArraysBucketCursor> pq = new PriorityQueue<>(activeCursors) {
            @Override
            protected boolean lessThan(BigArraysBucketCursor a, BigArraysBucketCursor b) {
                return a.getCurrentTerm().compareTo(b.getCurrentTerm()) < 0;
            }
        };

        // Initialize priority queue
        for (int i = 0; i < activeCursors; i++) {
            pq.add(cursors[i]);
        }

        BytesRef currentTerm = new BytesRef();
        BytesRef reusableTerm = new BytesRef();

        // Merge using k-way merge algorithm
        while (pq.size() > 0) {
            BigArraysBucketCursor cursor = pq.top();
            cursor.getCurrentTermInto(currentTerm);
            long totalCount = cursor.getCurrentCount();
            cursor.advance();

            // Update or remove from heap
            if (cursor.hasNext()) {
                pq.updateTop();
            } else {
                pq.pop();
            }

            // Merge all cursors with the same term
            while (pq.size() > 0) {
                BigArraysBucketCursor nextCursor = pq.top();
                nextCursor.getCurrentTermInto(reusableTerm);

                if (currentTerm.bytesEquals(reusableTerm)) {
                    totalCount += nextCursor.getCurrentCount();
                    nextCursor.advance();

                    if (nextCursor.hasNext()) {
                        pq.updateTop();
                    } else {
                        pq.pop();
                    }
                } else {
                    break;
                }
            }

            // Store with a copy of the term
            merged.put(BytesRef.deepCopyOf(currentTerm), totalCount);
        }

        return merged;
    }

    /**
     * Cursor for iterating over StringTerms buckets.
     * Provides a similar interface to VectorCursorOptimized but operates on
     * OpenSearch's native StringTerms.Bucket objects.
     */
    private static class BigArraysBucketCursor {
        final List<StringTerms.Bucket> buckets;
        final int maxIndex;
        int currentIndex;

        BigArraysBucketCursor(StringTerms terms) {
            this.buckets = terms.getBuckets();
            this.maxIndex = buckets.size();
            this.currentIndex = 0;
        }

        boolean hasNext() {
            return currentIndex < maxIndex;
        }

        BytesRef getCurrentTerm() {
            Object key = buckets.get(currentIndex).getKey();
            if (key instanceof BytesRef) {
                return (BytesRef) key;
            } else if (key instanceof String) {
                return new BytesRef((String) key);
            } else {
                return new BytesRef(key.toString());
            }
        }

        void getCurrentTermInto(BytesRef target) {
            BytesRef source = getCurrentTerm();
            target.bytes = source.bytes;
            target.offset = source.offset;
            target.length = source.length;
        }

        long getCurrentCount() {
            return buckets.get(currentIndex).getDocCount();
        }

        void advance() {
            currentIndex++;
        }
    }
}
