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
 * Benchmark comparing BigArrays-based terms aggregation vs Arrow vector-based approach
 * across different data distribution scenarios.
 *
 * Distribution modes:
 * - PARTITIONED: Each term appears in exactly one shard (best case for Arrow, no merge needed)
 * - UNIFORM: Terms uniformly distributed, each term appears in all shards
 * - ZIPF: Zipf distribution - popular terms in all shards, rare terms in few shards
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

    /**
     * Distribution mode for term allocation across shards.
     */
    public enum DistributionMode {
        /** Each term appears in exactly one shard - best case for Arrow */
        PARTITIONED,
        /** Terms uniformly distributed across all shards */
        UNIFORM,
        /** Zipf distribution - popular terms everywhere, rare terms localized */
        ZIPF
    }

    @Param({ "10000000" })
    private int totalDocuments;

    @Param({ "10000" })
    private int uniqueTerms;

    @Param({ "10" })
    private int numShards;

    @Param({ "100" })
    private int avgKeyLength;

    @Param({ "PARTITIONED", "UNIFORM", "ZIPF" })
    private DistributionMode distributionMode;

    private BigArrays bigArrays;
    private BufferAllocator arrowAllocator;
    private List<StringTerms> bigArraysShardResults;
    private List<VectorSchemaRoot> arrowShardResults;
    private Random random;
    private String[] termDictionary;  // Pre-generated string terms (index = termId)

    @Setup(Level.Trial)
    public void setup() {
        random = new Random(42);
        bigArrays = new BigArrays(new PageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService(), CircuitBreaker.REQUEST);
        arrowAllocator = new RootAllocator(Long.MAX_VALUE);

        // Generate string term dictionary upfront
        termDictionary = generateTermDictionary();

        // shardTermCounts.get(shard).get(term) = count of term in that shard
        List<Map<String, Integer>> shardTermCounts = new ArrayList<>(numShards);
        for (int i = 0; i < numShards; i++) {
            shardTermCounts.add(new HashMap<>());
        }

        // Generate data based on distribution mode
        switch (distributionMode) {
            case PARTITIONED:
                generatePartitionedData(shardTermCounts);
                break;
            case UNIFORM:
                generateUniformData(shardTermCounts);
                break;
            case ZIPF:
                generateZipfData(shardTermCounts);
                break;
        }

        // Create shard results from the distributed counts
        bigArraysShardResults = new ArrayList<>();
        arrowShardResults = new ArrayList<>();

        for (int shard = 0; shard < numShards; shard++) {
            // Sort once per shard by term key, reuse for both data structures
            List<Map.Entry<String, Integer>> sortedEntries = shardTermCounts.get(shard)
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .toList();

            bigArraysShardResults.add(createBigArraysShardData(sortedEntries));
            arrowShardResults.add(createArrowShardData(sortedEntries));
        }

        // Log statistics about the distribution
        logDistributionStats(shardTermCounts);
    }

    /**
     * Generate a dictionary of unique string terms with realistic variation.
     * Terms are sorted lexicographically to simulate real term dictionaries.
     * Returns an array where index = termId.
     */
    private String[] generateTermDictionary() {
        String[] terms = new String[uniqueTerms];
        Random termRandom = new Random(12345);  // Separate seed for reproducible terms

        for (int i = 0; i < uniqueTerms; i++) {
            // Generate random string of avgKeyLength characters
            StringBuilder sb = new StringBuilder(avgKeyLength);
            for (int j = 0; j < avgKeyLength; j++) {
                // Use alphanumeric characters for realistic terms
                int charType = termRandom.nextInt(3);
                if (charType == 0) {
                    sb.append((char) ('a' + termRandom.nextInt(26)));  // lowercase
                } else if (charType == 1) {
                    sb.append((char) ('A' + termRandom.nextInt(26)));  // uppercase
                } else {
                    sb.append((char) ('0' + termRandom.nextInt(10)));  // digit
                }
            }
            terms[i] = sb.toString();
        }

        // Sort terms lexicographically (like real term dictionaries)
        Arrays.sort(terms);
        return terms;
    }

    /**
     * PARTITIONED: Each term goes to exactly one shard (no overlap).
     * Best case for Arrow - no merging needed across shards.
     */
    private void generatePartitionedData(List<Map<String, Integer>> shardTermCounts) {
        int termsPerShard = uniqueTerms / numShards;

        for (int docId = 0; docId < totalDocuments; docId++) {
            int shard = Math.abs(random.nextInt()) % numShards;
            int termIdInShard = Math.abs(random.nextInt()) % termsPerShard;
            int termId = shard * termsPerShard + termIdInShard;
            String term = termDictionary[termId];
            shardTermCounts.get(shard).merge(term, 1, Integer::sum);
        }
    }

    /**
     * UNIFORM: Each document goes to random shard, term selected uniformly.
     * All terms appear in all shards with similar counts.
     */
    private void generateUniformData(List<Map<String, Integer>> shardTermCounts) {
        for (int docId = 0; docId < totalDocuments; docId++) {
            int shard = Math.abs(random.nextInt()) % numShards;
            int termId = Math.abs(random.nextInt()) % uniqueTerms;
            String term = termDictionary[termId];
            shardTermCounts.get(shard).merge(term, 1, Integer::sum);
        }
    }

    /**
     * ZIPF: Zipf distribution where popular terms are very common
     * and rare terms may only appear in a few shards.
     */
    private void generateZipfData(List<Map<String, Integer>> shardTermCounts) {
        // Pre-compute Zipf CDF
        double[] zipfCdf = new double[uniqueTerms];
        double zipfSum = 0.0;
        for (int k = 1; k <= uniqueTerms; k++) {
            zipfSum += 1.0 / k;
            zipfCdf[k - 1] = zipfSum;
        }
        for (int k = 0; k < uniqueTerms; k++) {
            zipfCdf[k] /= zipfSum;
        }

        for (int docId = 0; docId < totalDocuments; docId++) {
            int shard = Math.abs(random.nextInt()) % numShards;
            double u = random.nextDouble();
            int termId = binarySearchCdf(zipfCdf, u);
            String term = termDictionary[termId];
            shardTermCounts.get(shard).merge(term, 1, Integer::sum);
        }
    }

    private int binarySearchCdf(double[] cdf, double value) {
        int low = 0;
        int high = cdf.length - 1;
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (cdf[mid] < value) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    private void logDistributionStats(List<Map<String, Integer>> shardTermCounts) {
        int totalTermsAcrossShards = 0;
        int minTermsPerShard = Integer.MAX_VALUE;
        int maxTermsPerShard = 0;

        for (int shard = 0; shard < numShards; shard++) {
            int termsInShard = shardTermCounts.get(shard).size();
            totalTermsAcrossShards += termsInShard;
            minTermsPerShard = Math.min(minTermsPerShard, termsInShard);
            maxTermsPerShard = Math.max(maxTermsPerShard, termsInShard);
        }

        // Calculate overlap: how many shards does each term appear in on average
        Map<String, Integer> termShardCount = new HashMap<>();
        for (int shard = 0; shard < numShards; shard++) {
            for (String term : shardTermCounts.get(shard).keySet()) {
                termShardCount.merge(term, 1, Integer::sum);
            }
        }
        double avgShardsPerTerm = termShardCount.values().stream().mapToInt(Integer::intValue).average().orElse(0);

        System.out.println(
            "["
                + distributionMode
                + "] "
                + uniqueTerms
                + " unique terms, "
                + "avg "
                + (totalTermsAcrossShards / numShards)
                + " terms/shard, "
                + "min "
                + minTermsPerShard
                + ", max "
                + maxTermsPerShard
                + ", "
                + "avg overlap: "
                + String.format("%.1f", avgShardsPerTerm)
                + " shards/term"
        );
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

    private StringTerms createBigArraysShardData(List<Map.Entry<String, Integer>> sortedEntries) {
        List<StringTerms.Bucket> buckets = new ArrayList<>(sortedEntries.size());

        for (Map.Entry<String, Integer> entry : sortedEntries) {
            buckets.add(
                new StringTerms.Bucket(
                    new BytesRef(entry.getKey()),  // Direct string key
                    entry.getValue(),
                    InternalAggregations.EMPTY,
                    false,
                    0,
                    DocValueFormat.RAW
                )
            );
        }

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

    private VectorSchemaRoot createArrowShardData(List<Map.Entry<String, Integer>> sortedEntries) {
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

        for (int i = 0; i < sortedEntries.size(); i++) {
            Map.Entry<String, Integer> entry = sortedEntries.get(i);
            byte[] termBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);  // Direct string key
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

        return (StringTerms) bigArraysShardResults.get(0).reduce(new ArrayList<>(bigArraysShardResults), context);
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
     * K-way merge using Arrow vectors with priority queue.
     */
    @Benchmark
    public Map<String, Long> arrowReduce() {
        Map<String, Long> merged = new HashMap<>();

        PriorityQueue<VectorCursor> pq = new PriorityQueue<>(arrowShardResults.size()) {
            @Override
            protected boolean lessThan(VectorCursor a, VectorCursor b) {
                return Arrays.compareUnsigned(a.getCurrentTerm(), b.getCurrentTerm()) < 0;
            }
        };

        for (VectorSchemaRoot root : arrowShardResults) {
            if (root.getRowCount() > 0) {
                pq.add(new VectorCursor(root));
            }
        }

        while (pq.size() > 0) {
            VectorCursor cursor = pq.top();
            byte[] currentTerm = cursor.getCurrentTerm();
            long totalCount = cursor.getCurrentCount();
            cursor.advance();

            if (cursor.hasNext()) {
                pq.updateTop();
            } else {
                pq.pop();
            }

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

            merged.put(new String(currentTerm, StandardCharsets.UTF_8), totalCount);
        }

        return merged;
    }

    /**
     * SIMD-vectorized batch merging for count accumulation.
     * Builds a term index first, then uses Java Vector API for parallel count accumulation.
     */
    @Benchmark
    public Map<String, Long> arrowReduceSIMD() {
        Map<String, Long> merged = new HashMap<>();
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

        // Accumulate counts using SIMD when possible
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
}
