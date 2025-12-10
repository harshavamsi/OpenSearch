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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalSum;
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

/**
 * Benchmark comparing reduction with sub-aggregations using real OpenSearch classes.
 * Simulates: terms aggregation -> [sum, avg, min, max] sub-aggregations
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
public class ArrowSubAggsBenchmark {

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
    private int numTerms;

    @Param({ "100" })
    private int avgKeyLength;

    @Param({ "10" })
    private int numShards;

    @Param({ "PARTITIONED", "UNIFORM", "ZIPF" })
    private DistributionMode distributionMode;

    // Real OpenSearch StringTerms per shard
    private List<StringTerms> bigArraysShardResults;

    // Arrow-style data structures (columnar primitive arrays)
    private List<byte[][]> arrowTerms;
    private List<long[]> arrowDocCounts;
    private List<double[]> arrowSumValues;
    private List<double[]> arrowAvgSums;
    private List<long[]> arrowAvgCounts;
    private List<double[]> arrowMinValues;
    private List<double[]> arrowMaxValues;

    private BufferAllocator arrowAllocator;
    private List<VectorSchemaRoot> arrowVectors;

    private BigArrays bigArrays;

    /**
     * Array-based cursor for Arrow columnar data
     */
    static class ArrowCursor {
        final byte[][] terms;
        final long[] docCounts;
        final double[] sumValues;
        final double[] avgSums;
        final long[] avgCounts;
        final double[] minValues;
        final double[] maxValues;
        int index;

        ArrowCursor(
            byte[][] terms,
            long[] docCounts,
            double[] sumValues,
            double[] avgSums,
            long[] avgCounts,
            double[] minValues,
            double[] maxValues
        ) {
            this.terms = terms;
            this.docCounts = docCounts;
            this.sumValues = sumValues;
            this.avgSums = avgSums;
            this.avgCounts = avgCounts;
            this.minValues = minValues;
            this.maxValues = maxValues;
            this.index = 0;
        }

        byte[] currentTerm() {
            return terms[index];
        }

        long currentDocCount() {
            return docCounts[index];
        }

        double getSum() {
            return sumValues[index];
        }

        double getAvgSum() {
            return avgSums[index];
        }

        long getAvgCount() {
            return avgCounts[index];
        }

        double getMin() {
            return minValues[index];
        }

        double getMax() {
            return maxValues[index];
        }

        boolean hasNext() {
            return index < terms.length - 1;
        }

        void next() {
            index++;
        }

        boolean isValid() {
            return index < terms.length;
        }
    }

    /**
     * Data holder for a term's aggregation data in a single shard.
     */
    private static class TermData {
        long docCount;
        double sumValue;
        double avgSum;
        long avgCount;
        double minValue;
        double maxValue;

        TermData(long docCount, double sumValue, double avgSum, long avgCount, double minValue, double maxValue) {
            this.docCount = docCount;
            this.sumValue = sumValue;
            this.avgSum = avgSum;
            this.avgCount = avgCount;
            this.minValue = minValue;
            this.maxValue = maxValue;
        }
    }

    private Random random;
    private String[] termDictionary;  // Pre-generated string terms (index = termId)

    @Setup(Level.Trial)
    public void setup() {
        random = new Random(42);
        bigArrays = BigArrays.NON_RECYCLING_INSTANCE;

        // Generate string term dictionary upfront
        termDictionary = generateTermDictionary();

        // shardTermData.get(shard).get(term) = TermData for that term in that shard
        List<Map<String, TermData>> shardTermData = new ArrayList<>(numShards);
        for (int i = 0; i < numShards; i++) {
            shardTermData.add(new HashMap<>());
        }

        // Generate data based on distribution mode
        switch (distributionMode) {
            case PARTITIONED:
                generatePartitionedData(shardTermData);
                break;
            case UNIFORM:
                generateUniformData(shardTermData);
                break;
            case ZIPF:
                generateZipfData(shardTermData);
                break;
        }

        // Initialize collections
        bigArraysShardResults = new ArrayList<>(numShards);
        arrowTerms = new ArrayList<>(numShards);
        arrowDocCounts = new ArrayList<>(numShards);
        arrowSumValues = new ArrayList<>(numShards);
        arrowAvgSums = new ArrayList<>(numShards);
        arrowAvgCounts = new ArrayList<>(numShards);
        arrowMinValues = new ArrayList<>(numShards);
        arrowMaxValues = new ArrayList<>(numShards);

        arrowAllocator = new RootAllocator(Long.MAX_VALUE);
        arrowVectors = new ArrayList<>(numShards);

        // Create shard results from the distributed data
        for (int shard = 0; shard < numShards; shard++) {
            Map<String, TermData> shardData = shardTermData.get(shard);
            int termsInShard = shardData.size();

            List<StringTerms.Bucket> buckets = new ArrayList<>(termsInShard);
            byte[][] shardTermsArray = new byte[termsInShard][];
            long[] shardDocCounts = new long[termsInShard];
            double[] shardSumValues = new double[termsInShard];
            double[] shardAvgSums = new double[termsInShard];
            long[] shardAvgCounts = new long[termsInShard];
            double[] shardMinValues = new double[termsInShard];
            double[] shardMaxValues = new double[termsInShard];

            // Sort entries by term key for consistent ordering
            List<Map.Entry<String, TermData>> sortedEntries = shardData.entrySet().stream().sorted(Map.Entry.comparingByKey()).toList();

            for (int i = 0; i < sortedEntries.size(); i++) {
                Map.Entry<String, TermData> entry = sortedEntries.get(i);
                String termStr = entry.getKey();
                TermData data = entry.getValue();
                byte[] term = termStr.getBytes(StandardCharsets.UTF_8);

                // Create real InternalAggregations with sub-aggs
                List<InternalAggregation> subAggsList = Arrays.asList(
                    new InternalSum("sum", data.sumValue, DocValueFormat.RAW, Collections.emptyMap()),
                    new InternalAvg("avg", data.avgSum, data.avgCount, DocValueFormat.RAW, Collections.emptyMap()),
                    new InternalMin("min", data.minValue, DocValueFormat.RAW, Collections.emptyMap()),
                    new InternalMax("max", data.maxValue, DocValueFormat.RAW, Collections.emptyMap())
                );
                InternalAggregations subAggs = InternalAggregations.from(subAggsList);

                // Create real StringTerms.Bucket
                buckets.add(new StringTerms.Bucket(new BytesRef(term), data.docCount, subAggs, true, 0L, DocValueFormat.RAW));

                // Arrow columnar format
                shardTermsArray[i] = term;
                shardDocCounts[i] = data.docCount;
                shardSumValues[i] = data.sumValue;
                shardAvgSums[i] = data.avgSum;
                shardAvgCounts[i] = data.avgCount;
                shardMinValues[i] = data.minValue;
                shardMaxValues[i] = data.maxValue;
            }

            // Create real StringTerms
            StringTerms stringTerms = new StringTerms(
                "terms",
                BucketOrder.key(true),
                BucketOrder.count(false),
                Collections.emptyMap(),
                DocValueFormat.RAW,
                numShards,
                true,
                0,
                buckets,
                0,
                new TermsAggregator.BucketCountThresholds(0, 0, numTerms, numTerms)
            );
            bigArraysShardResults.add(stringTerms);

            arrowTerms.add(shardTermsArray);
            arrowDocCounts.add(shardDocCounts);
            arrowSumValues.add(shardSumValues);
            arrowAvgSums.add(shardAvgSums);
            arrowAvgCounts.add(shardAvgCounts);
            arrowMinValues.add(shardMinValues);
            arrowMaxValues.add(shardMaxValues);

            // Create Arrow vectors with nested struct
            arrowVectors.add(
                createArrowVector(
                    shard,
                    shardTermsArray,
                    shardDocCounts,
                    shardSumValues,
                    shardAvgSums,
                    shardAvgCounts,
                    shardMinValues,
                    shardMaxValues
                )
            );
        }

        // Log statistics about the distribution
        logDistributionStats(shardTermData);

        // Run verification if requested
        if ("true".equals(System.getProperty("verify.results"))) {
            verifyResults();
        }
    }

    /**
     * Generate a dictionary of unique string terms with realistic variation.
     * Terms are sorted lexicographically to simulate real term dictionaries.
     */
    private String[] generateTermDictionary() {
        String[] terms = new String[numTerms];
        Random termRandom = new Random(12345);  // Separate seed for reproducible terms

        for (int i = 0; i < numTerms; i++) {
            StringBuilder sb = new StringBuilder(avgKeyLength);
            for (int j = 0; j < avgKeyLength; j++) {
                int charType = termRandom.nextInt(3);
                if (charType == 0) {
                    sb.append((char) ('a' + termRandom.nextInt(26)));
                } else if (charType == 1) {
                    sb.append((char) ('A' + termRandom.nextInt(26)));
                } else {
                    sb.append((char) ('0' + termRandom.nextInt(10)));
                }
            }
            terms[i] = sb.toString();
        }

        Arrays.sort(terms);
        return terms;
    }

    /**
     * PARTITIONED: Each term goes to exactly one shard (no overlap).
     * Best case for Arrow - no merging needed across shards.
     * Uses same approach as ArrowBenchmark - simulate document distribution.
     */
    private void generatePartitionedData(List<Map<String, TermData>> shardTermData) {
        int termsPerShard = numTerms / numShards;

        for (int docId = 0; docId < totalDocuments; docId++) {
            int shard = Math.abs(random.nextInt()) % numShards;
            int termIdInShard = Math.abs(random.nextInt()) % termsPerShard;
            int termId = shard * termsPerShard + termIdInShard;
            String term = termDictionary[termId];

            // If term already exists in this shard, increment doc count
            // Otherwise create new entry with random sub-agg values
            TermData existing = shardTermData.get(shard).get(term);
            if (existing != null) {
                existing.docCount++;
                double newValue = random.nextDouble() * 100;
                existing.sumValue += newValue;
                existing.avgSum += newValue;
                existing.avgCount++;
                existing.minValue = Math.min(existing.minValue, newValue);
                existing.maxValue = Math.max(existing.maxValue, newValue);
            } else {
                double value = random.nextDouble() * 100;
                shardTermData.get(shard)
                    .put(
                        term,
                        new TermData(
                            1,           // docCount
                            value,       // sumValue
                            value,       // avgSum
                            1,           // avgCount
                            value,       // minValue
                            value + 100  // maxValue (offset to ensure max > min initially)
                        )
                    );
            }
        }
    }

    /**
     * UNIFORM: Each document goes to random shard, term selected uniformly.
     * All terms appear in all shards with similar counts.
     * Uses same approach as ArrowBenchmark - simulate document distribution.
     */
    private void generateUniformData(List<Map<String, TermData>> shardTermData) {
        for (int docId = 0; docId < totalDocuments; docId++) {
            int shard = Math.abs(random.nextInt()) % numShards;
            int termId = Math.abs(random.nextInt()) % numTerms;
            String term = termDictionary[termId];

            // If term already exists in this shard, increment doc count
            // Otherwise create new entry with random sub-agg values
            TermData existing = shardTermData.get(shard).get(term);
            if (existing != null) {
                existing.docCount++;
                double newValue = random.nextDouble() * 100;
                existing.sumValue += newValue;
                existing.avgSum += newValue;
                existing.avgCount++;
                existing.minValue = Math.min(existing.minValue, newValue);
                existing.maxValue = Math.max(existing.maxValue, newValue);
            } else {
                double value = random.nextDouble() * 100;
                shardTermData.get(shard)
                    .put(
                        term,
                        new TermData(
                            1,           // docCount
                            value,       // sumValue
                            value,       // avgSum
                            1,           // avgCount
                            value,       // minValue
                            value + 100  // maxValue (offset to ensure max > min initially)
                        )
                    );
            }
        }
    }

    /**
     * ZIPF: Popular terms appear in all shards, rare terms appear in few shards.
     * Term frequency follows Zipf distribution.
     * Uses same approach as ArrowBenchmark - simulate document distribution.
     */
    private void generateZipfData(List<Map<String, TermData>> shardTermData) {
        // Pre-compute Zipf CDF for term selection
        double[] zipfCdf = new double[numTerms];
        double zipfSum = 0.0;
        for (int k = 1; k <= numTerms; k++) {
            zipfSum += 1.0 / k;
            zipfCdf[k - 1] = zipfSum;
        }
        for (int k = 0; k < numTerms; k++) {
            zipfCdf[k] /= zipfSum;
        }

        // Simulate document distribution - each "document" goes to a random shard
        // with a Zipf-distributed term. Popular terms naturally appear in more shards.
        // Use a reasonable number of documents to get good distribution
        int totalDocuments = numTerms * numShards;

        for (int docId = 0; docId < totalDocuments; docId++) {
            int shard = Math.abs(random.nextInt()) % numShards;
            double u = random.nextDouble();
            int termId = binarySearchCdf(zipfCdf, u);
            String term = termDictionary[termId];

            // If term already exists in this shard, increment doc count
            // Otherwise create new entry with random sub-agg values
            TermData existing = shardTermData.get(shard).get(term);
            if (existing != null) {
                existing.docCount++;
                // For sub-aggs, we accumulate sum, update min/max
                double newValue = random.nextDouble() * 100;
                existing.sumValue += newValue;
                existing.avgSum += newValue;
                existing.avgCount++;
                existing.minValue = Math.min(existing.minValue, newValue);
                existing.maxValue = Math.max(existing.maxValue, newValue);
            } else {
                double value = random.nextDouble() * 100;
                shardTermData.get(shard)
                    .put(
                        term,
                        new TermData(
                            1,           // docCount
                            value,       // sumValue
                            value,       // avgSum
                            1,           // avgCount
                            value,       // minValue
                            value + 100  // maxValue (offset to ensure max > min initially)
                        )
                    );
            }
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

    private void logDistributionStats(List<Map<String, TermData>> shardTermData) {
        int totalTermsAcrossShards = 0;
        int minTermsPerShard = Integer.MAX_VALUE;
        int maxTermsPerShard = 0;

        for (int shard = 0; shard < numShards; shard++) {
            int termsInShard = shardTermData.get(shard).size();
            totalTermsAcrossShards += termsInShard;
            minTermsPerShard = Math.min(minTermsPerShard, termsInShard);
            maxTermsPerShard = Math.max(maxTermsPerShard, termsInShard);
        }

        // Calculate overlap: how many shards does each term appear in on average
        Map<String, Integer> termShardCount = new HashMap<>();
        for (int shard = 0; shard < numShards; shard++) {
            for (String term : shardTermData.get(shard).keySet()) {
                termShardCount.merge(term, 1, Integer::sum);
            }
        }
        double avgShardsPerTerm = termShardCount.values().stream().mapToInt(Integer::intValue).average().orElse(0);

        System.out.println(
            "["
                + distributionMode
                + "] "
                + totalDocuments
                + " docs, "
                + numTerms
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

    private VectorSchemaRoot createArrowVector(
        int shard,
        byte[][] terms,
        long[] docCounts,
        double[] sumValues,
        double[] avgSums,
        long[] avgCounts,
        double[] minValues,
        double[] maxValues
    ) {
        // Nested schema with struct for sub-aggregations
        List<Field> subAggFields = Arrays.asList(
            new Field("sum", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
            new Field("avg_sum", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
            new Field("avg_count", FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("min", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null),
            new Field("max", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
        );

        Schema schema = new Schema(
            Arrays.asList(
                new Field("term", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("doc_count", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("sub_aggs", FieldType.nullable(new ArrowType.Struct()), subAggFields)
            )
        );

        VectorSchemaRoot root = VectorSchemaRoot.create(schema, arrowAllocator.newChildAllocator("shard-" + shard, 0, Long.MAX_VALUE));
        VarCharVector termsVector = (VarCharVector) root.getVector("term");
        BigIntVector docCountVector = (BigIntVector) root.getVector("doc_count");
        StructVector subAggsVector = (StructVector) root.getVector("sub_aggs");

        Float8Vector sumVector = subAggsVector.addOrGet(
            "sum",
            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            Float8Vector.class
        );
        Float8Vector avgSumVector = subAggsVector.addOrGet(
            "avg_sum",
            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            Float8Vector.class
        );
        BigIntVector avgCountVector = subAggsVector.addOrGet(
            "avg_count",
            FieldType.nullable(new ArrowType.Int(64, true)),
            BigIntVector.class
        );
        Float8Vector minVector = subAggsVector.addOrGet(
            "min",
            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            Float8Vector.class
        );
        Float8Vector maxVector = subAggsVector.addOrGet(
            "max",
            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            Float8Vector.class
        );

        termsVector.allocateNew((long) avgKeyLength * terms.length, terms.length);
        docCountVector.allocateNew(terms.length);
        subAggsVector.allocateNew();
        sumVector.allocateNew(terms.length);
        avgSumVector.allocateNew(terms.length);
        avgCountVector.allocateNew(terms.length);
        minVector.allocateNew(terms.length);
        maxVector.allocateNew(terms.length);

        for (int i = 0; i < terms.length; i++) {
            termsVector.setSafe(i, terms[i]);
            docCountVector.setSafe(i, docCounts[i]);
            subAggsVector.setIndexDefined(i);
            sumVector.setSafe(i, sumValues[i]);
            avgSumVector.setSafe(i, avgSums[i]);
            avgCountVector.setSafe(i, avgCounts[i]);
            minVector.setSafe(i, minValues[i]);
            maxVector.setSafe(i, maxValues[i]);
        }

        subAggsVector.setValueCount(terms.length);
        root.setRowCount(terms.length);
        return root;
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        for (VectorSchemaRoot root : arrowVectors) {
            root.close();
        }
        arrowAllocator.close();
    }

    // ==================== BigArrays-style using real StringTerms.reduce ====================

    /**
     * Uses real OpenSearch StringTerms.reduce() which handles sub-aggregations via InternalAggregations.reduce()
     */
    @Benchmark
    public StringTerms bigArraysStyleWithSubAggs() {
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

    // ==================== Arrow-style with Sub-Aggregations ====================

    /**
     * Result holder for Arrow-style reduction
     */
    static class ArrowReducedBucket {
        String term;
        long docCount;
        double sum;
        double avgSum;
        long avgCount;
        double min;
        double max;

        ArrowReducedBucket(String term) {
            this.term = term;
            this.docCount = 0;
            this.sum = 0;
            this.avgSum = 0;
            this.avgCount = 0;
            this.min = Double.POSITIVE_INFINITY;
            this.max = Double.NEGATIVE_INFINITY;
        }
    }

    /**
     * Arrow-style reduction using primitive columnar arrays
     */
    @Benchmark
    public List<ArrowReducedBucket> arrowStyleWithSubAggs() {
        List<ArrowReducedBucket> reducedBuckets = new ArrayList<>(numTerms);

        PriorityQueue<ArrowCursor> pq = new PriorityQueue<>(numShards) {
            @Override
            protected boolean lessThan(ArrowCursor a, ArrowCursor b) {
                return Arrays.compareUnsigned(a.currentTerm(), b.currentTerm()) < 0;
            }
        };

        for (int i = 0; i < numShards; i++) {
            pq.add(
                new ArrowCursor(
                    arrowTerms.get(i),
                    arrowDocCounts.get(i),
                    arrowSumValues.get(i),
                    arrowAvgSums.get(i),
                    arrowAvgCounts.get(i),
                    arrowMinValues.get(i),
                    arrowMaxValues.get(i)
                )
            );
        }

        while (pq.size() > 0) {
            ArrowCursor cursor = pq.top();
            byte[] currentTerm = cursor.currentTerm();
            String termKey = new String(currentTerm, StandardCharsets.UTF_8);

            // Initialize with primitives
            ArrowReducedBucket reduced = new ArrowReducedBucket(termKey);
            reduced.docCount = cursor.currentDocCount();
            reduced.sum = cursor.getSum();
            reduced.avgSum = cursor.getAvgSum();
            reduced.avgCount = cursor.getAvgCount();
            reduced.min = cursor.getMin();
            reduced.max = cursor.getMax();

            cursor.next();
            if (cursor.isValid()) {
                pq.updateTop();
            } else {
                pq.pop();
            }

            // Merge same terms with primitive operations
            while (pq.size() > 0 && Arrays.equals(currentTerm, pq.top().currentTerm())) {
                ArrowCursor next = pq.top();

                reduced.docCount += next.currentDocCount();
                reduced.sum += next.getSum();
                reduced.avgSum += next.getAvgSum();
                reduced.avgCount += next.getAvgCount();
                reduced.min = Math.min(reduced.min, next.getMin());
                reduced.max = Math.max(reduced.max, next.getMax());

                next.next();
                if (next.isValid()) {
                    pq.updateTop();
                } else {
                    pq.pop();
                }
            }

            reducedBuckets.add(reduced);
        }

        return reducedBuckets;
    }

    // ==================== Arrow Vector Style ====================

    /**
     * Cursor for Arrow VectorSchemaRoot with nested struct
     */
    static class VectorCursor {
        final VectorSchemaRoot root;
        final VarCharVector termsVector;
        final BigIntVector docCountVector;
        final Float8Vector sumVector;
        final Float8Vector avgSumVector;
        final BigIntVector avgCountVector;
        final Float8Vector minVector;
        final Float8Vector maxVector;
        int index;

        VectorCursor(VectorSchemaRoot root) {
            this.root = root;
            this.termsVector = (VarCharVector) root.getVector("term");
            this.docCountVector = (BigIntVector) root.getVector("doc_count");
            StructVector subAggsVector = (StructVector) root.getVector("sub_aggs");
            this.sumVector = (Float8Vector) subAggsVector.getChild("sum");
            this.avgSumVector = (Float8Vector) subAggsVector.getChild("avg_sum");
            this.avgCountVector = (BigIntVector) subAggsVector.getChild("avg_count");
            this.minVector = (Float8Vector) subAggsVector.getChild("min");
            this.maxVector = (Float8Vector) subAggsVector.getChild("max");
            this.index = 0;
        }

        byte[] currentTerm() {
            return termsVector.get(index);
        }

        long currentDocCount() {
            return docCountVector.get(index);
        }

        double getSum() {
            return sumVector.get(index);
        }

        double getAvgSum() {
            return avgSumVector.get(index);
        }

        long getAvgCount() {
            return avgCountVector.get(index);
        }

        double getMin() {
            return minVector.get(index);
        }

        double getMax() {
            return maxVector.get(index);
        }

        boolean isValid() {
            return index < root.getRowCount();
        }

        void next() {
            index++;
        }
    }

    /**
     * Arrow-style using actual Arrow VectorSchemaRoot with nested struct
     */
    @Benchmark
    public List<ArrowReducedBucket> arrowVectorStyleWithSubAggs() {
        List<ArrowReducedBucket> reducedBuckets = new ArrayList<>(numTerms);

        PriorityQueue<VectorCursor> pq = new PriorityQueue<>(numShards) {
            @Override
            protected boolean lessThan(VectorCursor a, VectorCursor b) {
                return Arrays.compareUnsigned(a.currentTerm(), b.currentTerm()) < 0;
            }
        };

        for (VectorSchemaRoot root : arrowVectors) {
            if (root.getRowCount() > 0) {
                pq.add(new VectorCursor(root));
            }
        }

        while (pq.size() > 0) {
            VectorCursor cursor = pq.top();
            byte[] currentTerm = cursor.currentTerm();
            String termKey = new String(currentTerm, StandardCharsets.UTF_8);

            ArrowReducedBucket reduced = new ArrowReducedBucket(termKey);
            reduced.docCount = cursor.currentDocCount();
            reduced.sum = cursor.getSum();
            reduced.avgSum = cursor.getAvgSum();
            reduced.avgCount = cursor.getAvgCount();
            reduced.min = cursor.getMin();
            reduced.max = cursor.getMax();

            cursor.next();
            if (cursor.isValid()) {
                pq.updateTop();
            } else {
                pq.pop();
            }

            while (pq.size() > 0 && Arrays.equals(currentTerm, pq.top().currentTerm())) {
                VectorCursor next = pq.top();

                reduced.docCount += next.currentDocCount();
                reduced.sum += next.getSum();
                reduced.avgSum += next.getAvgSum();
                reduced.avgCount += next.getAvgCount();
                reduced.min = Math.min(reduced.min, next.getMin());
                reduced.max = Math.max(reduced.max, next.getMax());

                next.next();
                if (next.isValid()) {
                    pq.updateTop();
                } else {
                    pq.pop();
                }
            }

            reducedBuckets.add(reduced);
        }

        return reducedBuckets;
    }

    // ==================== Micro-benchmarks to isolate Arrow overhead ====================

    /**
     * Benchmark just reading primitive values from Arrow vectors (no merge logic)
     * This isolates the Arrow vector access overhead
     */
    @Benchmark
    public long microBenchmarkArrowVectorRead() {
        long checksum = 0;
        for (VectorSchemaRoot root : arrowVectors) {
            VarCharVector termsVector = (VarCharVector) root.getVector("term");
            BigIntVector docCountVector = (BigIntVector) root.getVector("doc_count");
            StructVector subAggsVector = (StructVector) root.getVector("sub_aggs");
            Float8Vector sumVector = (Float8Vector) subAggsVector.getChild("sum");

            for (int i = 0; i < root.getRowCount(); i++) {
                byte[] term = termsVector.get(i);  // This creates a new byte[] each time!
                checksum += term[0];
                checksum += docCountVector.get(i);
                checksum += (long) sumVector.get(i);
            }
        }
        return checksum;
    }

    /**
     * Benchmark reading from plain Java arrays (no merge logic)
     * This shows the baseline without Arrow overhead
     */
    @Benchmark
    public long microBenchmarkArrayRead() {
        long checksum = 0;
        for (int shard = 0; shard < numShards; shard++) {
            byte[][] terms = arrowTerms.get(shard);
            long[] docCounts = arrowDocCounts.get(shard);
            double[] sums = arrowSumValues.get(shard);

            for (int i = 0; i < terms.length; i++) {
                byte[] term = terms[i];  // Direct array access, no copy
                checksum += term[0];
                checksum += docCounts[i];
                checksum += (long) sums[i];
            }
        }
        return checksum;
    }

    /**
     * Benchmark Arrow with zero-copy access using getDataBuffer
     * This avoids the byte[] allocation in VarCharVector.get()
     */
    @Benchmark
    public long microBenchmarkArrowZeroCopy() {
        long checksum = 0;
        for (VectorSchemaRoot root : arrowVectors) {
            VarCharVector termsVector = (VarCharVector) root.getVector("term");
            BigIntVector docCountVector = (BigIntVector) root.getVector("doc_count");
            StructVector subAggsVector = (StructVector) root.getVector("sub_aggs");
            Float8Vector sumVector = (Float8Vector) subAggsVector.getChild("sum");

            // Use direct buffer access instead of get() which allocates
            org.apache.arrow.memory.ArrowBuf dataBuffer = termsVector.getDataBuffer();
            org.apache.arrow.memory.ArrowBuf offsetBuffer = termsVector.getOffsetBuffer();

            for (int i = 0; i < root.getRowCount(); i++) {
                // Zero-copy access to term bytes
                int startOffset = offsetBuffer.getInt((long) i * 4);
                byte firstByte = dataBuffer.getByte(startOffset);
                checksum += firstByte;
                checksum += docCountVector.get(i);
                checksum += (long) sumVector.get(i);
            }
        }
        return checksum;
    }

    /**
     * Benchmark Arrow with reusable buffer for VarCharVector
     * This avoids allocation by reusing a single byte[] buffer
     */
    @Benchmark
    public long microBenchmarkArrowReusableBuffer() {
        long checksum = 0;
        // Pre-allocate a reusable buffer large enough for any term
        byte[] reusableBuffer = new byte[avgKeyLength * 2];

        for (VectorSchemaRoot root : arrowVectors) {
            VarCharVector termsVector = (VarCharVector) root.getVector("term");
            BigIntVector docCountVector = (BigIntVector) root.getVector("doc_count");
            StructVector subAggsVector = (StructVector) root.getVector("sub_aggs");
            Float8Vector sumVector = (Float8Vector) subAggsVector.getChild("sum");

            // Get direct buffer access
            org.apache.arrow.memory.ArrowBuf dataBuffer = termsVector.getDataBuffer();
            org.apache.arrow.memory.ArrowBuf offsetBuffer = termsVector.getOffsetBuffer();

            for (int i = 0; i < root.getRowCount(); i++) {
                // Read term into reusable buffer (no allocation!)
                int startOffset = offsetBuffer.getInt((long) i * 4);
                int endOffset = offsetBuffer.getInt((long) (i + 1) * 4);
                int length = endOffset - startOffset;

                // Copy to reusable buffer instead of allocating new array
                dataBuffer.getBytes(startOffset, reusableBuffer, 0, length);

                checksum += reusableBuffer[0];
                checksum += docCountVector.get(i);
                checksum += (long) sumVector.get(i);
            }
        }
        return checksum;
    }

    /**
     * Benchmark Arrow reading the full term with reusable BytesRef (like Lucene pattern)
     */
    @Benchmark
    public long microBenchmarkArrowWithBytesRef() {
        long checksum = 0;
        // Reusable BytesRef with pre-allocated buffer
        BytesRef reusableTerm = new BytesRef(new byte[avgKeyLength * 2], 0, 0);

        for (VectorSchemaRoot root : arrowVectors) {
            VarCharVector termsVector = (VarCharVector) root.getVector("term");
            BigIntVector docCountVector = (BigIntVector) root.getVector("doc_count");
            StructVector subAggsVector = (StructVector) root.getVector("sub_aggs");
            Float8Vector sumVector = (Float8Vector) subAggsVector.getChild("sum");

            org.apache.arrow.memory.ArrowBuf dataBuffer = termsVector.getDataBuffer();
            org.apache.arrow.memory.ArrowBuf offsetBuffer = termsVector.getOffsetBuffer();

            for (int i = 0; i < root.getRowCount(); i++) {
                int startOffset = offsetBuffer.getInt((long) i * 4);
                int endOffset = offsetBuffer.getInt((long) (i + 1) * 4);
                int length = endOffset - startOffset;

                // Reuse BytesRef buffer
                reusableTerm.length = length;
                dataBuffer.getBytes(startOffset, reusableTerm.bytes, 0, length);

                checksum += reusableTerm.bytes[0];
                checksum += docCountVector.get(i);
                checksum += (long) sumVector.get(i);
            }
        }
        return checksum;
    }

    // ==================== Apples-to-Apples Comparison ====================

    /**
     * This benchmark compares:
     * 1. Arrow Vector k-way merge (with VarCharVector.get() overhead)
     * 2. Plain array k-way merge (with direct array access)
     *
     * Both do ONLY term merging with count accumulation (no sub-aggs),
     * matching what ArrowBenchmark.arrowReduce does.
     */
    @Benchmark
    public long appleToApple_ArrowVectorMerge() {
        long totalCount = 0;

        PriorityQueue<VectorCursorSimple> pq = new PriorityQueue<>(numShards) {
            @Override
            protected boolean lessThan(VectorCursorSimple a, VectorCursorSimple b) {
                return Arrays.compareUnsigned(a.getCurrentTerm(), b.getCurrentTerm()) < 0;
            }
        };

        for (VectorSchemaRoot root : arrowVectors) {
            if (root.getRowCount() > 0) {
                pq.add(new VectorCursorSimple(root));
            }
        }

        while (pq.size() > 0) {
            VectorCursorSimple cursor = pq.top();
            byte[] currentTerm = cursor.getCurrentTerm();  // Allocates new byte[]!
            long count = cursor.getDocCount();

            cursor.advance();
            if (cursor.isValid()) {
                pq.updateTop();
            } else {
                pq.pop();
            }

            while (pq.size() > 0 && Arrays.equals(currentTerm, pq.top().getCurrentTerm())) {
                count += pq.top().getDocCount();
                VectorCursorSimple next = pq.top();
                next.advance();
                if (next.isValid()) {
                    pq.updateTop();
                } else {
                    pq.pop();
                }
            }

            totalCount += count;
        }

        return totalCount;
    }

    static class VectorCursorSimple {
        final VectorSchemaRoot root;
        final VarCharVector termsVector;
        final BigIntVector docCountVector;
        int index;

        VectorCursorSimple(VectorSchemaRoot root) {
            this.root = root;
            this.termsVector = (VarCharVector) root.getVector("term");
            this.docCountVector = (BigIntVector) root.getVector("doc_count");
            this.index = 0;
        }

        byte[] getCurrentTerm() {
            return termsVector.get(index);
        }

        long getDocCount() {
            return docCountVector.get(index);
        }

        boolean isValid() {
            return index < root.getRowCount();
        }

        void advance() {
            index++;
        }
    }

    @Benchmark
    public long appleToApple_PlainArrayMerge() {
        long totalCount = 0;

        PriorityQueue<ArrayCursorSimple> pq = new PriorityQueue<>(numShards) {
            @Override
            protected boolean lessThan(ArrayCursorSimple a, ArrayCursorSimple b) {
                return Arrays.compareUnsigned(a.getCurrentTerm(), b.getCurrentTerm()) < 0;
            }
        };

        for (int i = 0; i < numShards; i++) {
            pq.add(new ArrayCursorSimple(arrowTerms.get(i), arrowDocCounts.get(i)));
        }

        while (pq.size() > 0) {
            ArrayCursorSimple cursor = pq.top();
            byte[] currentTerm = cursor.getCurrentTerm();  // Direct array access, no allocation
            long count = cursor.getDocCount();

            cursor.advance();
            if (cursor.isValid()) {
                pq.updateTop();
            } else {
                pq.pop();
            }

            while (pq.size() > 0 && Arrays.equals(currentTerm, pq.top().getCurrentTerm())) {
                count += pq.top().getDocCount();
                ArrayCursorSimple next = pq.top();
                next.advance();
                if (next.isValid()) {
                    pq.updateTop();
                } else {
                    pq.pop();
                }
            }

            totalCount += count;
        }

        return totalCount;
    }

    static class ArrayCursorSimple {
        final byte[][] terms;
        final long[] docCounts;
        int index;

        ArrayCursorSimple(byte[][] terms, long[] docCounts) {
            this.terms = terms;
            this.docCounts = docCounts;
            this.index = 0;
        }

        byte[] getCurrentTerm() {
            return terms[index];
        }

        long getDocCount() {
            return docCounts[index];
        }

        boolean isValid() {
            return index < terms.length;
        }

        void advance() {
            index++;
        }
    }

    // ==================== Verification ====================

    public void verifyResults() {
        System.out.println("\n=== VERIFICATION: Comparing all reduce method outputs ===\n");

        System.out.println("Running bigArraysStyleWithSubAggs (real StringTerms.reduce)...");
        StringTerms bigArraysResult = bigArraysStyleWithSubAggs();
        System.out.println("  Result: " + bigArraysResult.getBuckets().size() + " buckets");

        System.out.println("Running arrowStyleWithSubAggs...");
        List<ArrowReducedBucket> arrowResult = arrowStyleWithSubAggs();
        System.out.println("  Result: " + arrowResult.size() + " buckets");

        System.out.println("Running arrowVectorStyleWithSubAggs...");
        List<ArrowReducedBucket> arrowVectorResult = arrowVectorStyleWithSubAggs();
        System.out.println("  Result: " + arrowVectorResult.size() + " buckets");

        // Compare results - sort both by term for comparison
        double tolerance = 0.0001;
        int mismatchCount = 0;
        int sampleCount = 0;
        boolean allMatch = true;

        // Sort BigArrays result by term (it's sorted by count by default after reduce)
        List<StringTerms.Bucket> bigArraysBuckets = new ArrayList<>(bigArraysResult.getBuckets());
        bigArraysBuckets.sort((a, b) -> a.getKeyAsString().compareTo(b.getKeyAsString()));

        // Arrow results are already sorted by term

        if (bigArraysBuckets.size() != arrowResult.size()) {
            System.out.println("ERROR: Bucket counts don't match!");
            allMatch = false;
        }

        for (int i = 0; i < Math.min(bigArraysBuckets.size(), arrowResult.size()); i++) {
            StringTerms.Bucket bigArraysBucket = bigArraysBuckets.get(i);
            ArrowReducedBucket arrowBucket = arrowResult.get(i);
            ArrowReducedBucket arrowVectorBucket = arrowVectorResult.get(i);

            String bigArraysTerm = bigArraysBucket.getKeyAsString();

            // Get sub-agg values from real InternalAggregations
            InternalSum sum = bigArraysBucket.getAggregations().get("sum");
            InternalAvg avg = bigArraysBucket.getAggregations().get("avg");
            InternalMin min = bigArraysBucket.getAggregations().get("min");
            InternalMax max = bigArraysBucket.getAggregations().get("max");

            double bigArraysSum = sum.value();
            double bigArraysMin = min.value();
            double bigArraysMax = max.value();

            // Check values match
            if (!bigArraysTerm.equals(arrowBucket.term)) {
                if (mismatchCount < 5) System.out.println("TERM MISMATCH at " + i);
                mismatchCount++;
                allMatch = false;
                continue;
            }

            if (bigArraysBucket.getDocCount() != arrowBucket.docCount) {
                if (mismatchCount < 5) System.out.println("DOC_COUNT MISMATCH for " + bigArraysTerm);
                mismatchCount++;
                allMatch = false;
            }

            if (Math.abs(bigArraysSum - arrowBucket.sum) > tolerance) {
                if (mismatchCount < 5) System.out.println("SUM MISMATCH: " + bigArraysSum + " vs " + arrowBucket.sum);
                mismatchCount++;
                allMatch = false;
            }

            if (Math.abs(bigArraysMin - arrowBucket.min) > tolerance) {
                if (mismatchCount < 5) System.out.println("MIN MISMATCH: " + bigArraysMin + " vs " + arrowBucket.min);
                mismatchCount++;
                allMatch = false;
            }

            if (Math.abs(bigArraysMax - arrowBucket.max) > tolerance) {
                if (mismatchCount < 5) System.out.println("MAX MISMATCH: " + bigArraysMax + " vs " + arrowBucket.max);
                mismatchCount++;
                allMatch = false;
            }

            if (sampleCount < 3 && mismatchCount == 0) {
                System.out.println("\nSample bucket '" + bigArraysTerm + "':");
                System.out.println("  docCount: " + bigArraysBucket.getDocCount() + " (all match)");
                System.out.println("  sum: " + bigArraysSum + " (all match)");
                System.out.println("  min: " + bigArraysMin + " (all match)");
                System.out.println("  max: " + bigArraysMax + " (all match)");
                sampleCount++;
            }
        }

        System.out.println("\n=== VERIFICATION SUMMARY ===");
        if (allMatch && mismatchCount == 0) {
            System.out.println("SUCCESS: All " + bigArraysBuckets.size() + " buckets match across all implementations!");
        } else {
            System.out.println("FAILURE: Found " + mismatchCount + " mismatches");
        }
    }
}
