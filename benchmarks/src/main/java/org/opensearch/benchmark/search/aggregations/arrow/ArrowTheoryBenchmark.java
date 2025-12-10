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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Micro-benchmarks to test specific theories about why Arrow-based reduction is faster.
 *
 * Theory 1: Object Allocation Overhead - BigArrays creates Bucket objects, Arrow uses primitives
 * Theory 2: Memory Access Patterns - Arrow's columnar layout is more cache-friendly
 * Theory 3: Comparator Overhead - Generic comparator vs Arrays.compareUnsigned intrinsic
 * Theory 4: Reduction Complexity - BigArrays handles error tracking and sub-aggs
 * Theory 5: Iterator vs Index Access - Virtual dispatch vs direct array indexing
 *
 * Distribution modes:
 * - PARTITIONED: Each term appears in exactly one shard (best case for Arrow, no merge needed)
 * - UNIFORM: Terms uniformly distributed, each term appears in all shards
 * - ZIPF: Zipf distribution - popular terms in all shards, rare terms in few shards
 */
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = {
    "--add-modules=jdk.incubator.vector",
    "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED" })
public class ArrowTheoryBenchmark {

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

    @Param({ "100000" })
    private int numTerms;

    @Param({ "100" })
    private int avgKeyLength;

    @Param({ "10" })
    private int numShards;

    @Param({ "PARTITIONED", "UNIFORM", "ZIPF" })
    private DistributionMode distributionMode;

    // Data structures for different theories
    private List<byte[][]> arrowStyleTerms;      // Columnar: byte[][] per shard
    private List<int[]> arrowStyleCounts;         // Columnar: int[] per shard
    private List<List<BucketObject>> objectStyleBuckets;  // Row-oriented: List<Bucket> per shard
    private List<List<LightweightBucket>> lightweightBuckets;  // Lightweight objects

    private BufferAllocator arrowAllocator;
    private List<VectorSchemaRoot> arrowVectors;

    /**
     * Heavy bucket object similar to StringTerms.Bucket
     */
    static class BucketObject {
        BytesRef termBytes;
        long docCount;
        long docCountError;
        boolean showDocCountError;
        Object aggregations;  // Simulates InternalAggregations reference
        Object format;        // Simulates DocValueFormat reference

        BucketObject(byte[] term, int count) {
            this.termBytes = new BytesRef(term);
            this.docCount = count;
            this.docCountError = 0;
            this.showDocCountError = true;
            this.aggregations = null;
            this.format = null;
        }

        int compareKey(BucketObject other) {
            return termBytes.compareTo(other.termBytes);
        }
    }

    /**
     * Lightweight bucket with minimal fields
     */
    static class LightweightBucket {
        byte[] term;
        int count;

        LightweightBucket(byte[] term, int count) {
            this.term = term;
            this.count = count;
        }
    }

    /**
     * Iterator wrapper similar to IteratorAndCurrent
     */
    static class IteratorAndCurrent<T> {
        private final Iterator<T> iterator;
        private T current;

        IteratorAndCurrent(Iterator<T> iterator) {
            this.iterator = iterator;
            this.current = iterator.next();
        }

        T current() {
            return current;
        }

        boolean hasNext() {
            return iterator.hasNext();
        }

        void next() {
            current = iterator.next();
        }
    }

    /**
     * Index-based cursor (no iterator)
     */
    static class IndexCursor<T> {
        private final List<T> list;
        private int index;

        IndexCursor(List<T> list) {
            this.list = list;
            this.index = 0;
        }

        T current() {
            return list.get(index);
        }

        boolean hasNext() {
            return index < list.size() - 1;
        }

        void next() {
            index++;
        }

        boolean isValid() {
            return index < list.size();
        }
    }

    /**
     * Array-based cursor for columnar data
     */
    static class ArrayCursor {
        final byte[][] terms;
        final int[] counts;
        int index;

        ArrayCursor(byte[][] terms, int[] counts) {
            this.terms = terms;
            this.counts = counts;
            this.index = 0;
        }

        byte[] currentTerm() {
            return terms[index];
        }

        int currentCount() {
            return counts[index];
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

    private Random random;
    private String[] termDictionary;  // Pre-generated string terms (index = termId)

    @Setup(Level.Trial)
    public void setup() {
        random = new Random(42);

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

        // Initialize data structures
        arrowStyleTerms = new ArrayList<>(numShards);
        arrowStyleCounts = new ArrayList<>(numShards);
        objectStyleBuckets = new ArrayList<>(numShards);
        lightweightBuckets = new ArrayList<>(numShards);

        arrowAllocator = new RootAllocator(Long.MAX_VALUE);
        arrowVectors = new ArrayList<>(numShards);

        // Create shard data from the distributed counts
        for (int shard = 0; shard < numShards; shard++) {
            Map<String, Integer> termCounts = shardTermCounts.get(shard);
            int termsInShard = termCounts.size();

            // Sort entries by term key for merge-sort simulation
            List<Map.Entry<String, Integer>> sortedEntries = termCounts.entrySet().stream().sorted(Map.Entry.comparingByKey()).toList();

            byte[][] shardTerms = new byte[termsInShard][];
            int[] shardCounts = new int[termsInShard];
            List<BucketObject> buckets = new ArrayList<>(termsInShard);
            List<LightweightBucket> lightBuckets = new ArrayList<>(termsInShard);

            for (int i = 0; i < sortedEntries.size(); i++) {
                Map.Entry<String, Integer> entry = sortedEntries.get(i);
                byte[] term = entry.getKey().getBytes(StandardCharsets.UTF_8);
                int count = entry.getValue();

                shardTerms[i] = term;
                shardCounts[i] = count;
                buckets.add(new BucketObject(term, count));
                lightBuckets.add(new LightweightBucket(term, count));
            }

            arrowStyleTerms.add(shardTerms);
            arrowStyleCounts.add(shardCounts);
            objectStyleBuckets.add(buckets);
            lightweightBuckets.add(lightBuckets);

            // Create Arrow vectors
            if (termsInShard > 0) {
                arrowVectors.add(createArrowVector(shardTerms, shardCounts));
            }
        }

        // Log statistics about the distribution
        logDistributionStats(shardTermCounts);
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
     */
    private void generatePartitionedData(List<Map<String, Integer>> shardTermCounts) {
        int termsPerShard = numTerms / numShards;
        // Use a larger document count to ensure good bucket counts
        int totalDocuments = numTerms * 10;

        for (int docId = 0; docId < totalDocuments; docId++) {
            int shard = Math.abs(random.nextInt()) % numShards;
            int termIdInShard = Math.abs(random.nextInt()) % termsPerShard;
            int termId = shard * termsPerShard + termIdInShard;
            if (termId < numTerms) {
                String term = termDictionary[termId];
                shardTermCounts.get(shard).merge(term, 1, Integer::sum);
            }
        }
    }

    /**
     * UNIFORM: Each document goes to random shard, term selected uniformly.
     * All terms appear in all shards with similar counts.
     */
    private void generateUniformData(List<Map<String, Integer>> shardTermCounts) {
        int totalDocuments = numTerms * 10;

        for (int docId = 0; docId < totalDocuments; docId++) {
            int shard = Math.abs(random.nextInt()) % numShards;
            int termId = Math.abs(random.nextInt()) % numTerms;
            String term = termDictionary[termId];
            shardTermCounts.get(shard).merge(term, 1, Integer::sum);
        }
    }

    /**
     * ZIPF: Zipf distribution where popular terms are very common
     * and rare terms may only appear in a few shards.
     */
    private void generateZipfData(List<Map<String, Integer>> shardTermCounts) {
        int totalDocuments = numTerms * 10;

        // Pre-compute Zipf CDF
        double[] zipfCdf = new double[numTerms];
        double zipfSum = 0.0;
        for (int k = 1; k <= numTerms; k++) {
            zipfSum += 1.0 / k;
            zipfCdf[k - 1] = zipfSum;
        }
        for (int k = 0; k < numTerms; k++) {
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

    private VectorSchemaRoot createArrowVector(byte[][] terms, int[] counts) {
        Schema schema = new Schema(
            Arrays.asList(
                new Field("term", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("count", FieldType.nullable(new ArrowType.Int(32, true)), null)
            )
        );

        VectorSchemaRoot root = VectorSchemaRoot.create(schema, arrowAllocator);
        VarCharVector termsVector = (VarCharVector) root.getVector("term");
        IntVector countsVector = (IntVector) root.getVector("count");

        // Allocate enough space for all terms (avgKeyLength * numTerms for variable width)
        termsVector.allocateNew((long) avgKeyLength * terms.length, terms.length);
        countsVector.allocateNew(terms.length);

        for (int i = 0; i < terms.length; i++) {
            termsVector.setSafe(i, terms[i]);
            countsVector.setSafe(i, counts[i]);
        }

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

    // ==================== THEORY 1: Object Allocation Overhead ====================

    /**
     * Theory 1A: Heavy object allocation (like StringTerms.Bucket)
     * Follows exact pattern of InternalTerms.reduceMergeSort:
     * - Buffer buckets with same key into List
     * - Call reduceBucket to create new merged bucket
     */
    @Benchmark
    public List<BucketObject> theory1_heavyObjectAllocation() {
        List<BucketObject> reducedBuckets = new ArrayList<>();
        Comparator<BucketObject> cmp = (a, b) -> a.termBytes.compareTo(b.termBytes);

        PriorityQueue<IteratorAndCurrent<BucketObject>> pq = new PriorityQueue<>(numShards) {
            @Override
            protected boolean lessThan(IteratorAndCurrent<BucketObject> a, IteratorAndCurrent<BucketObject> b) {
                return cmp.compare(a.current(), b.current()) < 0;
            }
        };

        for (List<BucketObject> buckets : objectStyleBuckets) {
            if (!buckets.isEmpty()) {
                pq.add(new IteratorAndCurrent<>(buckets.iterator()));
            }
        }

        // Exact pattern from InternalTerms.reduceMergeSort
        List<BucketObject> currentBuckets = new ArrayList<>();
        BucketObject lastBucket = null;

        while (pq.size() > 0) {
            IteratorAndCurrent<BucketObject> top = pq.top();

            if (lastBucket != null && cmp.compare(top.current(), lastBucket) != 0) {
                // Key changed - reduce buffered buckets (like reduceBucket)
                BucketObject reduced = reduceBucket(currentBuckets);
                reducedBuckets.add(reduced);
                currentBuckets.clear();
            }

            lastBucket = top.current();
            currentBuckets.add(top.current());

            if (top.hasNext()) {
                top.next();
                pq.updateTop();
            } else {
                pq.pop();
            }
        }

        // Final reduction
        if (!currentBuckets.isEmpty()) {
            BucketObject reduced = reduceBucket(currentBuckets);
            reducedBuckets.add(reduced);
        }

        return reducedBuckets;
    }

    /**
     * Simulates InternalTerms.reduceBucket - creates new bucket object
     */
    private BucketObject reduceBucket(List<BucketObject> buckets) {
        long docCount = 0;
        long docCountError = 0;
        List<Object> aggregationsList = new ArrayList<>();

        for (BucketObject bucket : buckets) {
            docCount += bucket.docCount;
            if (docCountError != -1) {
                if (!bucket.showDocCountError) {
                    docCountError = -1;
                } else {
                    docCountError += bucket.docCountError;
                }
            }
            if (bucket.aggregations != null) {
                aggregationsList.add(bucket.aggregations);
            }
        }

        // Create new bucket (like createBucket in InternalTerms)
        BucketObject reduced = new BucketObject(buckets.get(0).termBytes.bytes, (int) docCount);
        reduced.docCountError = docCountError;
        reduced.showDocCountError = buckets.get(0).showDocCountError;
        // In real code, would also merge sub-aggregations here
        return reduced;
    }

    /**
     * Theory 1B: Primitive-only accumulation (like Arrow approach)
     * No object creation during merge
     */
    @Benchmark
    public Map<String, Long> theory1_primitiveAccumulation() {
        Map<String, Long> merged = new HashMap<>();

        PriorityQueue<ArrayCursor> pq = new PriorityQueue<>(numShards) {
            @Override
            protected boolean lessThan(ArrayCursor a, ArrayCursor b) {
                return Arrays.compareUnsigned(a.currentTerm(), b.currentTerm()) < 0;
            }
        };

        for (int i = 0; i < numShards; i++) {
            pq.add(new ArrayCursor(arrowStyleTerms.get(i), arrowStyleCounts.get(i)));
        }

        while (pq.size() > 0) {
            ArrayCursor cursor = pq.top();
            byte[] currentTerm = cursor.currentTerm();
            long totalCount = cursor.currentCount();  // Primitive accumulation

            cursor.next();
            if (cursor.isValid()) {
                pq.updateTop();
            } else {
                pq.pop();
            }

            while (pq.size() > 0 && Arrays.equals(currentTerm, pq.top().currentTerm())) {
                ArrayCursor next = pq.top();
                totalCount += next.currentCount();  // Just add primitives
                next.next();
                if (next.isValid()) {
                    pq.updateTop();
                } else {
                    pq.pop();
                }
            }

            merged.put(new String(currentTerm, StandardCharsets.UTF_8), totalCount);
        }

        return merged;
    }

    // ==================== THEORY 2: Memory Access Patterns ====================

    /**
     * Theory 2A: Row-oriented access (object fields scattered in heap)
     * Access pattern: bucket.term, bucket.count for each bucket
     */
    @Benchmark
    public long theory2_rowOrientedAccess() {
        long checksum = 0;

        for (List<BucketObject> shardBuckets : objectStyleBuckets) {
            for (BucketObject bucket : shardBuckets) {
                // Access term and count from same object (pointer chase)
                checksum += bucket.termBytes.bytes[0];
                checksum += bucket.docCount;
            }
        }

        return checksum;
    }

    /**
     * Theory 2B: Columnar access (contiguous arrays)
     * Access pattern: all terms, then all counts
     */
    @Benchmark
    public long theory2_columnarAccess() {
        long checksum = 0;

        for (int shard = 0; shard < numShards; shard++) {
            byte[][] terms = arrowStyleTerms.get(shard);
            int[] counts = arrowStyleCounts.get(shard);

            // Sequential access through contiguous memory
            for (byte[] term : terms) {
                checksum += term[0];
            }
            for (int count : counts) {
                checksum += count;
            }
        }

        return checksum;
    }

    /**
     * Theory 2C: Interleaved columnar access (how merge actually works)
     */
    @Benchmark
    public long theory2_interleavedColumnarAccess() {
        long checksum = 0;

        for (int shard = 0; shard < numShards; shard++) {
            byte[][] terms = arrowStyleTerms.get(shard);
            int[] counts = arrowStyleCounts.get(shard);

            // Access term[i] and count[i] together, but arrays are contiguous
            for (int i = 0; i < terms.length; i++) {
                checksum += terms[i][0];
                checksum += counts[i];
            }
        }

        return checksum;
    }

    // ==================== THEORY 3: Comparator Overhead ====================

    /**
     * Theory 3A: Generic Comparator with virtual dispatch
     */
    @Benchmark
    public long theory3_genericComparator() {
        Comparator<BucketObject> cmp = (a, b) -> a.termBytes.compareTo(b.termBytes);

        long comparisons = 0;
        List<BucketObject> buckets = objectStyleBuckets.get(0);

        for (int i = 0; i < buckets.size() - 1; i++) {
            if (cmp.compare(buckets.get(i), buckets.get(i + 1)) < 0) {
                comparisons++;
            }
        }

        return comparisons;
    }

    /**
     * Theory 3B: BytesRef.compareTo (Lucene's comparison)
     */
    @Benchmark
    public long theory3_bytesRefComparator() {
        long comparisons = 0;
        List<BucketObject> buckets = objectStyleBuckets.get(0);

        for (int i = 0; i < buckets.size() - 1; i++) {
            if (buckets.get(i).termBytes.compareTo(buckets.get(i + 1).termBytes) < 0) {
                comparisons++;
            }
        }

        return comparisons;
    }

    /**
     * Theory 3C: Arrays.compareUnsigned intrinsic (JVM optimized)
     */
    @Benchmark
    public long theory3_arraysCompareUnsigned() {
        long comparisons = 0;
        byte[][] terms = arrowStyleTerms.get(0);

        for (int i = 0; i < terms.length - 1; i++) {
            if (Arrays.compareUnsigned(terms[i], terms[i + 1]) < 0) {
                comparisons++;
            }
        }

        return comparisons;
    }

    // ==================== THEORY 4: Reduction Logic Complexity ====================

    /**
     * Theory 4A: Full reduction logic (error tracking, sub-aggs simulation)
     */
    @Benchmark
    public Map<BytesRef, Long> theory4_fullReductionLogic() {
        Map<BytesRef, Long> merged = new HashMap<>();

        PriorityQueue<IndexCursor<BucketObject>> pq = new PriorityQueue<>(numShards) {
            @Override
            protected boolean lessThan(IndexCursor<BucketObject> a, IndexCursor<BucketObject> b) {
                return a.current().compareKey(b.current()) < 0;
            }
        };

        for (List<BucketObject> buckets : objectStyleBuckets) {
            if (!buckets.isEmpty()) {
                pq.add(new IndexCursor<>(buckets));
            }
        }

        while (pq.size() > 0) {
            IndexCursor<BucketObject> cursor = pq.top();
            BucketObject current = cursor.current();

            // Simulate reduceBucket with full logic
            long docCount = current.docCount;
            long docCountError = 0;
            List<Object> aggregationsList = new ArrayList<>();

            // Error tracking logic
            if (current.showDocCountError) {
                docCountError += current.docCountError;
            } else {
                docCountError = -1;
            }

            // Sub-aggregation handling
            if (current.aggregations != null) {
                aggregationsList.add(current.aggregations);
            }

            cursor.next();
            if (cursor.isValid()) {
                pq.updateTop();
            } else {
                pq.pop();
            }

            while (pq.size() > 0 && pq.top().current().termBytes.equals(current.termBytes)) {
                IndexCursor<BucketObject> next = pq.top();
                BucketObject nextBucket = next.current();

                docCount += nextBucket.docCount;

                // More error tracking
                if (docCountError != -1) {
                    if (!nextBucket.showDocCountError) {
                        docCountError = -1;
                    } else {
                        docCountError += nextBucket.docCountError;
                    }
                }

                if (nextBucket.aggregations != null) {
                    aggregationsList.add(nextBucket.aggregations);
                }

                next.next();
                if (next.isValid()) {
                    pq.updateTop();
                } else {
                    pq.pop();
                }
            }

            merged.put(BytesRef.deepCopyOf(current.termBytes), docCount);
        }

        return merged;
    }

    /**
     * Theory 4B: Minimal reduction logic (just count accumulation)
     */
    @Benchmark
    public Map<String, Long> theory4_minimalReductionLogic() {
        Map<String, Long> merged = new HashMap<>();

        PriorityQueue<ArrayCursor> pq = new PriorityQueue<>(numShards) {
            @Override
            protected boolean lessThan(ArrayCursor a, ArrayCursor b) {
                return Arrays.compareUnsigned(a.currentTerm(), b.currentTerm()) < 0;
            }
        };

        for (int i = 0; i < numShards; i++) {
            pq.add(new ArrayCursor(arrowStyleTerms.get(i), arrowStyleCounts.get(i)));
        }

        while (pq.size() > 0) {
            ArrayCursor cursor = pq.top();
            byte[] currentTerm = cursor.currentTerm();
            long totalCount = cursor.currentCount();

            cursor.next();
            if (cursor.isValid()) {
                pq.updateTop();
            } else {
                pq.pop();
            }

            while (pq.size() > 0 && Arrays.equals(currentTerm, pq.top().currentTerm())) {
                totalCount += pq.top().currentCount();
                ArrayCursor next = pq.top();
                next.next();
                if (next.isValid()) {
                    pq.updateTop();
                } else {
                    pq.pop();
                }
            }

            merged.put(new String(currentTerm, StandardCharsets.UTF_8), totalCount);
        }

        return merged;
    }

    // ==================== THEORY 5: Iterator vs Index Access ====================

    /**
     * Theory 5A: Iterator-based access with virtual dispatch
     */
    @Benchmark
    public long theory5_iteratorAccess() {
        long checksum = 0;

        for (List<BucketObject> shardBuckets : objectStyleBuckets) {
            Iterator<BucketObject> iter = shardBuckets.iterator();
            while (iter.hasNext()) {
                BucketObject bucket = iter.next();
                checksum += bucket.docCount;
            }
        }

        return checksum;
    }

    /**
     * Theory 5B: Index-based access on List
     */
    @Benchmark
    public long theory5_listIndexAccess() {
        long checksum = 0;

        for (List<BucketObject> shardBuckets : objectStyleBuckets) {
            for (int i = 0; i < shardBuckets.size(); i++) {
                checksum += shardBuckets.get(i).docCount;
            }
        }

        return checksum;
    }

    /**
     * Theory 5C: Direct array index access
     */
    @Benchmark
    public long theory5_arrayIndexAccess() {
        long checksum = 0;

        for (int[] counts : arrowStyleCounts) {
            for (int i = 0; i < counts.length; i++) {
                checksum += counts[i];
            }
        }

        return checksum;
    }

    /**
     * Theory 5D: Enhanced for-loop on array (compiler optimized)
     */
    @Benchmark
    public long theory5_enhancedForArray() {
        long checksum = 0;

        for (int[] counts : arrowStyleCounts) {
            for (int count : counts) {
                checksum += count;
            }
        }

        return checksum;
    }

    // ==================== COMBINED: Full merge comparison ====================

    /**
     * Combined: Iterator + Objects + Generic Comparator + Full Logic
     * (Simulates BigArrays approach)
     */
    @Benchmark
    public Map<BytesRef, Long> combined_bigArraysStyle() {
        Map<BytesRef, Long> merged = new HashMap<>();
        Comparator<BucketObject> cmp = (a, b) -> a.termBytes.compareTo(b.termBytes);

        PriorityQueue<IteratorAndCurrent<BucketObject>> pq = new PriorityQueue<>(numShards) {
            @Override
            protected boolean lessThan(IteratorAndCurrent<BucketObject> a, IteratorAndCurrent<BucketObject> b) {
                return cmp.compare(a.current(), b.current()) < 0;
            }
        };

        for (List<BucketObject> buckets : objectStyleBuckets) {
            if (!buckets.isEmpty()) {
                pq.add(new IteratorAndCurrent<>(buckets.iterator()));
            }
        }

        List<BucketObject> currentBuckets = new ArrayList<>();
        BucketObject lastBucket = null;

        while (pq.size() > 0) {
            IteratorAndCurrent<BucketObject> top = pq.top();

            if (lastBucket != null && cmp.compare(top.current(), lastBucket) != 0) {
                // Reduce buffered buckets
                long docCount = 0;
                long docCountError = 0;
                for (BucketObject b : currentBuckets) {
                    docCount += b.docCount;
                    if (docCountError != -1) {
                        if (!b.showDocCountError) {
                            docCountError = -1;
                        } else {
                            docCountError += b.docCountError;
                        }
                    }
                }
                merged.put(BytesRef.deepCopyOf(lastBucket.termBytes), docCount);
                currentBuckets.clear();
            }

            lastBucket = top.current();
            currentBuckets.add(top.current());

            if (top.hasNext()) {
                top.next();
                pq.updateTop();
            } else {
                pq.pop();
            }
        }

        // Final reduction
        if (!currentBuckets.isEmpty()) {
            long docCount = 0;
            for (BucketObject b : currentBuckets) {
                docCount += b.docCount;
            }
            merged.put(BytesRef.deepCopyOf(lastBucket.termBytes), docCount);
        }

        return merged;
    }

    /**
     * Combined: Array Index + Primitives + Arrays.compareUnsigned + Minimal Logic
     * (Simulates Arrow approach)
     */
    @Benchmark
    public Map<String, Long> combined_arrowStyle() {
        Map<String, Long> merged = new HashMap<>();

        PriorityQueue<ArrayCursor> pq = new PriorityQueue<>(numShards) {
            @Override
            protected boolean lessThan(ArrayCursor a, ArrayCursor b) {
                return Arrays.compareUnsigned(a.currentTerm(), b.currentTerm()) < 0;
            }
        };

        for (int i = 0; i < numShards; i++) {
            pq.add(new ArrayCursor(arrowStyleTerms.get(i), arrowStyleCounts.get(i)));
        }

        while (pq.size() > 0) {
            ArrayCursor cursor = pq.top();
            byte[] currentTerm = cursor.currentTerm();
            long totalCount = cursor.currentCount();

            cursor.next();
            if (cursor.isValid()) {
                pq.updateTop();
            } else {
                pq.pop();
            }

            while (pq.size() > 0 && Arrays.equals(currentTerm, pq.top().currentTerm())) {
                totalCount += pq.top().currentCount();
                ArrayCursor next = pq.top();
                next.next();
                if (next.isValid()) {
                    pq.updateTop();
                } else {
                    pq.pop();
                }
            }

            merged.put(new String(currentTerm, StandardCharsets.UTF_8), totalCount);
        }

        return merged;
    }
}
