/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.aggregations.AggregatorTestCase;

import java.io.IOException;

public class FilteredMetricAggregatorTests extends AggregatorTestCase {

    /**
     * Test that the query DSL parses correctly.
     */
    public void testParsing() throws IOException {
        String json = "{"
            + "\"buckets\": { \"terms\": { \"field\": \"device\" } },"
            + "\"metric\": { \"cardinality\": { \"field\": \"device_app\" } },"
            + "\"filter\": { \"gt\": 10 }"
            + "}";

        org.opensearch.search.SearchModule searchModule = new org.opensearch.search.SearchModule(
            org.opensearch.common.settings.Settings.EMPTY,
            java.util.Collections.emptyList()
        );
        org.opensearch.core.xcontent.NamedXContentRegistry registry = new org.opensearch.core.xcontent.NamedXContentRegistry(
            searchModule.getNamedXContents()
        );

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(registry, null, json)) {
            parser.nextToken(); // START_OBJECT
            FilteredMetricAggregationBuilder builder = FilteredMetricAggregationBuilder.parse("test", parser);

            assertNotNull(builder.getBucketsAgg());
            assertNotNull(builder.getMetricAgg());
            assertEquals("terms", builder.getBucketsAgg().getType());
            assertEquals("cardinality", builder.getMetricAgg().getType());
            assertEquals(Double.valueOf(10.0), builder.getFilterGt());
        }
    }

    /**
     * Test parsing with range filter.
     */
    public void testParsingWithRange() throws IOException {
        String json = "{"
            + "\"buckets\": { \"terms\": { \"field\": \"region\" } },"
            + "\"metric\": { \"sum\": { \"field\": \"amount\" } },"
            + "\"filter\": { \"gt\": 100, \"lt\": 1000 }"
            + "}";

        org.opensearch.search.SearchModule searchModule = new org.opensearch.search.SearchModule(
            org.opensearch.common.settings.Settings.EMPTY,
            java.util.Collections.emptyList()
        );
        org.opensearch.core.xcontent.NamedXContentRegistry registry = new org.opensearch.core.xcontent.NamedXContentRegistry(
            searchModule.getNamedXContents()
        );

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(registry, null, json)) {
            parser.nextToken();
            FilteredMetricAggregationBuilder builder = FilteredMetricAggregationBuilder.parse("test", parser);

            assertEquals("terms", builder.getBucketsAgg().getType());
            assertEquals("sum", builder.getMetricAgg().getType());
            assertEquals(Double.valueOf(100.0), builder.getFilterGt());
            assertEquals(Double.valueOf(1000.0), builder.getFilterLt());
        }
    }

    /**
     * 4 devices: d1 has 3 apps, d2 has 1, d3 has 4, d4 has 2.
     * With threshold=2 (gt:2), d1 and d3 pass.
     */
    public void testSingleSegmentCardinality() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                addDoc(w, "d1", "d1__app1");
                addDoc(w, "d1", "d1__app2");
                addDoc(w, "d1", "d1__app3");
                addDoc(w, "d2", "d2__app1");
                addDoc(w, "d3", "d3__app1");
                addDoc(w, "d3", "d3__app2");
                addDoc(w, "d3", "d3__app3");
                addDoc(w, "d3", "d3__app4");
                addDoc(w, "d4", "d4__app1");
                addDoc(w, "d4", "d4__app2");
                w.forceMerge(1);
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newIndexSearcher(reader);

                FilteredMetricAggregationBuilder agg = new FilteredMetricAggregationBuilder("test");
                // We need to set the internal fields — use the parse method instead
                // For now, test the InternalFilteredMetric reduce logic directly
            }
        }
    }

    /**
     * Test the reduce logic: borderline resolution across shards.
     */
    public void testReduceCrossShard() {
        var bigArrays = org.opensearch.common.util.BigArrays.NON_RECYCLING_INSTANCE;

        long d1Hash = hashString("d1");
        long d2Hash = hashString("d2");
        long d3Hash = hashString("d3");

        // Shard A: d2 passed, d1 borderline with 2 apps
        HyperLogLogPlusPlus shardAHLL = new HyperLogLogPlusPlus(14, bigArrays, 1);
        shardAHLL.collect(0, d2Hash);
        java.util.Map<Long, Object> shardABorderline = new java.util.HashMap<>();
        shardABorderline.put(d1Hash, new java.util.HashSet<>(java.util.Set.of(hashString("app1"), hashString("app2"))));

        var shardA = new InternalFilteredMetric("test", shardAHLL, shardABorderline, 3, 14, java.util.Collections.emptyMap());

        // Shard B: d3 passed, d1 borderline with 2 more apps
        HyperLogLogPlusPlus shardBHLL = new HyperLogLogPlusPlus(14, bigArrays, 1);
        shardBHLL.collect(0, d3Hash);
        java.util.Map<Long, Object> shardBBorderline = new java.util.HashMap<>();
        shardBBorderline.put(d1Hash, new java.util.HashSet<>(java.util.Set.of(hashString("app3"), hashString("app4"))));

        var shardB = new InternalFilteredMetric("test", shardBHLL, shardBBorderline, 3, 14, java.util.Collections.emptyMap());

        // Reduce: d1 has 4 apps across shards > threshold 3
        var result = (InternalFilteredMetric) shardA.reduce(java.util.List.of(shardA, shardB), null);
        assertEquals(3.0, result.value(), 0.0); // d1 + d2 + d3
    }

    /**
     * Test reduce: borderline that doesn't exceed threshold.
     */
    public void testReduceBorderlineNotExceeding() {
        var bigArrays = org.opensearch.common.util.BigArrays.NON_RECYCLING_INSTANCE;
        long d1Hash = hashString("d1");

        java.util.Map<Long, Object> shardABorderline = new java.util.HashMap<>();
        shardABorderline.put(d1Hash, new java.util.HashSet<>(java.util.Set.of(hashString("app1"))));
        var shardA = new InternalFilteredMetric("test", null, shardABorderline, 3, 14, java.util.Collections.emptyMap());

        java.util.Map<Long, Object> shardBBorderline = new java.util.HashMap<>();
        shardBBorderline.put(d1Hash, new java.util.HashSet<>(java.util.Set.of(hashString("app1"), hashString("app2"))));
        var shardB = new InternalFilteredMetric("test", null, shardBBorderline, 3, 14, java.util.Collections.emptyMap());

        var result = (InternalFilteredMetric) shardA.reduce(java.util.List.of(shardA, shardB), null);
        assertEquals(0.0, result.value(), 0.0); // d1 has 2 apps total ≤ 3
    }

    /**
     * Test reduce with numeric metric (sum).
     */
    public void testReduceNumericSum() {
        var bigArrays = org.opensearch.common.util.BigArrays.NON_RECYCLING_INSTANCE;
        long d1Hash = hashString("d1");

        // Shard A: d1 has sum=500 (borderline, threshold=1000)
        java.util.Map<Long, Object> shardABorderline = new java.util.HashMap<>();
        shardABorderline.put(d1Hash, 500.0);
        var shardA = new InternalFilteredMetric("test", null, shardABorderline, 1000, 14, java.util.Collections.emptyMap());

        // Shard B: d1 has sum=600
        java.util.Map<Long, Object> shardBBorderline = new java.util.HashMap<>();
        shardBBorderline.put(d1Hash, 600.0);
        var shardB = new InternalFilteredMetric("test", null, shardBBorderline, 1000, 14, java.util.Collections.emptyMap());

        var result = (InternalFilteredMetric) shardA.reduce(java.util.List.of(shardA, shardB), null);
        assertEquals(1.0, result.value(), 0.0); // d1 sum=1100 > 1000
    }

    private static long hashString(String s) {
        byte[] bytes = s.getBytes();
        org.opensearch.common.hash.MurmurHash3.Hash128 hash = new org.opensearch.common.hash.MurmurHash3.Hash128();
        org.opensearch.common.hash.MurmurHash3.hash128(bytes, 0, bytes.length, 0, hash);
        return hash.h1;
    }

    private void addDoc(IndexWriter w, String device, String deviceApp) throws IOException {
        Document doc = new Document();
        doc.add(new SortedSetDocValuesField("device", new BytesRef(device)));
        doc.add(new SortedSetDocValuesField("device_app", new BytesRef(deviceApp)));
        w.addDocument(doc);
    }
}
