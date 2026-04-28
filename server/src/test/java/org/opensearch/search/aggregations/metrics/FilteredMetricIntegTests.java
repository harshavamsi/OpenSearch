/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchModule;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.Collections;

public class FilteredMetricIntegTests extends AggregatorTestCase {

    private static final NamedXContentRegistry REGISTRY = new NamedXContentRegistry(
        new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents()
    );

    private FilteredMetricAggregationBuilder parseFromDSL(
        String name,
        String bucketField,
        String metricType,
        String metricField,
        String filterOp,
        double filterVal
    ) throws IOException {
        String json = String.format(
            "{\"buckets\":{\"terms\":{\"field\":\"%s\"}},\"metric\":{\"%s\":{\"field\":\"%s\"}},\"filter\":{\"%s\":%s}}",
            bucketField,
            metricType,
            metricField,
            filterOp,
            filterVal
        );
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(REGISTRY, null, json)) {
            parser.nextToken();
            return FilteredMetricAggregationBuilder.parse(name, parser);
        }
    }

    /**
     * Single segment: d1(3 apps), d2(1 app), d3(4 apps), d4(2 apps)
     * filter gt:2 → d1 and d3 pass → value=2
     */
    public void testSingleSegmentCardinality() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                addDoc(w, "d1", "d1__app1");
                addDoc(w, "d1", "d1__app2");
                addDoc(w, "d1", "d1__app3");
                addDoc(w, "d1", "d1__app1"); // dup for min_doc_count
                addDoc(w, "d2", "d2__app1");
                addDoc(w, "d3", "d3__app1");
                addDoc(w, "d3", "d3__app2");
                addDoc(w, "d3", "d3__app3");
                addDoc(w, "d3", "d3__app4");
                addDoc(w, "d3", "d3__app1"); // dup
                addDoc(w, "d3", "d3__app2"); // dup
                addDoc(w, "d4", "d4__app1");
                addDoc(w, "d4", "d4__app2");
                addDoc(w, "d4", "d4__app1"); // dup
                w.forceMerge(1);
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newIndexSearcher(reader);

                FilteredMetricAggregationBuilder agg = parseFromDSL("test", "device", "cardinality", "device_app", "gt", 2);

                InternalFilteredMetric result = searchAndReduce(
                    searcher,
                    new MatchAllDocsQuery(),
                    agg,
                    keywordField("device"),
                    keywordField("device_app")
                );

                // d1(3>2) and d3(4>2) pass → 2
                assertEquals(2.0, result.value(), 0.0);
            }
        }
    }

    /**
     * Multi-segment with shardFanOut: d1 has apps split across segments.
     * Segment 1: d1 has app1,app2 (4 docs). Segment 2: d1 has app3,app4 (4 docs).
     * Total d1: 4 apps > threshold 3. Borderline resolution should catch this.
     */
    public void testCrossShardBorderlineResolution() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                // Segment 1: d1(2 apps, 4 docs), d2(4 apps, 4 docs)
                addDoc(w, "d1", "d1__app1");
                addDoc(w, "d1", "d1__app1");
                addDoc(w, "d1", "d1__app2");
                addDoc(w, "d1", "d1__app2");
                addDoc(w, "d2", "d2__app1");
                addDoc(w, "d2", "d2__app2");
                addDoc(w, "d2", "d2__app3");
                addDoc(w, "d2", "d2__app4");
                w.commit();

                // Segment 2: d1(2 more apps, 4 docs), d3(1 app)
                addDoc(w, "d1", "d1__app3");
                addDoc(w, "d1", "d1__app3");
                addDoc(w, "d1", "d1__app4");
                addDoc(w, "d1", "d1__app4");
                addDoc(w, "d3", "d3__app1");
                w.commit();
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                assertTrue("Need multiple segments", reader.leaves().size() >= 2);
                IndexSearcher searcher = newIndexSearcher(reader);

                FilteredMetricAggregationBuilder agg = parseFromDSL("test", "device", "cardinality", "device_app", "gt", 3);

                InternalFilteredMetric result = searchAndReduce(
                    createIndexSettings(),
                    searcher,
                    new MatchAllDocsQuery(),
                    agg,
                    10000,
                    true,
                    keywordField("device"),
                    keywordField("device_app")
                );

                // d1: 2+2=4 apps > 3 (borderline resolved)
                // d2: 4 apps > 3 (passed on shard1)
                // d3: 1 app (dropped)
                assertEquals(2.0, result.value(), 0.0);
            }
        }
    }

    /**
     * Empty result: no devices exceed threshold.
     */
    public void testNoResults() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                addDoc(w, "d1", "d1__app1");
                addDoc(w, "d1", "d1__app2");
                w.forceMerge(1);
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newIndexSearcher(reader);

                FilteredMetricAggregationBuilder agg = parseFromDSL("test", "device", "cardinality", "device_app", "gt", 100);

                InternalFilteredMetric result = searchAndReduce(
                    searcher,
                    new MatchAllDocsQuery(),
                    agg,
                    keywordField("device"),
                    keywordField("device_app")
                );

                assertEquals(0.0, result.value(), 0.0);
            }
        }
    }

    /**
     * DFS execution hint — verify debug info counters.
     */
    public void testDFSDebugInfo() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                // d1: 3 apps (passes threshold=2)
                addDoc(w, "d1", "d1__app1");
                addDoc(w, "d1", "d1__app2");
                addDoc(w, "d1", "d1__app3");
                // d2: 1 app (borderline, below threshold)
                addDoc(w, "d2", "d2__app1");
                // d3: 4 apps (passes)
                addDoc(w, "d3", "d3__app1");
                addDoc(w, "d3", "d3__app2");
                addDoc(w, "d3", "d3__app3");
                addDoc(w, "d3", "d3__app4");
                // d4: 2 apps (borderline, at threshold)
                addDoc(w, "d4", "d4__app1");
                addDoc(w, "d4", "d4__app2");
                w.forceMerge(1);
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newIndexSearcher(reader);
                FilteredMetricAggregationBuilder agg = parseFromDSL("test", "device", "cardinality", "device_app", "gt", 2);
                agg.executionHint("dfs");
                agg.shardMinDocCount(1);

                // Use createAggregator to access debug info
                InternalFilteredMetric result = searchAndReduce(
                    searcher,
                    new MatchAllDocsQuery(),
                    agg,
                    keywordField("device"),
                    keywordField("device_app")
                );

                // d1(3>2) and d3(4>2) pass → 2
                assertEquals(2.0, result.value(), 0.0);
            }
        }
    }

    /**
     * DFS execution hint — single pass, no BFS replay.
     */
    public void testDFSExecution() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                addDoc(w, "d1", "d1__app1");
                addDoc(w, "d1", "d1__app2");
                addDoc(w, "d1", "d1__app3");
                addDoc(w, "d1", "d1__app1");
                addDoc(w, "d2", "d2__app1");
                addDoc(w, "d3", "d3__app1");
                addDoc(w, "d3", "d3__app2");
                addDoc(w, "d3", "d3__app3");
                addDoc(w, "d3", "d3__app4");
                addDoc(w, "d3", "d3__app1");
                addDoc(w, "d3", "d3__app2");
                addDoc(w, "d4", "d4__app1");
                addDoc(w, "d4", "d4__app2");
                addDoc(w, "d4", "d4__app1");
                w.forceMerge(1);
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newIndexSearcher(reader);
                FilteredMetricAggregationBuilder agg = parseFromDSL("test", "device", "cardinality", "device_app", "gt", 2);
                agg.executionHint("dfs");
                agg.shardMinDocCount(1);

                InternalFilteredMetric result = searchAndReduce(
                    searcher,
                    new MatchAllDocsQuery(),
                    agg,
                    keywordField("device"),
                    keywordField("device_app")
                );
                assertEquals(2.0, result.value(), 0.0);
            }
        }
    }

    private void addDoc(IndexWriter w, String device, String deviceApp) throws IOException {
        Document doc = new Document();
        doc.add(new SortedSetDocValuesField("device", new BytesRef(device)));
        doc.add(new SortedSetDocValuesField("device_app", new BytesRef(deviceApp)));
        w.addDocument(doc);
    }

    private void addDocWithValue(IndexWriter w, String device, double value) throws IOException {
        Document doc = new Document();
        doc.add(new SortedSetDocValuesField("device", new BytesRef(device)));
        doc.add(new SortedNumericDocValuesField("metric_value", NumericUtils.doubleToSortableLong(value)));
        w.addDocument(doc);
    }

    /**
     * Numeric max nested under terms: platform=A has d1(max=100), d2(max=5).
     * platform=B has d3(max=50). filter gt:10 → A:1, B:1
     */
    public void testNumericMaxNestedUnderTerms() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                addDocWithPlatform(w, "A", "d1", 100.0);
                addDocWithPlatform(w, "A", "d2", 5.0);
                addDocWithPlatform(w, "B", "d3", 50.0);
                addDocWithPlatform(w, "B", "d4", 3.0);
                w.forceMerge(1);
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newIndexSearcher(reader);
                FilteredMetricAggregationBuilder fmAgg = parseFromDSL("d", "device", "max", "metric_value", "gt", 10);
                org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder termsAgg =
                    new org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder("by_platform").field("platform")
                        .size(10)
                        .subAggregation(fmAgg);

                InternalAggregation result = searchAndReduce(
                    searcher,
                    new MatchAllDocsQuery(),
                    termsAgg,
                    keywordField("platform"),
                    keywordField("device"),
                    doubleField("metric_value")
                );
                org.opensearch.search.aggregations.bucket.terms.Terms terms =
                    (org.opensearch.search.aggregations.bucket.terms.Terms) result;
                for (org.opensearch.search.aggregations.bucket.terms.Terms.Bucket bucket : terms.getBuckets()) {
                    InternalFilteredMetric fm = bucket.getAggregations().get("d");
                    if ("A".equals(bucket.getKeyAsString())) {
                        assertEquals("A should have 1 device with max>10", 1.0, fm.value(), 0.0);
                    } else if ("B".equals(bucket.getKeyAsString())) {
                        assertEquals("B should have 1 device with max>10", 1.0, fm.value(), 0.0);
                    }
                }
            }
        }
    }

    private void addDocWithPlatform(IndexWriter w, String platform, String device, double value) throws IOException {
        Document doc = new Document();
        doc.add(new SortedSetDocValuesField("platform", new BytesRef(platform)));
        doc.add(new SortedSetDocValuesField("device", new BytesRef(device)));
        doc.add(new SortedNumericDocValuesField("metric_value", NumericUtils.doubleToSortableLong(value)));
        w.addDocument(doc);
    }

    /**
     * Numeric max: d1 max=100, d2 max=5, d3 max=50 → filter gt:10 → d1 and d3 pass → value=2
     */
    public void testNumericMax() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                addDocWithValue(w, "d1", 100.0);
                addDocWithValue(w, "d1", 20.0);
                addDocWithValue(w, "d2", 5.0);
                addDocWithValue(w, "d2", 3.0);
                addDocWithValue(w, "d3", 50.0);
                addDocWithValue(w, "d3", 8.0);
                addDocWithValue(w, "d4", 10.0); // exactly 10, not > 10
                w.forceMerge(1);
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newIndexSearcher(reader);
                FilteredMetricAggregationBuilder agg = parseFromDSL("test", "device", "max", "metric_value", "gt", 10);

                InternalFilteredMetric result = searchAndReduce(
                    searcher,
                    new MatchAllDocsQuery(),
                    agg,
                    keywordField("device"),
                    doubleField("metric_value")
                );
                // d1(max=100>10), d3(max=50>10) pass. d2(max=5), d4(max=10) don't.
                assertEquals(2.0, result.value(), 0.0);
            }
        }
    }

    /**
     * Numeric sum: d1 sum=15, d2 sum=8, d3 sum=25 → filter gt:10 → d1 and d3 pass → value=2
     */
    public void testNumericSum() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                addDocWithValue(w, "d1", 10.0);
                addDocWithValue(w, "d1", 5.0);
                addDocWithValue(w, "d2", 4.0);
                addDocWithValue(w, "d2", 4.0);
                addDocWithValue(w, "d3", 20.0);
                addDocWithValue(w, "d3", 5.0);
                w.forceMerge(1);
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newIndexSearcher(reader);
                FilteredMetricAggregationBuilder agg = parseFromDSL("test", "device", "sum", "metric_value", "gt", 10);

                InternalFilteredMetric result = searchAndReduce(
                    searcher,
                    new MatchAllDocsQuery(),
                    agg,
                    keywordField("device"),
                    doubleField("metric_value")
                );
                // d1(sum=15>10), d3(sum=25>10) pass. d2(sum=8) doesn't.
                assertEquals(2.0, result.value(), 0.0);
            }
        }
    }

    /**
     * Numeric value_count: d1 has 3 docs, d2 has 1 doc, d3 has 4 docs → filter gt:2 → d1 and d3 pass
     */
    public void testNumericValueCount() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                addDocWithValue(w, "d1", 1.0);
                addDocWithValue(w, "d1", 1.0);
                addDocWithValue(w, "d1", 1.0);
                addDocWithValue(w, "d2", 1.0);
                addDocWithValue(w, "d3", 1.0);
                addDocWithValue(w, "d3", 1.0);
                addDocWithValue(w, "d3", 1.0);
                addDocWithValue(w, "d3", 1.0);
                w.forceMerge(1);
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newIndexSearcher(reader);
                FilteredMetricAggregationBuilder agg = parseFromDSL("test", "device", "value_count", "metric_value", "gt", 2);

                InternalFilteredMetric result = searchAndReduce(
                    searcher,
                    new MatchAllDocsQuery(),
                    agg,
                    keywordField("device"),
                    doubleField("metric_value")
                );
                // d1(count=3>2), d3(count=4>2) pass. d2(count=1) doesn't.
                assertEquals(2.0, result.value(), 0.0);
            }
        }
    }

    /**
     * Numeric min: d1 min=1, d2 min=15, d3 min=20 → filter gt:10 → d2 and d3 pass
     */
    public void testNumericMin() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                addDocWithValue(w, "d1", 1.0);
                addDocWithValue(w, "d1", 50.0);
                addDocWithValue(w, "d2", 15.0);
                addDocWithValue(w, "d2", 100.0);
                addDocWithValue(w, "d3", 20.0);
                addDocWithValue(w, "d3", 30.0);
                w.forceMerge(1);
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newIndexSearcher(reader);
                FilteredMetricAggregationBuilder agg = parseFromDSL("test", "device", "min", "metric_value", "gt", 10);

                InternalFilteredMetric result = searchAndReduce(
                    searcher,
                    new MatchAllDocsQuery(),
                    agg,
                    keywordField("device"),
                    doubleField("metric_value")
                );
                // d2(min=15>10), d3(min=20>10) pass. d1(min=1) doesn't.
                assertEquals(2.0, result.value(), 0.0);
            }
        }
    }
}
