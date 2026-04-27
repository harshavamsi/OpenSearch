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
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.io.IOException;

public class ThresholdCardinalityCountIntegTests extends AggregatorTestCase {

    /**
     * Single segment: 4 devices, threshold=2.
     * d1: 3 apps → passes
     * d2: 1 app → borderline
     * d3: 4 apps → passes
     * d4: 2 apps → borderline (at threshold, not exceeded)
     */
    public void testSingleSegment() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                addDoc(w, "ios", "d1", "d1__app1");
                addDoc(w, "ios", "d1", "d1__app2");
                addDoc(w, "ios", "d1", "d1__app3");
                addDoc(w, "ios", "d2", "d2__app1");
                addDoc(w, "android", "d3", "d3__app1");
                addDoc(w, "android", "d3", "d3__app2");
                addDoc(w, "android", "d3", "d3__app3");
                addDoc(w, "android", "d3", "d3__app4");
                addDoc(w, "android", "d4", "d4__app1");
                addDoc(w, "android", "d4", "d4__app2");
                w.forceMerge(1);
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newIndexSearcher(reader);

                TermsAggregationBuilder termsAgg = new TermsAggregationBuilder("by_platform").field("platform").size(10);
                ThresholdCardinalityCountAggregationBuilder tccAgg = new ThresholdCardinalityCountAggregationBuilder("devices_over_2")
                    .groupField("device")
                    .countField("device_app")
                    .threshold(2);
                termsAgg.subAggregation(tccAgg);

                StringTerms result = searchAndReduce(
                    searcher,
                    new MatchAllDocsQuery(),
                    termsAgg,
                    keywordField("platform"),
                    keywordField("device"),
                    keywordField("device_app")
                );

                // ios: d1 passes (3 > 2), d2 doesn't (1 ≤ 2) → 1
                // android: d3 passes (4 > 2), d4 doesn't (2 ≤ 2) → 1
                for (StringTerms.Bucket bucket : result.getBuckets()) {
                    InternalThresholdCardinalityCount tcc = bucket.getAggregations().get("devices_over_2");
                    assertEquals("platform=" + bucket.getKeyAsString(), 1.0, tcc.value(), 0.0);
                }
            }
        }
    }

    /**
     * Multiple segments simulating shards: device d1 has apps split across segments.
     * Segment 1: d1 has app1, app2 (2 apps)
     * Segment 2: d1 has app3, app4 (2 apps)
     * Total: d1 has 4 distinct apps → should pass threshold=3
     * With shardFanOut=true, each segment is a separate "shard" and borderline merge resolves d1.
     */
    public void testCrossShardBorderlineResolution() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                // Segment 1: d1 has 2 distinct apps but 4 docs (duplicates)
                addDoc(w, "ios", "d1", "d1__app1");
                addDoc(w, "ios", "d1", "d1__app1");
                addDoc(w, "ios", "d1", "d1__app2");
                addDoc(w, "ios", "d1", "d1__app2");
                addDoc(w, "ios", "d2", "d2__app1");
                addDoc(w, "ios", "d2", "d2__app2");
                addDoc(w, "ios", "d2", "d2__app3");
                addDoc(w, "ios", "d2", "d2__app4"); // d2: 4 apps, 4 docs → passes
                w.commit();

                // Segment 2: d1 has 2 more distinct apps, 4 docs
                addDoc(w, "ios", "d1", "d1__app3");
                addDoc(w, "ios", "d1", "d1__app3");
                addDoc(w, "ios", "d1", "d1__app4");
                addDoc(w, "ios", "d1", "d1__app4");
                addDoc(w, "ios", "d3", "d3__app1"); // d3: 1 app, 1 doc → not eligible
                w.commit();
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                assertTrue("Need multiple segments", reader.leaves().size() >= 2);
                IndexSearcher searcher = newIndexSearcher(reader);

                TermsAggregationBuilder termsAgg = new TermsAggregationBuilder("by_platform").field("platform").size(10);
                ThresholdCardinalityCountAggregationBuilder tccAgg = new ThresholdCardinalityCountAggregationBuilder("devices_over_3")
                    .groupField("device")
                    .countField("device_app")
                    .threshold(3);
                termsAgg.subAggregation(tccAgg);

                // shardFanOut=true: each segment treated as separate shard
                StringTerms result = searchAndReduce(
                    createIndexSettings(),
                    searcher,
                    new MatchAllDocsQuery(),
                    termsAgg,
                    10000,
                    true,
                    keywordField("platform"),
                    keywordField("device"),
                    keywordField("device_app")
                );

                StringTerms.Bucket iosBucket = result.getBucketByKey("ios");
                assertNotNull(iosBucket);
                InternalThresholdCardinalityCount tcc = iosBucket.getAggregations().get("devices_over_3");

                // d1: 2 apps on shard1 + 2 apps on shard2 = 4 total > 3 → passes (borderline resolved)
                // d2: 4 apps on shard1 > 3 → passes
                // d3: 1 app → doesn't pass
                // Total: 2 devices
                assertEquals(2.0, tcc.value(), 0.0);
            }
        }
    }

    /**
     * Test with threshold=0: all devices with at least 1 app should pass.
     */
    public void testThresholdZero() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                addDoc(w, "ios", "d1", "d1__app1");
                addDoc(w, "ios", "d2", "d2__app1");
                addDoc(w, "ios", "d3", "d3__app1");
                w.forceMerge(1);
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newIndexSearcher(reader);

                TermsAggregationBuilder termsAgg = new TermsAggregationBuilder("by_platform").field("platform").size(10);
                ThresholdCardinalityCountAggregationBuilder tccAgg = new ThresholdCardinalityCountAggregationBuilder("all_devices")
                    .groupField("device")
                    .countField("device_app")
                    .threshold(0);
                termsAgg.subAggregation(tccAgg);

                StringTerms result = searchAndReduce(
                    searcher,
                    new MatchAllDocsQuery(),
                    termsAgg,
                    keywordField("platform"),
                    keywordField("device"),
                    keywordField("device_app")
                );

                StringTerms.Bucket iosBucket = result.getBucketByKey("ios");
                InternalThresholdCardinalityCount tcc = iosBucket.getAggregations().get("all_devices");
                assertEquals(3.0, tcc.value(), 0.0);
            }
        }
    }

    /**
     * Test with no matching documents.
     */
    public void testEmpty() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                addDoc(w, "ios", "d1", "d1__app1");
                w.forceMerge(1);
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newIndexSearcher(reader);

                ThresholdCardinalityCountAggregationBuilder tccAgg = new ThresholdCardinalityCountAggregationBuilder("test").groupField(
                    "device"
                ).countField("device_app").threshold(100);

                InternalThresholdCardinalityCount result = searchAndReduce(
                    searcher,
                    new MatchAllDocsQuery(),
                    tccAgg,
                    keywordField("device"),
                    keywordField("device_app")
                );

                assertEquals(0.0, result.value(), 0.0);
            }
        }
    }

    private void addDoc(IndexWriter w, String platform, String device, String deviceApp) throws IOException {
        Document doc = new Document();
        doc.add(new SortedSetDocValuesField("platform", new BytesRef(platform)));
        doc.add(new SortedSetDocValuesField("device", new BytesRef(device)));
        doc.add(new SortedSetDocValuesField("device_app", new BytesRef(deviceApp)));
        w.addDocument(doc);
    }
}
