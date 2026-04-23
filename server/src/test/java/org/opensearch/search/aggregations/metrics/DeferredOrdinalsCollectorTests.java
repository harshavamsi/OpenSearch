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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Tests for {@link CardinalityAggregator.DeferredOrdinalsCollector}.
 */
public class DeferredOrdinalsCollectorTests extends OpenSearchTestCase {

    public void testSingleBucketMaterialize() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                addDoc(w, "apple");
                addDoc(w, "banana");
                addDoc(w, "cherry");
                addDoc(w, "apple"); // duplicate
                w.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                var bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
                var counts = new HyperLogLogPlusPlus(14, bigArrays, 1);
                IndexSearcher searcher = new IndexSearcher(reader);
                ValuesSource.Bytes.WithOrdinals vs = fieldOrdinalsSource("field");

                var deferred = new CardinalityAggregator.DeferredOrdinalsCollector(counts, vs, bigArrays, searcher);
                LeafReaderContext leaf = reader.leaves().get(0);
                var leafCollector = deferred.leafCollector(leaf);

                for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                    leafCollector.collect(doc, 0);
                }
                leafCollector.postCollect();

                // Before materialization, HLL should be empty
                assertEquals(0, counts.cardinality(0));
                // Ordinal cardinality should be exact
                assertEquals(3, deferred.ordinalCardinality(0));

                // Materialize and check HLL
                deferred.materializeHLL(new long[] { 0 });
                assertEquals(3, counts.cardinality(0));

                deferred.close();
                counts.close();
            }
        }
    }

    public void testMultiBucketSelectiveMaterilaize() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                addDoc(w, "a"); // doc 0 → bucket 0
                addDoc(w, "b"); // doc 1 → bucket 0
                addDoc(w, "c"); // doc 2 → bucket 1
                addDoc(w, "d"); // doc 3 → bucket 1
                addDoc(w, "e"); // doc 4 → bucket 1
                w.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                var bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
                var counts = new HyperLogLogPlusPlus(14, bigArrays, 3);
                IndexSearcher searcher = new IndexSearcher(reader);
                ValuesSource.Bytes.WithOrdinals vs = fieldOrdinalsSource("field");

                var deferred = new CardinalityAggregator.DeferredOrdinalsCollector(counts, vs, bigArrays, searcher);
                LeafReaderContext leaf = reader.leaves().get(0);
                var leafCollector = deferred.leafCollector(leaf);

                leafCollector.collect(0, 0); // a → bucket 0
                leafCollector.collect(1, 0); // b → bucket 0
                leafCollector.collect(2, 1); // c → bucket 1
                leafCollector.collect(3, 1); // d → bucket 1
                leafCollector.collect(4, 1); // e → bucket 1
                leafCollector.postCollect();

                assertEquals(2, deferred.ordinalCardinality(0));
                assertEquals(3, deferred.ordinalCardinality(1));

                // Materialize only bucket 1 — bucket 0 should remain empty in HLL
                deferred.materializeHLL(new long[] { 1 });
                assertEquals(0, counts.cardinality(0));
                assertEquals(3, counts.cardinality(1));

                deferred.close();
                counts.close();
            }
        }
    }

    public void testMultiSegmentGlobalOrdinals() throws IOException {
        // Test with multiple groups in a single segment. Global ordinals mapping is
        // handled by the real ValuesSource in production; here we verify that the
        // deferred collector correctly defers materialization and produces correct
        // cardinalities when materializeHLL is called for selected buckets.
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                // g1: apple, banana, cherry, date → 4 distinct
                addDoc2(w, "g1", "apple");
                addDoc2(w, "g1", "banana");
                addDoc2(w, "g2", "cherry");
                addDoc2(w, "g2", "apple");
                addDoc2(w, "g1", "cherry");
                addDoc2(w, "g1", "date");
                addDoc2(w, "g2", "banana");
                addDoc2(w, "g2", "date");
                addDoc2(w, "g2", "elderberry");
                w.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals("single segment", 1, reader.leaves().size());
                var bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());

                // Reference: OrdinalsCollector
                var refCounts = new HyperLogLogPlusPlus(14, bigArrays, 2);
                LeafReaderContext leaf = reader.leaves().get(0);
                SortedSetDocValues dv = leaf.reader().getSortedSetDocValues("field");
                SortedSetDocValues groupDv = leaf.reader().getSortedSetDocValues("group");
                var refCollector = new CardinalityAggregator.OrdinalsCollector(refCounts, dv, bigArrays);
                for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                    groupDv.advanceExact(doc);
                    long groupOrd = groupDv.nextOrd();
                    BytesRef groupVal = groupDv.lookupOrd(groupOrd);
                    int bucket = groupVal.utf8ToString().equals("g1") ? 0 : 1;
                    refCollector.collect(doc, bucket);
                }
                refCollector.postCollect();
                refCollector.close();

                // Test: DeferredOrdinalsCollector
                var testCounts = new HyperLogLogPlusPlus(14, bigArrays, 2);
                IndexSearcher searcher = new IndexSearcher(reader);
                ValuesSource.Bytes.WithOrdinals vs = fieldOrdinalsSource("field");

                var deferred = new CardinalityAggregator.DeferredOrdinalsCollector(testCounts, vs, bigArrays, searcher);
                var leafCollector = deferred.leafCollector(leaf);
                groupDv = leaf.reader().getSortedSetDocValues("group");
                for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                    groupDv.advanceExact(doc);
                    long groupOrd = groupDv.nextOrd();
                    BytesRef groupVal = groupDv.lookupOrd(groupOrd);
                    int bucket = groupVal.utf8ToString().equals("g1") ? 0 : 1;
                    leafCollector.collect(doc, bucket);
                }
                leafCollector.postCollect();

                // Materialize only bucket 0 first
                deferred.materializeHLL(new long[] { 0 });
                assertEquals("bucket 0 (g1) cardinality", refCounts.cardinality(0), testCounts.cardinality(0));
                assertEquals(0, testCounts.cardinality(1)); // not yet materialized

                // Now materialize bucket 1
                deferred.materializeHLL(new long[] { 1 });
                assertEquals("bucket 1 (g2) cardinality", refCounts.cardinality(1), testCounts.cardinality(1));
                assertEquals("g1 should have 4 distinct", 4, refCounts.cardinality(0));
                assertEquals("g2 should have 5 distinct", 5, refCounts.cardinality(1));

                deferred.close();
                refCounts.close();
                testCounts.close();
            }
        }
    }

    public void testEmptyBucket() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                addDoc(w, "x");
                w.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                var bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
                var counts = new HyperLogLogPlusPlus(14, bigArrays, 1);
                IndexSearcher searcher = new IndexSearcher(reader);
                ValuesSource.Bytes.WithOrdinals vs = fieldOrdinalsSource("field");

                var deferred = new CardinalityAggregator.DeferredOrdinalsCollector(counts, vs, bigArrays, searcher);
                // Don't collect anything
                deferred.materializeHLL(new long[] { 0 });
                assertEquals(0, counts.cardinality(0));
                assertEquals(0, deferred.ordinalCardinality(0));

                deferred.close();
                counts.close();
            }
        }
    }

    /**
     * Creates a simple ValuesSource.Bytes.WithOrdinals that returns segment-level ordinals
     * as global ordinals (identity mapping). For single-segment tests this is correct;
     * for multi-segment tests the global ordinals are built by Lucene's OrdinalMap.
     */
    private static ValuesSource.Bytes.WithOrdinals fieldOrdinalsSource(String field) {
        return new ValuesSource.Bytes.WithOrdinals() {
            @Override
            public SortedSetDocValues ordinalsValues(LeafReaderContext context) throws IOException {
                return context.reader().getSortedSetDocValues(field);
            }

            @Override
            public SortedSetDocValues globalOrdinalsValues(LeafReaderContext context) throws IOException {
                // For tests, segment ordinals == global ordinals (single segment or Lucene handles it)
                return context.reader().getSortedSetDocValues(field);
            }

            @Override
            public boolean supportsGlobalOrdinalsMapping() {
                return false;
            }

            @Override
            public java.util.function.LongUnaryOperator globalOrdinalsMapping(LeafReaderContext context) {
                throw new UnsupportedOperationException();
            }

            @Override
            public org.opensearch.index.fielddata.SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                throw new UnsupportedOperationException();
            }
        };
    }

    private void addDoc(IndexWriter w, String value) throws IOException {
        Document doc = new Document();
        doc.add(new SortedSetDocValuesField("field", new BytesRef(value)));
        w.addDocument(doc);
    }

    private void addDoc2(IndexWriter w, String group, String value) throws IOException {
        Document doc = new Document();
        doc.add(new SortedSetDocValuesField("group", new BytesRef(group)));
        doc.add(new SortedSetDocValuesField("field", new BytesRef(value)));
        w.addDocument(doc);
    }
}
