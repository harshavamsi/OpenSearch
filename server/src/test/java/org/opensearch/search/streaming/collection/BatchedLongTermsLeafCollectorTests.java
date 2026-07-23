/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.streaming.collection;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.Directory;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for the batched leaf collector: bulk-vs-sparse path selection and
 * count correctness against a per-doc reference on dense and sparse fields.
 */
public class BatchedLongTermsLeafCollectorTests extends OpenSearchTestCase {

    public void testDenseFieldTakesBulkPathAndCountsMatch() throws Exception {
        int docCount = BatchedLongTermsLeafCollector.BATCH_SIZE * 2 + 137; // exercise tail flush
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                for (int i = 0; i < docCount; i++) {
                    Document d = new Document();
                    d.add(new NumericDocValuesField("f", i % 7));
                    w.addDocument(d);
                }
                w.forceMerge(1);
                try (IndexReader reader = DirectoryReader.open(w)) {
                    LeafReaderContext leaf = reader.leaves().get(0);
                    Map<Long, Long> counts = collectAll(leaf, docCount, true);
                    for (long term = 0; term < 7; term++) {
                        long expected = expectedCount(docCount, 7, term);
                        assertEquals("term " + term, expected, (long) counts.get(term));
                    }
                }
            }
        }
    }

    public void testSparseFieldFallsBackAndSkipsMissingDocs() throws Exception {
        int docCount = BatchedLongTermsLeafCollector.BATCH_SIZE + 55;
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                for (int i = 0; i < docCount; i++) {
                    Document d = new Document();
                    if (i % 3 != 0) { // every third doc missing the field
                        d.add(new NumericDocValuesField("f", i % 5));
                    }
                    w.addDocument(d);
                }
                w.forceMerge(1);
                try (IndexReader reader = DirectoryReader.open(w)) {
                    LeafReaderContext leaf = reader.leaves().get(0);
                    Map<Long, Long> counts = collectAll(leaf, docCount, false);
                    long total = counts.values().stream().mapToLong(Long::longValue).sum();
                    long expectedTotal = 0;
                    for (int i = 0; i < docCount; i++) {
                        if (i % 3 != 0) expectedTotal++;
                    }
                    assertEquals("missing docs must not be counted", expectedTotal, total);
                }
            }
        }
    }

    private Map<Long, Long> collectAll(LeafReaderContext leaf, int docCount, boolean expectBulk) throws Exception {
        SortedNumericDocValues dv = DocValues.getSortedNumeric(leaf.reader(), "f");
        Map<Long, Long> counts = new HashMap<>();
        BatchedLongTermsLeafCollector collector = BatchedLongTermsLeafCollector.tryCreate(
            dv,
            LeafBucketCollector.NO_OP_COLLECTOR,
            (docs, values, count) -> {
                for (int i = 0; i < count; i++) {
                    counts.merge(values[i], 1L, Long::sum);
                }
            },
            null
        );
        assertNotNull("singleton numeric field must be batchable", collector);
        try {
            for (int doc = 0; doc < docCount; doc++) {
                collector.collect(doc, 0);
            }
            collector.finish();
            if (expectBulk) {
                assertTrue("dense field should use the bulk path", collector.bulkBatches() > 0);
                assertEquals("dense field should never fall back", 0, collector.sparseBatches());
            } else {
                assertTrue("sparse field should use the fallback path", collector.sparseBatches() > 0);
            }
        } finally {
            collector.close();
        }
        return counts;
    }

    private static long expectedCount(int docCount, int mod, long term) {
        long c = 0;
        for (int i = 0; i < docCount; i++) {
            if (i % mod == term) c++;
        }
        return c;
    }
}
