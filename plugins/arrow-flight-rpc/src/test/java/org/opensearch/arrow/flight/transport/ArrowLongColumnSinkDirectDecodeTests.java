/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * {@link ArrowLongColumnSink#appendFromDocValues} must decode doc_values directly into the Arrow
 * data buffer (via {@link NumericDocValues#longValuesInto}) and produce a vector identical to the
 * heap-copy path ({@code longValues} + {@link ArrowLongColumnSink#appendLongs}). On an mmap-backed
 * segment the direct path engages; on a heap directory it returns false and callers fall back.
 */
public class ArrowLongColumnSinkDirectDecodeTests extends OpenSearchTestCase {

    private static final int NUM_DOCS = 50_000;
    private static final int BATCH_SIZE = 4096;

    private RootAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testDirectDecodeOnMMapDirectoryMatchesHeapCopy() throws Exception {
        try (Directory dir = new MMapDirectory(createTempDir())) {
            long[] expected = indexSingleSegment(dir);
            int directBatches = runBothPathsAndAssertEqual(dir, expected);
            assertTrue("direct decode should engage on an mmap-backed segment", directBatches > 0);
        }
    }

    public void testDirectDecodeReturnsFalseOnHeapDirectory() throws Exception {
        try (Directory dir = new ByteBuffersDirectory()) {
            long[] expected = indexSingleSegment(dir);
            int directBatches = runBothPathsAndAssertEqual(dir, expected);
            assertEquals("direct decode must refuse non-mmap directories", 0, directBatches);
        }
    }

    /** Index NUM_DOCS docs with varied non-negative values (gcd 1, no offset) and force-merge to one segment. */
    private long[] indexSingleSegment(Directory dir) throws IOException {
        long[] values = new long[NUM_DOCS];
        // The randomized test codec wraps doc_values in an Asserting format that blocks the
        // codec-level bulk decode; pin the real default codec so longValuesInto can engage.
        IndexWriterConfig iwc = new IndexWriterConfig().setCodec(org.apache.lucene.tests.util.TestUtil.getDefaultCodec());
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < NUM_DOCS; i++) {
                // Varied widths up to ~40 bits. The codec only overrides longValuesInto on the
                // "ordinal-style" branch (minValue == 0 && gcd == 1), so pin doc 0 to value 0
                // (forces minValue 0) and doc 1 to value 1 (forces gcd 1).
                values[i] = i == 0 ? 0L : (i == 1 ? 1L : randomLongBetween(0, 1L << 40));
                Document doc = new Document();
                doc.add(new NumericDocValuesField("v", values[i]));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        return values;
    }

    /**
     * Feed contiguous ascending batches through appendFromDocValues on one sink and the heap
     * longValues + appendLongs path on another, assert both vectors match {@code expected}
     * exactly with zero nulls, and return how many batches the direct path accepted.
     */
    private int runBothPathsAndAssertEqual(Directory dir, long[] expected) throws IOException {
        int directBatches = 0;
        try (
            DirectoryReader reader = DirectoryReader.open(dir);
            ArrowLongColumnSink directSink = new ArrowLongColumnSink(allocator, "direct", NUM_DOCS);
            ArrowLongColumnSink copySink = new ArrowLongColumnSink(allocator, "copy", NUM_DOCS)
        ) {
            assertEquals("expected a single segment", 1, reader.leaves().size());
            LeafReader leaf = reader.leaves().get(0).reader();

            int[] docs = new int[BATCH_SIZE];
            long[] heap = new long[BATCH_SIZE];
            for (int base = 0; base < NUM_DOCS; base += BATCH_SIZE) {
                int size = Math.min(BATCH_SIZE, NUM_DOCS - base);
                for (int i = 0; i < size; i++) {
                    docs[i] = base + i;
                }
                // Fresh iterator per batch, positioned like the collector's dense check.
                NumericDocValues directDv = leaf.getNumericDocValues("v");
                assertTrue(directDv.advanceExact(base));
                if (directSink.appendFromDocValues(directDv, size, docs)) {
                    directBatches++;
                } else {
                    // Fallback path: heap decode + copy, exactly what the collector does.
                    NumericDocValues fallbackDv = leaf.getNumericDocValues("v");
                    assertTrue(fallbackDv.advanceExact(base));
                    fallbackDv.longValues(size, docs, 0, heap, 0, 0L);
                    directSink.appendLongs(heap, 0, size);
                }
                NumericDocValues copyDv = leaf.getNumericDocValues("v");
                assertTrue(copyDv.advanceExact(base));
                copyDv.longValues(size, docs, 0, heap, 0, 0L);
                copySink.appendLongs(heap, 0, size);
            }

            BigIntVector direct = directSink.vector();
            BigIntVector copy = copySink.vector();
            assertEquals(NUM_DOCS, direct.getValueCount());
            assertEquals(NUM_DOCS, copy.getValueCount());
            assertEquals("direct path must leave no nulls", 0, direct.getNullCount());
            assertEquals(0, copy.getNullCount());
            for (int i = 0; i < NUM_DOCS; i++) {
                assertEquals("value mismatch at " + i, expected[i], copy.get(i));
                assertEquals("direct/copy mismatch at " + i, copy.get(i), direct.get(i));
            }
        }
        return directBatches;
    }
}
