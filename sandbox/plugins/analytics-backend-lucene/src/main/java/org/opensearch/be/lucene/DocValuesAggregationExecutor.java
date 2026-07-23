/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.spi.ShardAggregationEngine;
import org.opensearch.analytics.spi.ShardAggregationEngine.ColumnKind;
import org.opensearch.analytics.spi.ShardAggregationEngine.InputColumn;
import org.opensearch.analytics.spi.ShardAggregationEngineHolder;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;

/**
 * Shard-local grouped aggregation over doc_values: matches docids with the given Lucene
 * query, bulk-decodes the needed columns per batch into Arrow vectors, and pushes the
 * batches through the installed {@link ShardAggregationEngine} (DataFusion), returning its
 * Arrow result stream.
 *
 * <p>Column decode paths:
 * <ul>
 *   <li><b>LONG</b> — {@link NumericDocValues#longValuesInto} straight into the vector's
 *       data buffer (no heap {@code long[]} when the segment supports it); heap bulk decode
 *       + one copy otherwise.</li>
 *   <li><b>KEYWORD</b> — {@link SortedDocValues#ordValues} bulk ordinal decode, then
 *       per-segment term materialization into a {@link ViewVarCharVector}. Terms are looked up
 *       once per distinct ordinal per batch (ordinal-sorted memo), not once per row, so
 *       low-cardinality keys cost ~cardinality lookups per batch. Dictionary-preserving
 *       feed (ords + dictionary, no materialization) is the known follow-up — blocked on
 *       the engine deriving Utf8 (not dictionary) schemas from Substrait today.</li>
 * </ul>
 *
 * <p>This is the doc_values twin of the parquet scan: same engine, same plan shape, only the
 * column source differs. Batch size is 65536 — measured 5× better end-to-end than 4096
 * (per-batch C-Data export + native channel hop dominate small batches).
 *
 * @opensearch.internal
 */
public final class DocValuesAggregationExecutor {

    private static final Logger LOGGER = LogManager.getLogger(DocValuesAggregationExecutor.class);

    public static final int BATCH_SIZE = 65536;

    private long directBatches;
    private long fallbackBatches;
    private final long[] fallbackScratch = new long[BATCH_SIZE];
    private final int[] ordScratch = new int[BATCH_SIZE];
    // BATCH_SIZE = 65536 fits in the 20 row bits of the packed (ord+1)<<20 | row encoding.
    private final long[] ordRowScratch = new long[BATCH_SIZE];

    /**
     * Structured-spec entry (wire v2): all columns LONG.
     *
     * @return the engine's result stream (group cols + agg outputs); caller closes it.
     */
    public EngineResultStream execute(
        IndexSearcher searcher,
        Query query,
        ShardAggregationEngine.AggSpec spec,
        BufferAllocator allocator,
        long taskId
    ) throws IOException {
        // Column kinds are probed from the index (SortedDocValues => keyword), so the wire
        // format needs no kind annotations. A single keyword group key with mergeable
        // aggregates takes the ordinal-first path: group natively on the per-segment ordinal
        // (Int64 — the fast path), materialize terms only for RESULT groups, then merge
        // segments by term. lookupOrd calls drop from once-per-row to once-per-group.
        if (spec.groupColumns().size() == 1 && isKeywordColumn(searcher, spec.groupColumns().get(0))) {
            return executeOrdinalFirst(searcher, query, spec, allocator, taskId);
        }
        ShardAggregationEngine engine = ShardAggregationEngineHolder.get();
        List<InputColumn> columns = new ArrayList<>(spec.inputColumns().size());
        for (String name : spec.inputColumns()) {
            columns.add(new InputColumn(name, ColumnKind.LONG));
        }
        return execute(searcher, query, engine.open(allocator, spec, taskId), columns, allocator, taskId);
    }

    private static boolean isKeywordColumn(IndexSearcher searcher, String column) throws IOException {
        for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
            if (leaf.reader().getSortedDocValues(column) != null) {
                return true;
            }
            if (leaf.reader().getNumericDocValues(column) != null) {
                return false;
            }
        }
        return false;
    }

    /** Wire-v3 entry: run a pre-compiled engine plan over the decoded {@code inputColumns}. */
    public EngineResultStream execute(
        IndexSearcher searcher,
        Query query,
        byte[] planBytes,
        List<InputColumn> inputColumns,
        BufferAllocator allocator,
        long taskId
    ) throws IOException {
        ShardAggregationEngine engine = ShardAggregationEngineHolder.get();
        return execute(searcher, query, engine.open(allocator, planBytes, inputColumns, taskId), inputColumns, allocator, taskId);
    }

    private EngineResultStream execute(
        IndexSearcher searcher,
        Query query,
        ShardAggregationEngine.Session opened,
        List<InputColumn> inputColumns,
        BufferAllocator allocator,
        long taskId
    ) throws IOException {
        ShardAggregationEngine.Session session = opened;
        boolean finished = false;
        try {
            Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
            Schema schema = batchSchema(inputColumns);
            int[] docs = new int[BATCH_SIZE];
            for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
                Scorer scorer = weight.scorer(leaf);
                if (scorer == null) {
                    continue;
                }
                ColumnReader[] readers = new ColumnReader[inputColumns.size()];
                for (int c = 0; c < readers.length; c++) {
                    readers[c] = openColumn(leaf, inputColumns.get(c));
                }
                Bits liveDocs = leaf.reader().getLiveDocs();
                DocIdSetIterator it = scorer.iterator();
                int size = 0;
                for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                    if (liveDocs != null && liveDocs.get(doc) == false) {
                        continue;
                    }
                    docs[size++] = doc;
                    if (size == BATCH_SIZE) {
                        session.feed(decodeBatch(allocator, schema, readers, docs, size));
                        size = 0;
                    }
                }
                if (size > 0) {
                    session.feed(decodeBatch(allocator, schema, readers, docs, size));
                }
            }
            EngineResultStream result = session.finish();
            finished = true;
            LOGGER.debug("[dv-agg] executed: columns={} directBatches={} fallbackBatches={}", inputColumns, directBatches, fallbackBatches);
            return result;
        } finally {
            if (finished == false) {
                session.close();
            }
        }
    }

    // ---- ordinal-first keyword group-by ----

    /**
     * Two-phase keyword group-by. Phase 1, per segment: group on the per-segment ORDINAL as
     * Int64 (native fast path) with the spec's aggregates as partials. Phase 2: materialize
     * the term for each phase-1 RESULT group ({@code lookupOrd} once per group, not per row)
     * and merge across segments by term (COUNT partials merge via SUM; SUM/MIN/MAX by
     * themselves). Requires every aggregate to be merge-associative — the caller routes only
     * COUNT(*)/SUM/MIN/MAX shapes here (v2 spec), never DISTINCT.
     */
    private EngineResultStream executeOrdinalFirst(
        IndexSearcher searcher,
        Query query,
        ShardAggregationEngine.AggSpec spec,
        BufferAllocator allocator,
        long taskId
    ) throws IOException {
        ShardAggregationEngine engine = ShardAggregationEngineHolder.get();
        String keyColumn = spec.groupColumns().get(0);
        List<String> metricColumns = new ArrayList<>();
        for (String col : spec.inputColumns()) {
            if (col.equals(keyColumn) == false) {
                metricColumns.add(col);
            }
        }

        // Phase-1 plan: GROUP BY ord + partial aggregates over {ord, metrics...}.
        List<String> p1Input = new ArrayList<>();
        p1Input.add("$ord");
        p1Input.addAll(metricColumns);
        byte[] phase1Plan = engine.compileFragment(OrdinalFirstPlans.phase1(spec, p1Input));

        // Phase-2 plan: GROUP BY term + merge aggregates over {term, partials...}.
        List<InputColumn> p2Input = new ArrayList<>();
        p2Input.add(new InputColumn(keyColumn, ColumnKind.KEYWORD));
        for (ShardAggregationEngine.AggCall call : spec.aggCalls()) {
            p2Input.add(new InputColumn(call.outputName(), ColumnKind.LONG));
        }
        byte[] phase2Plan = engine.compileFragment(OrdinalFirstPlans.phase2(spec, keyColumn));

        ShardAggregationEngine.Session merge = engine.open(allocator, phase2Plan, p2Input, taskId);
        boolean finished = false;
        try {
            Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
            int[] docs = new int[BATCH_SIZE];
            List<InputColumn> p1Columns = new ArrayList<>();
            for (String name : p1Input) {
                p1Columns.add(new InputColumn(name, ColumnKind.LONG));
            }
            Schema p1Schema = batchSchema(p1Columns);
            Schema p2Schema = batchSchema(p2Input);

            for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
                Scorer scorer = weight.scorer(leaf);
                if (scorer == null) {
                    continue;
                }
                SortedDocValues keyDv = leaf.reader().getSortedDocValues(keyColumn);
                if (keyDv == null) {
                    throw new IllegalStateException("column [" + keyColumn + "] has no SortedDocValues in segment " + leaf.ord);
                }
                NumericDocValues[] metricDvs = new NumericDocValues[metricColumns.size()];
                for (int c = 0; c < metricDvs.length; c++) {
                    metricDvs[c] = leaf.reader().getNumericDocValues(metricColumns.get(c));
                    if (metricDvs[c] == null) {
                        throw new IllegalStateException(
                            "column [" + metricColumns.get(c) + "] has no NumericDocValues in segment " + leaf.ord
                        );
                    }
                }

                ShardAggregationEngine.Session perSegment = engine.open(allocator, phase1Plan, p1Columns, taskId);
                boolean segmentFinished = false;
                try {
                    Bits liveDocs = leaf.reader().getLiveDocs();
                    DocIdSetIterator it = scorer.iterator();
                    int size = 0;
                    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                        if (liveDocs != null && liveDocs.get(doc) == false) {
                            continue;
                        }
                        docs[size++] = doc;
                        if (size == BATCH_SIZE) {
                            perSegment.feed(ordBatch(allocator, p1Schema, keyDv, metricDvs, docs, size));
                            size = 0;
                        }
                    }
                    if (size > 0) {
                        perSegment.feed(ordBatch(allocator, p1Schema, keyDv, metricDvs, docs, size));
                    }
                    EngineResultStream partials = perSegment.finish();
                    segmentFinished = true;
                    try {
                        // Buffer the segment's partials, sort by ordinal, then materialize in
                        // ONE sequential pass over the term dictionary — random-ord lookupOrd
                        // is a per-call term-block decode and dominates when unsorted
                        // (measured: 38.7s -> the fix target at 6M groups).
                        materializeSortedAndFeed(allocator, p2Schema, keyDv, partials, spec.aggCalls().size(), merge);
                    } finally {
                        partials.close();
                    }
                } finally {
                    if (segmentFinished == false) {
                        perSegment.close();
                    }
                }
            }
            EngineResultStream result = merge.finish();
            finished = true;
            LOGGER.debug("[dv-agg] ordinal-first executed: key={} metrics={}", keyColumn, metricColumns);
            return result;
        } finally {
            if (finished == false) {
                merge.close();
            }
        }
    }

    /** Phase-1 feed batch: {ord: Int64 (missing = -1), metrics...}. */
    private VectorSchemaRoot ordBatch(
        BufferAllocator allocator,
        Schema schema,
        SortedDocValues keyDv,
        NumericDocValues[] metricDvs,
        int[] docs,
        int size
    ) throws IOException {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        boolean transferred = false;
        try {
            keyDv.ordValues(size, docs, 0, ordScratch, 0, -1);
            BigIntVector ordVec = (BigIntVector) root.getVector(0);
            ordVec.allocateNew(size);
            for (int i = 0; i < size; i++) {
                fallbackScratch[i] = ordScratch[i];
            }
            long byteLen = (long) size * Long.BYTES;
            MemorySegment.ofAddress(ordVec.getDataBuffer().memoryAddress())
                .reinterpret(byteLen)
                .copyFrom(MemorySegment.ofArray(fallbackScratch).asSlice(0, byteLen));
            ordVec.getValidityBuffer().setOne(0, (size + 7) / 8);
            ordVec.setValueCount(size);
            for (int c = 0; c < metricDvs.length; c++) {
                decodeLong(metricDvs[c], (BigIntVector) root.getVector(c + 1), docs, size);
            }
            root.setRowCount(size);
            transferred = true;
            return root;
        } finally {
            if (transferred == false) {
                root.close();
            }
        }
    }

    /**
     * Drains a segment's phase-1 partials into heap arrays, sorts rows by ordinal, then
     * materializes terms sequentially (each ord visited once, ascending — the term dict is
     * read in order) and feeds phase-2 batches in ord order. Heap cost: (1 + aggCount)
     * longs per RESULT group (~150MB at 6M groups x 2 aggs) — bounded by group count,
     * not row count.
     */
    private void materializeSortedAndFeed(
        BufferAllocator allocator,
        Schema p2Schema,
        SortedDocValues keyDv,
        EngineResultStream partials,
        int aggCount,
        ShardAggregationEngine.Session merge
    ) throws IOException {
        // Drain into growable parallel arrays.
        long[] ords = new long[1 << 16];
        long[][] aggs = new long[aggCount][1 << 16];
        int n = 0;
        java.util.Iterator<org.opensearch.analytics.backend.EngineResultBatch> it = partials.iterator();
        while (it.hasNext()) {
            VectorSchemaRoot in = it.next().getArrowRoot();
            try {
                int rows = in.getRowCount();
                if (n + rows > ords.length) {
                    int newLen = Integer.highestOneBit(n + rows) << 1;
                    ords = java.util.Arrays.copyOf(ords, newLen);
                    for (int c = 0; c < aggCount; c++) {
                        aggs[c] = java.util.Arrays.copyOf(aggs[c], newLen);
                    }
                }
                BigIntVector ordVec = (BigIntVector) in.getVector(0);
                for (int i = 0; i < rows; i++) {
                    ords[n + i] = ordVec.get(i);
                }
                for (int c = 0; c < aggCount; c++) {
                    BigIntVector v = (BigIntVector) in.getVector(c + 1);
                    long[] dst = aggs[c];
                    for (int i = 0; i < rows; i++) {
                        dst[n + i] = v.get(i);
                    }
                }
                n += rows;
            } finally {
                in.close();
            }
        }

        // Sort row indices by ord: packed (ord+1) << 24 | idx. Group counts beyond 2^24 rows
        // per segment result would overflow the index bits; fall back to unsorted in that
        // (pathological) case rather than failing.
        boolean sorted = n < (1 << 24);
        int[] order;
        if (sorted) {
            long[] packed = new long[n];
            for (int i = 0; i < n; i++) {
                packed[i] = ((ords[i] + 1) << 24) | i;
            }
            java.util.Arrays.sort(packed);
            order = new int[n];
            for (int i = 0; i < n; i++) {
                order[i] = (int) (packed[i] & 0xFFFFFF);
            }
        } else {
            order = new int[n];
            for (int i = 0; i < n; i++) {
                order[i] = i;
            }
        }

        // Emit ord-ascending batches; lookupOrd advances monotonically within the segment.
        for (int start = 0; start < n; start += BATCH_SIZE) {
            int len = Math.min(BATCH_SIZE, n - start);
            VectorSchemaRoot root = VectorSchemaRoot.create(p2Schema, allocator);
            boolean transferred = false;
            try {
                ViewVarCharVector terms = (ViewVarCharVector) root.getVector(0);
                terms.allocateNew((long) len * 16, len);
                long lastOrd = Long.MIN_VALUE;
                BytesRef term = null;
                for (int i = 0; i < len; i++) {
                    int row = order[start + i];
                    long ord = ords[row];
                    if (ord < 0) {
                        terms.setNull(i);
                        continue;
                    }
                    if (ord != lastOrd) {
                        term = keyDv.lookupOrd((int) ord);
                        lastOrd = ord;
                    }
                    terms.set(i, term.bytes, term.offset, term.length);
                }
                terms.setValueCount(len);
                for (int c = 0; c < aggCount; c++) {
                    BigIntVector dst = (BigIntVector) root.getVector(c + 1);
                    dst.allocateNew(len);
                    long[] srcCol = aggs[c];
                    for (int i = 0; i < len; i++) {
                        dst.set(i, srcCol[order[start + i]]);
                    }
                    dst.setValueCount(len);
                }
                root.setRowCount(len);
                transferred = true;
            } finally {
                if (transferred == false) {
                    root.close();
                }
            }
            merge.feed(root);
        }
    }

    // ---- per-column decode ----

    private sealed interface ColumnReader permits LongColumn, KeywordColumn {}

    private record LongColumn(NumericDocValues dv) implements ColumnReader {
    }

    private record KeywordColumn(SortedDocValues dv) implements ColumnReader {
    }

    private static ColumnReader openColumn(LeafReaderContext leaf, InputColumn column) throws IOException {
        if (column.kind() == ColumnKind.KEYWORD) {
            SortedDocValues dv = leaf.reader().getSortedDocValues(column.name());
            if (dv == null) {
                throw new IllegalStateException(
                    "column [" + column.name() + "] has no SortedDocValues in segment " + leaf.ord + " — eligibility gate failed"
                );
            }
            return new KeywordColumn(dv);
        }
        NumericDocValues dv = leaf.reader().getNumericDocValues(column.name());
        if (dv == null) {
            // Singleton SortedNumericDocValues (e.g. __row_id__, indexed as the sort field via
            // SortedNumericSortField) unwraps to the underlying NumericDocValues.
            org.apache.lucene.index.SortedNumericDocValues sorted = leaf.reader().getSortedNumericDocValues(column.name());
            if (sorted != null) {
                dv = org.apache.lucene.index.DocValues.unwrapSingleton(sorted);
            }
        }
        if (dv == null) {
            throw new IllegalStateException(
                "column [" + column.name() + "] has no NumericDocValues in segment " + leaf.ord + " — eligibility gate failed"
            );
        }
        return new LongColumn(dv);
    }

    private VectorSchemaRoot decodeBatch(BufferAllocator allocator, Schema schema, ColumnReader[] readers, int[] docs, int size)
        throws IOException {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        boolean transferred = false;
        try {
            for (int c = 0; c < readers.length; c++) {
                FieldVector vec = root.getVector(c);
                switch (readers[c]) {
                    // Timestamp vectors share BigIntVector's 8-byte fixed-width layout;
                    // decodeLong writes through the raw data buffer so either works.
                    case LongColumn lc -> decodeLong(lc.dv(), (BaseFixedWidthVector) vec, docs, size);
                    case KeywordColumn kc -> decodeKeyword(kc.dv(), (ViewVarCharVector) vec, docs, size);
                }
            }
            root.setRowCount(size);
            transferred = true;
            return root;
        } finally {
            if (transferred == false) {
                root.close();
            }
        }
    }

    /**
     * LONG decode: direct MemorySegment path (dense byte-aligned mmap segments — raw,
     * gcd/delta and table encodings all covered by the fork), heap bulk + one copy otherwise.
     * Docids ascending within the batch — required by both decode APIs.
     */
    private void decodeLong(NumericDocValues dv, BaseFixedWidthVector vec, int[] docs, int size) throws IOException {
        vec.allocateNew(size);
        long byteLen = (long) size * Long.BYTES;
        MemorySegment dst = MemorySegment.ofAddress(vec.getDataBuffer().memoryAddress()).reinterpret(byteLen);
        if (dv.longValuesInto(size, docs, 0, dst, 0L, 0L)) {
            directBatches++;
        } else {
            fallbackBatches++;
            dv.longValues(size, docs, 0, fallbackScratch, 0, 0L);
            dst.copyFrom(MemorySegment.ofArray(fallbackScratch).asSlice(0, byteLen));
        }
        vec.getValidityBuffer().setOne(0, (size + 7) / 8);
        vec.setValueCount(size);
    }

    /**
     * KEYWORD decode: bulk ordinal read ({@code ordValues} — the fork's codec override), then
     * term materialization. Rows are filled in ordinal-sorted order via an index sort so each
     * distinct ordinal is looked up exactly once per batch ({@code lookupOrd} reuses its
     * BytesRef; ords ascend so the term dict read is sequential).
     */
    private void decodeKeyword(SortedDocValues dv, ViewVarCharVector vec, int[] docs, int size) throws IOException {
        dv.ordValues(size, docs, 0, ordScratch, 0, -1);
        vec.allocateNew((long) size * 16, size);

        // (ord+1, row) packed into primitive longs — sorting groups equal ords (missing = 0
        // sorts first) without boxed-comparator overhead; each distinct ord costs one
        // lookupOrd per batch and term-dict reads are sequential.
        long[] packed = ordRowScratch;
        for (int i = 0; i < size; i++) {
            packed[i] = ((long) (ordScratch[i] + 1) << 20) | i;
        }
        java.util.Arrays.sort(packed, 0, size);

        int lastOrd = Integer.MIN_VALUE;
        BytesRef term = null;
        for (int k = 0; k < size; k++) {
            int row = (int) (packed[k] & 0xFFFFF);
            int ord = (int) (packed[k] >>> 20) - 1;
            if (ord < 0) {
                vec.setNull(row);
                continue;
            }
            if (ord != lastOrd) {
                term = dv.lookupOrd(ord);
                lastOrd = ord;
            }
            vec.set(row, term.bytes, term.offset, term.length);
        }
        vec.setValueCount(size);
    }

    private static Schema batchSchema(List<InputColumn> columns) {
        List<Field> fields = new ArrayList<>(columns.size());
        FieldType int64Nullable = new FieldType(true, new ArrowType.Int(64, true), null);
        // Utf8View, not Utf8 — DataFusion 54's string group-by asserts view arrays.
        FieldType utf8Nullable = new FieldType(true, new ArrowType.Utf8View(), null);
        // MILLISECOND: date doc_values store epoch millis and Calcite TIMESTAMP(3) lowers to
        // PrecisionTimestamp(3)=ms on both the fragment and stub sides — no scaling needed.
        FieldType tsMillisNullable = new FieldType(
            true,
            new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null),
            null
        );
        for (InputColumn col : columns) {
            FieldType ft = switch (col.kind()) {
                case KEYWORD -> utf8Nullable;
                case TIMESTAMP -> tsMillisNullable;
                case LONG -> int64Nullable;
            };
            fields.add(new Field(col.name(), ft, null));
        }
        return new Schema(fields);
    }

    public long directBatches() {
        return directBatches;
    }

    public long fallbackBatches() {
        return fallbackBatches;
    }
}
