/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds an Arrow {@link Schema} + {@link VectorSchemaRoot} matching an {@link AggColumnarPlan}.
 *
 * <p>Layout per batch:
 * <ul>
 *   <li>{@code header} (nullable VarBinary) — non-null on row 0 only; carries
 *       the non-per-bucket plumbing (QuerySearchResult state with aggs stripped,
 *       then terms non-bucket state: order, required size, format, etc.).</li>
 *   <li>{@code term} (VarBinary for string terms / BigInt for long terms) — per-bucket key.</li>
 *   <li>{@code doc_count} (BigInt) — per-bucket doc count.</li>
 *   <li>{@code doc_count_error} (nullable BigInt) — per-bucket error, only populated
 *       when the plan's underlying terms agg has {@code showTermDocCountError}.</li>
 *   <li>Per metric entry: one or two typed columns named
 *       {@code <name>__value|__hll|__sum|__count}.</li>
 * </ul>
 *
 * <p>Arrow row count equals bucket count: row 0 does double duty as the header row
 * AND bucket 0. Columns that are semantically "row 0 only" use nullable types.
 */
final class AggColumnarSchema {

    /** Column name for the non-per-bucket header payload. */
    static final String HEADER = "header";
    /** Column name for the bucket term key. VarBinary for string terms, BigInt for long terms. */
    static final String TERM = "term";
    /** Column name for the bucket doc count. */
    static final String DOC_COUNT = "doc_count";
    /** Column name for per-bucket doc count error (nullable; populated only when shown). */
    static final String DOC_COUNT_ERROR = "doc_count_error";

    /** Metadata key on the term field that carries the TermKeyKind name (STRING/LONG/MULTI). */
    static final String META_KEY_KIND = "term_key_kind";

    /** Suffix for HLL payload (cardinality). */
    static final String SUFFIX_HLL = "__hll";
    /** Suffix for count field (avg, value_count). */
    static final String SUFFIX_COUNT = "__count";
    /** Suffix for avg's sum field. */
    static final String SUFFIX_SUM = "__sum";
    /** Suffix for max scalar. */
    static final String SUFFIX_MAX = "__max";
    /** Suffix for min scalar. */
    static final String SUFFIX_MIN = "__min";
    /** Suffix for sum scalar (distinct from avg's sum — only applied when there's no count companion). */
    static final String SUFFIX_SUM_SCALAR = "__sumscalar";
    /** Suffix for filtered_metric / threshold_cardinality_count opaque payload (InternalFilteredMetric serialized form). */
    static final String SUFFIX_FM = "__fm";

    private AggColumnarSchema() {}

    /**
     * Build the Arrow schema describing a batch for the given plan.
     */
    static Schema build(AggColumnarPlan plan) {
        List<Field> fields = new ArrayList<>(4 + plan.getMetrics().size() * 2);

        fields.add(new Field(HEADER, FieldType.nullable(ArrowType.Binary.INSTANCE), null));

        Field termField;
        Map<String, String> termMeta = new HashMap<>(1);
        termMeta.put(META_KEY_KIND, plan.getTermKeyKind().name());
        if (plan.getTermKeyKind() == AggColumnarPlan.TermKeyKind.LONG) {
            termField = new Field(TERM, new FieldType(false, new ArrowType.Int(64, true), null, termMeta), null);
        } else {
            // STRING or MULTI: both stored as VarBinary (MULTI as a writeGenericValue-list blob).
            termField = new Field(TERM, new FieldType(false, ArrowType.Binary.INSTANCE, null, termMeta), null);
        }
        fields.add(termField);
        fields.add(new Field(DOC_COUNT, FieldType.notNullable(new ArrowType.Int(64, true)), null));
        fields.add(new Field(DOC_COUNT_ERROR, FieldType.nullable(new ArrowType.Int(64, true)), null));

        for (AggColumnarPlan.MetricEntry m : plan.getMetrics()) {
            switch (m.kind) {
                case CARDINALITY:
                    fields.add(new Field(m.name + SUFFIX_HLL, FieldType.notNullable(ArrowType.Binary.INSTANCE), null));
                    break;
                case MAX:
                    fields.add(
                        new Field(
                            m.name + SUFFIX_MAX,
                            FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                            null
                        )
                    );
                    break;
                case MIN:
                    fields.add(
                        new Field(
                            m.name + SUFFIX_MIN,
                            FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                            null
                        )
                    );
                    break;
                case SUM:
                    fields.add(
                        new Field(
                            m.name + SUFFIX_SUM_SCALAR,
                            FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                            null
                        )
                    );
                    break;
                case AVG:
                    fields.add(
                        new Field(
                            m.name + SUFFIX_SUM,
                            FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                            null
                        )
                    );
                    fields.add(new Field(m.name + SUFFIX_COUNT, FieldType.notNullable(new ArrowType.Int(64, true)), null));
                    break;
                case VALUE_COUNT:
                    fields.add(new Field(m.name + SUFFIX_COUNT, FieldType.notNullable(new ArrowType.Int(64, true)), null));
                    break;
                case FILTERED_METRIC:
                    // Opaque per-bucket payload: serialized InternalFilteredMetric state (passedHLL + borderline).
                    // Not decomposed into typed columns — the borderline map is a heterogeneous
                    // Map<Long,Object> whose value union (Set<Long> vs Double) doesn't fit Arrow's
                    // typed-column model cleanly. Keeping it as a blob lets reduce logic stay in one place.
                    fields.add(new Field(m.name + SUFFIX_FM, FieldType.notNullable(ArrowType.Binary.INSTANCE), null));
                    break;
                default:
                    throw new IllegalStateException("Unknown metric kind: " + m.kind);
            }
        }
        return new Schema(fields);
    }

    /**
     * Allocates a {@link VectorSchemaRoot} for this schema.
     */
    static VectorSchemaRoot createRoot(Schema schema, BufferAllocator allocator) {
        List<FieldVector> vectors = new ArrayList<>(schema.getFields().size());
        for (Field f : schema.getFields()) {
            FieldVector v = f.createVector(allocator);
            v.allocateNew();
            vectors.add(v);
        }
        return new VectorSchemaRoot(schema, vectors, 0);
    }

    /**
     * Accessor helpers so the writer/reader don't repeat the cast + lookup.
     */
    static VarBinaryVector varBinary(VectorSchemaRoot root, String name) {
        return (VarBinaryVector) root.getVector(name);
    }

    static BigIntVector bigInt(VectorSchemaRoot root, String name) {
        return (BigIntVector) root.getVector(name);
    }

    static Float8Vector float8(VectorSchemaRoot root, String name) {
        return (Float8Vector) root.getVector(name);
    }
}
