/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MatchOnlyTextFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry of {@link LuceneFieldFactory} instances keyed by OpenSearch field type name.
 *
 * Provides a default registry pre-populated with factories for the standard full-text-searchable
 * types ({@code text}, {@code keyword}, {@code match_only_text}). Additional types can be
 * registered at runtime via {@link #register(String, LuceneFieldFactory)}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class LuceneFieldFactoryRegistry {

    private static final FieldType ID_FIELD_TYPE = new FieldType();

    static {
        ID_FIELD_TYPE.setTokenized(false);
        ID_FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        ID_FIELD_TYPE.setOmitNorms(true);
        ID_FIELD_TYPE.setStored(false);
        ID_FIELD_TYPE.setDocValuesType(DocValuesType.NONE);
        ID_FIELD_TYPE.freeze();
    }

    // ── Default factories ──
    private static final LuceneFieldFactory TEXT_FACTORY = (doc, ft, value, lft) -> {
        doc.add(new Field(ft.name(), value.toString(), lft));
    };

    private static final LuceneFieldFactory KEYWORD_FACTORY = (doc, ft, value, lft) -> {
        String v = value.toString();
        doc.add(new Field(ft.name(), v, lft));
        // Columnar side as singleton SortedDocValues — the doc_values scan executor's ordinal
        // bulk decode (ordValues) reads getSortedDocValues, which returns null for the
        // SORTED_SET the classic mapper path emits. The mapper's Lucene FieldType carries
        // docValuesType=NONE (dv normally added as a separate field), so key off hasDocValues.
        if (ft.hasDocValues()) {
            doc.add(new SortedDocValuesField(ft.name(), new BytesRef(v)));
        }
    };

    private static final LuceneFieldFactory MATCH_ONLY_TEXT_FACTORY = (doc, ft, value, lft) -> {
        doc.add(new Field(ft.name(), value.toString(), lft));
    };

    private static final LuceneFieldFactory ID_FIELD_FACTORY = (doc, ft, value, lft) -> {
        doc.add(new Field(ft.name(), new BytesRef((byte[]) value), ID_FIELD_TYPE));
    };

    private static final LuceneFieldFactory SEQ_NO_FIELD_FACTORY = (doc, ft, value, lft) -> {
        // do nothing for now since we don't want to index seq no indexing without soft deletes enabled.
    };

    // Numeric family: LongPoint (BKD range/filter) + NumericDocValues (columnar), keyed by the
    // long representation the mapper already computed (Number for numerics/date epoch millis,
    // 1/0 for boolean).
    private static final LuceneFieldFactory LONG_FACTORY = (doc, ft, value, lft) -> {
        long v = ((Number) value).longValue();
        doc.add(new LongPoint(ft.name(), v));
        // Singleton NumericDocValues (not SortedNumeric): the doc_values scan executor's
        // eligibility gate and bulk decode (longValuesInto) read the singleton view.
        doc.add(new NumericDocValuesField(ft.name(), v));
    };

    // Floating point: point + sortable-bits doc values, matching NumberFieldMapper.
    private static final LuceneFieldFactory DOUBLE_FACTORY = (doc, ft, value, lft) -> {
        double v = ((Number) value).doubleValue();
        doc.add(new DoublePoint(ft.name(), v));
        doc.add(new NumericDocValuesField(ft.name(), NumericUtils.doubleToSortableLong(v)));
    };

    private static final LuceneFieldFactory FLOAT_FACTORY = (doc, ft, value, lft) -> {
        float v = ((Number) value).floatValue();
        doc.add(new FloatPoint(ft.name(), v));
        doc.add(new NumericDocValuesField(ft.name(), NumericUtils.floatToSortableInt(v)));
    };

    private static final LuceneFieldFactory BOOLEAN_FACTORY = (doc, ft, value, lft) -> {
        boolean v = value instanceof Boolean b ? b : Boolean.parseBoolean(value.toString());
        long l = v ? 1L : 0L;
        doc.add(new LongPoint(ft.name(), l));
        doc.add(new NumericDocValuesField(ft.name(), l));
    };

    private static final LuceneFieldFactory VERSION_FACTORY = (doc, ft, value, lft) -> {
        doc.add(new NumericDocValuesField(ft.name(), ((Number) value).longValue()));
    };

    // ── Registry ──

    private final Map<String, LuceneFieldFactory> factories = new ConcurrentHashMap<>();

    /**
     * Creates a registry pre-populated with the default full-text-searchable field factories.
     */
    public LuceneFieldFactoryRegistry() {
        register(TextFieldMapper.CONTENT_TYPE, TEXT_FACTORY);
        register(KeywordFieldMapper.CONTENT_TYPE, KEYWORD_FACTORY);
        register(MatchOnlyTextFieldMapper.CONTENT_TYPE, MATCH_ONLY_TEXT_FACTORY);
        register("long", LONG_FACTORY);
        register("integer", LONG_FACTORY);
        register("short", LONG_FACTORY);
        register("byte", LONG_FACTORY);
        register("unsigned_long", LONG_FACTORY);
        register("date", LONG_FACTORY);
        register("date_nanos", LONG_FACTORY);
        register("double", DOUBLE_FACTORY);
        register("float", FLOAT_FACTORY);
        register("half_float", FLOAT_FACTORY);
        register("boolean", BOOLEAN_FACTORY);
        registerMetaFields();
    }

    private void registerMetaFields() {
        register(IdFieldMapper.CONTENT_TYPE, ID_FIELD_FACTORY);
        register(SeqNoFieldMapper.CONTENT_TYPE, SEQ_NO_FIELD_FACTORY);
        register(SeqNoFieldMapper.PRIMARY_TERM_NAME, (d, ft, v, lft) -> d.add(new SortedNumericDocValuesField(ft.name(), (long) v)));
        register(SourceFieldMapper.CONTENT_TYPE, (d, ft, v, lft) -> d.add(new Field(ft.name(), (BytesRef) v, lft)));
        register("_version", VERSION_FACTORY);
        register("_doc_count", (d, ft, v, lft) -> d.add(new NumericDocValuesField(ft.name(), ((Number) v).longValue())));
        // pending routing and ignored field handling
    }

    /**
     * Registers a factory for the given field type name. Overwrites any existing registration.
     *
     * @param typeName the OpenSearch field type name (e.g., "text", "keyword")
     * @param factory  the factory that creates Lucene fields for this type
     */
    public void register(String typeName, LuceneFieldFactory factory) {
        factories.put(typeName, factory);
    }

    /**
     * Returns the factory for the given type name, or {@code null} if not registered.
     *
     * @param typeName the OpenSearch field type name
     * @return the factory, or null
     */
    public LuceneFieldFactory get(String typeName) {
        return factories.get(typeName);
    }

    /**
     * Returns the set of currently registered type names.
     *
     * @return unmodifiable set of supported type names
     */
    public Set<String> supportedTypes() {
        return Set.copyOf(factories.keySet());
    }
}
