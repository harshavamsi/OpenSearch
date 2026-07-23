/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.BooleanFieldMapper;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.DocCountFieldMapper;
import org.opensearch.index.mapper.FieldNamesFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.index.mapper.IpFieldMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.IgnoredFieldMapper;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MatchOnlyTextFieldMapper;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.index.mapper.RoutingFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;

import java.util.Set;

import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.COLUMNAR_STORAGE;
import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH;
import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.POINT_RANGE;
import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.STORED_FIELDS;

/**
 * {@link DataFormat} descriptor for Lucene inverted indices.
 * <p>
 * Declares support for {@code text} and {@code keyword} fields with inverted index and
 * stored field capabilities. Used by the composite engine to identify Lucene as a
 * secondary data format alongside Parquet (primary).
 * <p>
 * The priority value ({@code 50}) is lower than the primary Parquet format, ensuring
 * Lucene is treated as a secondary format in the composite engine's format ordering.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneDataFormat extends DataFormat {

    /** The format name used to register Lucene in the {@link org.opensearch.index.engine.dataformat.DataFormatRegistry}. */
    public static final String LUCENE_FORMAT_NAME = "lucene";

    private static final Set<FieldTypeCapabilities> SUPPORTED_FIELDS = Set.of(

        // Text types — full-text search + stored
        new FieldTypeCapabilities(TextFieldMapper.CONTENT_TYPE, Set.of(FULL_TEXT_SEARCH, STORED_FIELDS)),
        new FieldTypeCapabilities(KeywordFieldMapper.CONTENT_TYPE, Set.of(FULL_TEXT_SEARCH, STORED_FIELDS, COLUMNAR_STORAGE)),
        new FieldTypeCapabilities(MatchOnlyTextFieldMapper.CONTENT_TYPE, Set.of(FULL_TEXT_SEARCH, STORED_FIELDS)),

        // Numeric/date/ip/boolean types — LongPoint/BKD ranges + doc_values columns. These are
        // native Lucene structures; they were previously unclaimed only because this format ran
        // as a secondary behind Parquet in the composite engine. Claiming them lets Lucene serve
        // as the sole (primary) format for a doc_values-backed analytics index.
        new FieldTypeCapabilities("long", Set.of(POINT_RANGE, COLUMNAR_STORAGE, STORED_FIELDS)),
        new FieldTypeCapabilities("integer", Set.of(POINT_RANGE, COLUMNAR_STORAGE, STORED_FIELDS)),
        new FieldTypeCapabilities("short", Set.of(POINT_RANGE, COLUMNAR_STORAGE, STORED_FIELDS)),
        new FieldTypeCapabilities("byte", Set.of(POINT_RANGE, COLUMNAR_STORAGE, STORED_FIELDS)),
        new FieldTypeCapabilities("double", Set.of(POINT_RANGE, COLUMNAR_STORAGE, STORED_FIELDS)),
        new FieldTypeCapabilities("float", Set.of(POINT_RANGE, COLUMNAR_STORAGE, STORED_FIELDS)),
        new FieldTypeCapabilities("half_float", Set.of(POINT_RANGE, COLUMNAR_STORAGE, STORED_FIELDS)),
        new FieldTypeCapabilities("unsigned_long", Set.of(POINT_RANGE, COLUMNAR_STORAGE, STORED_FIELDS)),
        new FieldTypeCapabilities(DateFieldMapper.CONTENT_TYPE, Set.of(POINT_RANGE, COLUMNAR_STORAGE, STORED_FIELDS)),
        new FieldTypeCapabilities(DateFieldMapper.DATE_NANOS_CONTENT_TYPE, Set.of(POINT_RANGE, COLUMNAR_STORAGE, STORED_FIELDS)),
        new FieldTypeCapabilities(IpFieldMapper.CONTENT_TYPE, Set.of(POINT_RANGE, COLUMNAR_STORAGE, STORED_FIELDS)),
        new FieldTypeCapabilities(BooleanFieldMapper.CONTENT_TYPE, Set.of(POINT_RANGE, COLUMNAR_STORAGE, STORED_FIELDS)),

        // Metadata fields
        new FieldTypeCapabilities(SourceFieldMapper.CONTENT_TYPE, Set.of(STORED_FIELDS)),
        new FieldTypeCapabilities(NestedPathFieldMapper.NAME, Set.of(FULL_TEXT_SEARCH)),
        new FieldTypeCapabilities(FieldNamesFieldMapper.CONTENT_TYPE, Set.of(FULL_TEXT_SEARCH)),
        new FieldTypeCapabilities(IndexFieldMapper.CONTENT_TYPE, Set.of(COLUMNAR_STORAGE, FULL_TEXT_SEARCH)),
        new FieldTypeCapabilities(IdFieldMapper.CONTENT_TYPE, Set.of(STORED_FIELDS, FULL_TEXT_SEARCH)),
        new FieldTypeCapabilities(SeqNoFieldMapper.CONTENT_TYPE, Set.of(COLUMNAR_STORAGE, POINT_RANGE)),
        new FieldTypeCapabilities(SeqNoFieldMapper.PRIMARY_TERM_NAME, Set.of(COLUMNAR_STORAGE)),
        new FieldTypeCapabilities(RoutingFieldMapper.CONTENT_TYPE, Set.of(STORED_FIELDS, FULL_TEXT_SEARCH)),
        new FieldTypeCapabilities(IgnoredFieldMapper.CONTENT_TYPE, Set.of(STORED_FIELDS, FULL_TEXT_SEARCH)),
        new FieldTypeCapabilities(DocCountFieldMapper.CONTENT_TYPE, Set.of(COLUMNAR_STORAGE)),
        new FieldTypeCapabilities(VersionFieldMapper.CONTENT_TYPE, Set.of(COLUMNAR_STORAGE))
    );

    /** {@inheritDoc} Returns {@code "lucene"}. */
    @Override
    public String name() {
        return LUCENE_FORMAT_NAME;
    }

    /** {@inheritDoc} Returns {@code 50}, lower than the primary Parquet format. */
    @Override
    public long priority() {
        return 50L;
    }

    /** {@inheritDoc} Returns capabilities for {@code text} and {@code keyword} fields. */
    @Override
    public Set<FieldTypeCapabilities> supportedFields() {
        return SUPPORTED_FIELDS;
    }
}
