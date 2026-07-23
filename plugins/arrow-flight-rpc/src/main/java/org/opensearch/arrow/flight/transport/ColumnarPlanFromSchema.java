/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Reconstructs an {@link AggColumnarPlan} from an Arrow {@link Schema} that was produced
 * by the writer side. Used by the coordinator so it doesn't need the original shard-side
 * agg tree to understand what columns are coming.
 */
final class ColumnarPlanFromSchema {

    private ColumnarPlanFromSchema() {}

    static AggColumnarPlan build(Schema schema) {
        AggColumnarPlan.TermKeyKind keyKind;
        Field termField = schema.findField(AggColumnarSchema.TERM);
        if (termField == null) {
            throw new IllegalStateException("Columnar schema missing 'term' field");
        }
        String metaKind = termField.getMetadata() == null ? null : termField.getMetadata().get(AggColumnarSchema.META_KEY_KIND);
        if (metaKind != null) {
            // Newer writer path: trust the metadata marker.
            keyKind = AggColumnarPlan.TermKeyKind.valueOf(metaKind);
        } else if (termField.getType() instanceof ArrowType.Binary) {
            keyKind = AggColumnarPlan.TermKeyKind.STRING;
        } else if (termField.getType() instanceof ArrowType.Int) {
            keyKind = AggColumnarPlan.TermKeyKind.LONG;
        } else {
            throw new IllegalStateException("Unexpected term column type " + termField.getType());
        }

        // Walk the remaining fields and group by prefix (sub-agg name) to rebuild metric entries.
        // We ignore reserved column names (header, term, doc_count, doc_count_error) and any
        // field whose name doesn't end in one of the known suffixes.
        Set<String> reserved = Set.of(
            AggColumnarSchema.HEADER,
            AggColumnarSchema.TERM,
            AggColumnarSchema.DOC_COUNT,
            AggColumnarSchema.DOC_COUNT_ERROR
        );
        Set<String> emitted = new HashSet<>();
        List<AggColumnarPlan.MetricEntry> metrics = new ArrayList<>();
        for (Field f : schema.getFields()) {
            String name = f.getName();
            if (reserved.contains(name)) continue;
            String aggName = stripKnownSuffix(name);
            if (aggName == null || !emitted.add(aggName)) {
                continue;
            }
            AggColumnarPlan.MetricKind kind = kindFromFieldName(schema, aggName);
            if (kind == null) {
                throw new IllegalStateException("Cannot classify metric column set for " + aggName);
            }
            metrics.add(new AggColumnarPlan.MetricEntry(aggName, kind));
        }

        // Plan's termsAggName is only used for the defensive matches() check — we don't
        // serialize it through the Arrow schema, so use a synthetic placeholder.
        return newPlan("<unknown>", keyKind, metrics);
    }

    private static String stripKnownSuffix(String colName) {
        String[] suffixes = new String[] {
            AggColumnarSchema.SUFFIX_HLL,
            AggColumnarSchema.SUFFIX_MAX,
            AggColumnarSchema.SUFFIX_MIN,
            AggColumnarSchema.SUFFIX_SUM_SCALAR,
            AggColumnarSchema.SUFFIX_SUM,    // avg's sum column
            AggColumnarSchema.SUFFIX_COUNT   // avg's count or value_count
        };
        for (String s : suffixes) {
            if (colName.endsWith(s)) {
                return colName.substring(0, colName.length() - s.length());
            }
        }
        return null;
    }

    private static AggColumnarPlan.MetricKind kindFromFieldName(Schema schema, String aggName) {
        // Collect top-level field names once. Arrow's Schema#findField(String) throws
        // IllegalArgumentException on miss (not null), so null-check style probing like
        // findField(X) != null fails loudly on the very first miss. Walk the name set instead.
        Set<String> names = new HashSet<>();
        for (Field f : schema.getFields()) {
            names.add(f.getName());
        }
        if (names.contains(aggName + AggColumnarSchema.SUFFIX_HLL)) return AggColumnarPlan.MetricKind.CARDINALITY;
        if (names.contains(aggName + AggColumnarSchema.SUFFIX_MAX)) return AggColumnarPlan.MetricKind.MAX;
        if (names.contains(aggName + AggColumnarSchema.SUFFIX_MIN)) return AggColumnarPlan.MetricKind.MIN;
        if (names.contains(aggName + AggColumnarSchema.SUFFIX_SUM_SCALAR)) return AggColumnarPlan.MetricKind.SUM;
        boolean hasAvgSum = names.contains(aggName + AggColumnarSchema.SUFFIX_SUM);
        boolean hasCount = names.contains(aggName + AggColumnarSchema.SUFFIX_COUNT);
        if (hasAvgSum && hasCount) return AggColumnarPlan.MetricKind.AVG;
        if (hasCount) return AggColumnarPlan.MetricKind.VALUE_COUNT;
        return null;
    }

    // Wrapper so we don't expose a 3-arg ctor on AggColumnarPlan beyond what it needs.
    private static AggColumnarPlan newPlan(String name, AggColumnarPlan.TermKeyKind keyKind, List<AggColumnarPlan.MetricEntry> metrics) {
        return AggColumnarPlan.fromParts(name, keyKind, metrics);
    }
}
