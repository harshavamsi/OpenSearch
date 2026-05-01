/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.opensearch.core.transport.TransportResponse;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.InternalMultiTerms;
import org.opensearch.search.aggregations.bucket.terms.InternalTerms;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalCardinality;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.query.QuerySearchResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Describes the Arrow columnar layout for a streaming aggregation response.
 *
 * <p>Phase A scope: exactly one top-level {@link InternalTerms} (string or numeric-keyed)
 * whose per-bucket sub-aggs are all simple metrics — {@link InternalCardinality},
 * {@link InternalMax}, {@link InternalMin}, {@link InternalSum}, {@link InternalAvg},
 * {@link InternalValueCount}. Any deviation (e.g. a nested terms, a script metric, a
 * histogram sub-agg) causes {@link #detect} to return empty and the stream falls back
 * to the existing single-row VarBinary path.
 *
 * <p>Arrow Flight locks the schema on the first batch (start() call) so this plan is
 * computed once per stream from the first response and reused across batches. The
 * shard always emits the same agg-tree shape per batch so this is safe in practice.
 */
final class AggColumnarPlan {

    enum MetricKind {
        CARDINALITY,   // HLL payload as VarBinary
        MAX,           // Float8
        MIN,           // Float8
        SUM,           // Float8
        AVG,           // Float8 sum + BigInt count
        VALUE_COUNT    // BigInt
    }

    enum TermKeyKind {
        STRING,  // BytesRef term; column is VarBinary
        LONG,    // long term; column is BigInt
        MULTI    // List<Object> composite key (multi_terms); column is VarBinary blob (writeGenericValue list)
    }

    /** Entry for one sub-agg column-set within the plan. */
    static final class MetricEntry {
        final String name;
        final MetricKind kind;

        MetricEntry(String name, MetricKind kind) {
            this.name = name;
            this.kind = kind;
        }
    }

    private final String termsAggName;
    private final TermKeyKind termKeyKind;
    private final List<MetricEntry> metrics;

    private AggColumnarPlan(String termsAggName, TermKeyKind termKeyKind, List<MetricEntry> metrics) {
        this.termsAggName = termsAggName;
        this.termKeyKind = termKeyKind;
        this.metrics = metrics;
    }

    /** Package-private factory for callers that already have the parts (e.g. rebuilt from a schema). */
    static AggColumnarPlan fromParts(String termsAggName, TermKeyKind termKeyKind, List<MetricEntry> metrics) {
        return new AggColumnarPlan(termsAggName, termKeyKind, metrics);
    }

    String getTermsAggName() {
        return termsAggName;
    }

    TermKeyKind getTermKeyKind() {
        return termKeyKind;
    }

    List<MetricEntry> getMetrics() {
        return metrics;
    }

    /**
     * Inspects a response and returns a columnar plan if the shape is eligible.
     *
     * <p>Eligible shape:
     * <ul>
     *   <li>Response is a {@link QuerySearchResult} with aggs present.</li>
     *   <li>Exactly one top-level aggregation; it is an {@link InternalTerms} with
     *       at least one bucket so the sub-agg shape can be inspected.</li>
     *   <li>Every sub-agg on the first bucket is one of the {@link MetricKind} types.
     *       We do NOT re-validate subsequent buckets — per-bucket sub-agg shape is
     *       expected to be homogeneous within a single agg.</li>
     * </ul>
     */
    static Optional<AggColumnarPlan> detect(TransportResponse response) {
        if (!(response instanceof QuerySearchResult qsr)) {
            return Optional.empty();
        }
        if (!qsr.hasAggs()) {
            return Optional.empty();
        }
        InternalAggregations aggs;
        try {
            aggs = qsr.aggregations().expand();
        } catch (Exception e) {
            return Optional.empty();
        }
        if (aggs == null) {
            return Optional.empty();
        }
        List<? extends Aggregation> top = aggs.asList();
        if (top.size() != 1) {
            return Optional.empty();
        }
        Aggregation only = top.get(0);
        if (!(only instanceof InternalTerms<?, ?> terms)) {
            return Optional.empty();
        }
        List<?> buckets = terms.getBuckets();
        if (buckets == null || buckets.isEmpty()) {
            // No sample bucket to introspect sub-agg shape; fall back rather than guess.
            return Optional.empty();
        }

        TermKeyKind termKeyKind;
        InternalAggregations sample;
        if (only instanceof InternalMultiTerms multi) {
            termKeyKind = TermKeyKind.MULTI;
            InternalMultiTerms.Bucket firstBucket = multi.getBuckets().get(0);
            sample = (InternalAggregations) firstBucket.getAggregations();
        } else {
            InternalTerms.Bucket<?> firstBucket = (InternalTerms.Bucket<?>) buckets.get(0);
            Object firstKey = firstBucket.getKey();
            if (firstKey instanceof String || firstKey instanceof org.apache.lucene.util.BytesRef) {
                termKeyKind = TermKeyKind.STRING;
            } else if (firstKey instanceof Number) {
                termKeyKind = TermKeyKind.LONG;
            } else {
                return Optional.empty();
            }
            sample = (InternalAggregations) firstBucket.getAggregations();
        }

        // Every InternalTerms.Bucket implementation stores an InternalAggregations even though
        // the public getter downgrades the return type to Aggregations.
        if (sample == null) {
            // No sub-aggs on buckets — valid, but the plan still has no metric columns.
            return Optional.of(new AggColumnarPlan(terms.getName(), termKeyKind, List.of()));
        }
        List<MetricEntry> metrics = new ArrayList<>(sample.asList().size());
        for (Aggregation sub : sample.asList()) {
            MetricKind kind = classify(sub);
            if (kind == null) {
                return Optional.empty();
            }
            metrics.add(new MetricEntry(sub.getName(), kind));
        }
        return Optional.of(new AggColumnarPlan(terms.getName(), termKeyKind, metrics));
    }

    private static MetricKind classify(Aggregation sub) {
        if (sub instanceof InternalCardinality) return MetricKind.CARDINALITY;
        if (sub instanceof InternalMax) return MetricKind.MAX;
        if (sub instanceof InternalMin) return MetricKind.MIN;
        if (sub instanceof InternalSum) return MetricKind.SUM;
        if (sub instanceof InternalAvg) return MetricKind.AVG;
        if (sub instanceof InternalValueCount) return MetricKind.VALUE_COUNT;
        return null;
    }

    /**
     * Confirms a subsequent batch's agg shape matches this plan. Used on the second+
     * batch as a defensive check — in practice a shard emits homogeneous shape, but a
     * mismatch means we must abort cleanly rather than produce corrupt Arrow columns.
     */
    boolean matches(InternalAggregation topLevel) {
        if (!(topLevel instanceof InternalTerms<?, ?> terms)) return false;
        if (!termsAggName.equals(terms.getName())) return false;
        List<?> buckets = terms.getBuckets();
        if (buckets == null || buckets.isEmpty()) return true; // empty batch — trivially compatible
        InternalAggregations sample;
        if (topLevel instanceof InternalMultiTerms multi) {
            if (termKeyKind != TermKeyKind.MULTI) return false;
            sample = (InternalAggregations) multi.getBuckets().get(0).getAggregations();
        } else {
            InternalTerms.Bucket<?> firstBucket = (InternalTerms.Bucket<?>) buckets.get(0);
            sample = (InternalAggregations) firstBucket.getAggregations();
        }
        List<? extends Aggregation> subs = sample == null ? List.of() : sample.asList();
        if (subs.size() != metrics.size()) return false;
        for (int i = 0; i < subs.size(); i++) {
            Aggregation sub = subs.get(i);
            MetricEntry expected = metrics.get(i);
            if (!expected.name.equals(sub.getName())) return false;
            if (classify(sub) != expected.kind) return false;
        }
        return true;
    }
}
