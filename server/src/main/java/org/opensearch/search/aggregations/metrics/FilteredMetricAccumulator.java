/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.common.util.BigArrays;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Mutable, coord-side merge state for a single survivor bucket's {@code filtered_metric}
 * sub-aggregation. Owned by the streaming terms reducer so the state never rides the
 * wire between partial reduces; only a compact, threshold-resolved
 * {@link InternalFilteredMetric} is materialized at {@link #materializeFinal(String, Map)}.
 *
 * <p>The existing {@link InternalFilteredMetric#reduce} path buffered every shard's
 * borderline map and only promoted crossed-threshold groups at final reduce. Under the
 * streaming+columnar path, partial reduces fire repeatedly and each emitted
 * {@code InternalFilteredMetric} carries the full merged borderline back onto the
 * coord's reduce buffer — tens of GBs of {@code (Long, Set<Long>)} entries for
 * high-cardinality group_fields like {@code device_guid} × 128 shards.
 *
 * <p>This accumulator fixes it by promoting eagerly: on every {@link #accept},
 * groups that cross threshold are merged into {@link #mergedPassed} and dropped from
 * the borderline. The borderline's steady-state size is then bounded by the
 * not-yet-crossed groups per shard, not by the cumulative sum across all shards.
 *
 * <p>Not thread-safe; callers (same pattern as {@link org.opensearch.search.aggregations.bucket.terms.StreamingTermsReducer})
 * serialize accepts.
 *
 * @opensearch.internal
 */
public final class FilteredMetricAccumulator {

    private final double threshold;
    private final int precision;

    /**
     * Running HLL of passed-threshold groups. Lazily allocated on first promotion to
     * avoid paying for the ~12 KB HLL register block on accumulators that never see a
     * crossed-threshold group.
     */
    private HyperLogLogPlusPlus mergedPassed;

    /**
     * Under-threshold groups with partial metric state accumulated across shards.
     * Value type is the same as {@link InternalFilteredMetric}'s borderline:
     * {@code Set<Long>} for cardinality-backed metrics, {@code Double} for scalar.
     */
    private final Map<Long, Object> borderline = new HashMap<>();

    public FilteredMetricAccumulator(double threshold, int precision) {
        this.threshold = threshold;
        this.precision = precision;
    }

    /**
     * Fold one shard's (or one prior partial's) {@link InternalFilteredMetric} into
     * the running state. Merges the incoming {@code passedHLL} into {@link #mergedPassed},
     * unions partial borderline values into ours, then promotes any groups now over
     * threshold and drops them from the borderline.
     */
    @SuppressWarnings("unchecked")
    public void accept(InternalFilteredMetric incoming) {
        if (incoming == null) {
            return;
        }

        if (incoming.getPassedHLL() != null) {
            if (mergedPassed == null) {
                mergedPassed = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
            }
            mergedPassed.merge(0, incoming.getPassedHLL(), 0);
        }

        for (Map.Entry<Long, Object> entry : incoming.getBorderline().entrySet()) {
            long key = entry.getKey();
            Object existing = borderline.get(key);
            Object inc = entry.getValue();
            if (existing == null) {
                // Copy the incoming value by reference; we own the merged map.
                // For Set<Long>, take ownership of the incoming set — the shard-side
                // InternalFilteredMetric is dropped after this call so no aliasing.
                borderline.put(key, inc);
            } else if (existing instanceof Set && inc instanceof Set) {
                ((Set<Long>) existing).addAll((Set<Long>) inc);
            } else if (existing instanceof Double && inc instanceof Double) {
                borderline.put(key, (Double) existing + (Double) inc);
            }
        }

        // Eager promotion: any group now over threshold moves to the HLL and leaves the map.
        Iterator<Map.Entry<Long, Object>> it = borderline.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Object> entry = it.next();
            double v;
            Object val = entry.getValue();
            if (val instanceof Set) {
                v = ((Set<?>) val).size();
            } else {
                v = (Double) val;
            }
            if (v > threshold) {
                if (mergedPassed == null) {
                    mergedPassed = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
                }
                mergedPassed.collect(0, entry.getKey());
                it.remove();
            }
        }
    }

    /**
     * Materialize a final, coord-ready {@link InternalFilteredMetric}. The borderline
     * is dropped — any groups still under threshold at this point didn't cross on
     * any shard in this subtree, so they contribute zero to the final count.
     */
    public InternalFilteredMetric materializeFinal(String name, Map<String, Object> metadata) {
        return new InternalFilteredMetric(name, mergedPassed, new HashMap<>(), threshold, precision, metadata);
    }

    /** True if the accumulator is effectively empty (no passed HLL and empty borderline). */
    public boolean isEmpty() {
        return mergedPassed == null && borderline.isEmpty();
    }
}
