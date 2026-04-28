/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Result of filtered_metric aggregation. Contains an HLL counting buckets that
 * passed the filter, plus borderline data for cross-shard resolution.
 *
 * <p>Borderline entries are keyed by bucket_key_hash (long). Values are either:
 * - {@code Set<Long>} for cardinality (hash sets for exact cross-shard merge)
 * - {@code Double} for numeric metrics (partial values for cross-shard merge)
 *
 * @opensearch.internal
 */
public class InternalFilteredMetric extends InternalNumericMetricsAggregation.SingleValue {

    private final AbstractHyperLogLogPlusPlus passedHLL;
    private final Map<Long, Object> borderline;
    private final double threshold;
    private final int precision;

    InternalFilteredMetric(
        String name,
        AbstractHyperLogLogPlusPlus passedHLL,
        Map<Long, Object> borderline,
        double threshold,
        int precision,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
        this.passedHLL = passedHLL;
        this.borderline = borderline;
        this.threshold = threshold;
        this.precision = precision;
    }

    public InternalFilteredMetric(StreamInput in) throws IOException {
        super(in);
        this.threshold = in.readDouble();
        this.precision = in.readVInt();
        if (in.readBoolean()) {
            this.passedHLL = AbstractHyperLogLogPlusPlus.readFrom(in, BigArrays.NON_RECYCLING_INSTANCE);
        } else {
            this.passedHLL = null;
        }
        int size = in.readVInt();
        this.borderline = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            long key = in.readLong();
            byte type = in.readByte();
            if (type == 0) {
                // Set<Long> for cardinality
                int setSize = in.readVInt();
                Set<Long> hashes = new HashSet<>(setSize);
                for (int j = 0; j < setSize; j++) {
                    hashes.add(in.readLong());
                }
                borderline.put(key, hashes);
            } else {
                // Double for numeric metrics
                borderline.put(key, in.readDouble());
            }
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(threshold);
        out.writeVInt(precision);
        if (passedHLL != null) {
            out.writeBoolean(true);
            passedHLL.writeTo(0, out);
        } else {
            out.writeBoolean(false);
        }
        out.writeVInt(borderline.size());
        for (Map.Entry<Long, Object> entry : borderline.entrySet()) {
            out.writeLong(entry.getKey());
            if (entry.getValue() instanceof Set) {
                out.writeByte((byte) 0);
                @SuppressWarnings("unchecked")
                Set<Long> hashes = (Set<Long>) entry.getValue();
                out.writeVInt(hashes.size());
                for (Long h : hashes) {
                    out.writeLong(h);
                }
            } else {
                out.writeByte((byte) 1);
                out.writeDouble((Double) entry.getValue());
            }
        }
    }

    @Override
    public String getWriteableName() {
        return FilteredMetricAggregationBuilder.NAME;
    }

    @Override
    public double value() {
        return passedHLL == null ? 0 : passedHLL.cardinality(0);
    }

    @Override
    @SuppressWarnings("unchecked")
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        HyperLogLogPlusPlus mergedPassed = null;
        Map<Long, Object> mergedBorderline = new HashMap<>();

        for (InternalAggregation agg : aggregations) {
            InternalFilteredMetric fm = (InternalFilteredMetric) agg;

            if (fm.passedHLL != null) {
                if (mergedPassed == null) {
                    mergedPassed = new HyperLogLogPlusPlus(fm.precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
                }
                mergedPassed.merge(0, fm.passedHLL, 0);
            }

            for (Map.Entry<Long, Object> entry : fm.borderline.entrySet()) {
                long key = entry.getKey();
                Object existing = mergedBorderline.get(key);
                Object incoming = entry.getValue();

                if (existing == null) {
                    mergedBorderline.put(key, incoming);
                } else if (existing instanceof Set && incoming instanceof Set) {
                    ((Set<Long>) existing).addAll((Set<Long>) incoming);
                } else if (existing instanceof Double && incoming instanceof Double) {
                    // Sum partials for sum/value_count; for min take min, for max take max
                    // Default: sum (works for sum, value_count)
                    mergedBorderline.put(key, (Double) existing + (Double) incoming);
                }
            }
        }

        // Resolve borderline: check if merged metric exceeds threshold
        for (Map.Entry<Long, Object> entry : mergedBorderline.entrySet()) {
            double mergedValue;
            if (entry.getValue() instanceof Set) {
                mergedValue = ((Set<?>) entry.getValue()).size();
            } else {
                mergedValue = (Double) entry.getValue();
            }

            if (mergedValue > threshold) {
                if (mergedPassed == null) {
                    mergedPassed = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
                }
                mergedPassed.collect(0, entry.getKey());
            }
        }

        if (mergedPassed == null) {
            return aggregations.get(0);
        }
        return new InternalFilteredMetric(name, mergedPassed, new HashMap<>(), threshold, precision, getMetadata());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field("value", value());
        return builder;
    }
}
