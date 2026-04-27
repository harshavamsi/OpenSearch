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
 * Result of {@link ThresholdCardinalityCountAggregator}.
 *
 * <p>Contains an HLL counting groups that definitely exceeded the threshold on their shard,
 * plus borderline groups (hash sets with ≤threshold entries) that need cross-shard merging.
 *
 * @opensearch.internal
 */
public class InternalThresholdCardinalityCount extends InternalNumericMetricsAggregation.SingleValue {

    private final AbstractHyperLogLogPlusPlus passedHLL;
    private final Map<Long, Set<Long>> borderline; // group_hash → count_hash_set
    private final int threshold;
    private final int precision;

    InternalThresholdCardinalityCount(
        String name,
        AbstractHyperLogLogPlusPlus passedHLL,
        Map<Long, Set<Long>> borderline,
        int threshold,
        int precision,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
        this.passedHLL = passedHLL;
        this.borderline = borderline;
        this.threshold = threshold;
        this.precision = precision;
    }

    public InternalThresholdCardinalityCount(StreamInput in) throws IOException {
        super(in);
        this.threshold = in.readVInt();
        this.precision = in.readVInt();
        if (in.readBoolean()) {
            this.passedHLL = AbstractHyperLogLogPlusPlus.readFrom(in, BigArrays.NON_RECYCLING_INSTANCE);
        } else {
            this.passedHLL = null;
        }
        int borderlineSize = in.readVInt();
        this.borderline = new HashMap<>(borderlineSize);
        for (int i = 0; i < borderlineSize; i++) {
            long groupHash = in.readLong();
            int setSize = in.readVInt();
            Set<Long> countHashes = new HashSet<>(setSize);
            for (int j = 0; j < setSize; j++) {
                countHashes.add(in.readLong());
            }
            borderline.put(groupHash, countHashes);
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(threshold);
        out.writeVInt(precision);
        if (passedHLL != null) {
            out.writeBoolean(true);
            passedHLL.writeTo(0, out);
        } else {
            out.writeBoolean(false);
        }
        out.writeVInt(borderline.size());
        for (Map.Entry<Long, Set<Long>> entry : borderline.entrySet()) {
            out.writeLong(entry.getKey());
            out.writeVInt(entry.getValue().size());
            for (Long hash : entry.getValue()) {
                out.writeLong(hash);
            }
        }
    }

    @Override
    public String getWriteableName() {
        return ThresholdCardinalityCountAggregationBuilder.NAME;
    }

    @Override
    public double value() {
        return passedHLL == null ? 0 : passedHLL.cardinality(0);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        // 1. Merge all passed HLLs
        HyperLogLogPlusPlus mergedPassed = null;

        // 2. Merge all borderline hash sets by group_hash
        Map<Long, Set<Long>> mergedBorderline = new HashMap<>();

        for (InternalAggregation agg : aggregations) {
            InternalThresholdCardinalityCount tcc = (InternalThresholdCardinalityCount) agg;

            // Merge passed HLL
            if (tcc.passedHLL != null) {
                if (mergedPassed == null) {
                    mergedPassed = new HyperLogLogPlusPlus(tcc.precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
                }
                mergedPassed.merge(0, tcc.passedHLL, 0);
            }

            // Merge borderline sets
            for (Map.Entry<Long, Set<Long>> entry : tcc.borderline.entrySet()) {
                mergedBorderline.computeIfAbsent(entry.getKey(), k -> new HashSet<>()).addAll(entry.getValue());
            }
        }

        // 3. Resolve borderline: check if any merged set exceeds threshold
        for (Map.Entry<Long, Set<Long>> entry : mergedBorderline.entrySet()) {
            if (entry.getValue().size() > threshold) {
                // This group passed after cross-shard merge — add to passed HLL
                if (mergedPassed == null) {
                    mergedPassed = new HyperLogLogPlusPlus(precision, BigArrays.NON_RECYCLING_INSTANCE, 1);
                }
                mergedPassed.collect(0, entry.getKey());
            }
        }

        if (mergedPassed == null) {
            return aggregations.get(0);
        }
        // Return with empty borderline — all resolved
        return new InternalThresholdCardinalityCount(name, mergedPassed, new HashMap<>(), threshold, precision, getMetadata());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field("value", value());
        return builder;
    }
}
