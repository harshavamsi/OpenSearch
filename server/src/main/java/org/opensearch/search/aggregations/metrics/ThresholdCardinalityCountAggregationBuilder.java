/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AbstractAggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Builder for {@link ThresholdCardinalityCountAggregator}.
 *
 * @opensearch.internal
 */
public class ThresholdCardinalityCountAggregationBuilder extends AbstractAggregationBuilder<ThresholdCardinalityCountAggregationBuilder> {

    public static final String NAME = "threshold_cardinality_count";

    private static final ObjectParser<ThresholdCardinalityCountAggregationBuilder, String> PARSER = new ObjectParser<>(
        NAME,
        ThresholdCardinalityCountAggregationBuilder::new
    );

    static {
        PARSER.declareString(ThresholdCardinalityCountAggregationBuilder::groupField, new ParseField("group_field"));
        PARSER.declareString(ThresholdCardinalityCountAggregationBuilder::countField, new ParseField("count_field"));
        PARSER.declareInt(ThresholdCardinalityCountAggregationBuilder::threshold, new ParseField("threshold"));
        PARSER.declareInt(ThresholdCardinalityCountAggregationBuilder::precisionThreshold, new ParseField("precision_threshold"));
        PARSER.declareInt(ThresholdCardinalityCountAggregationBuilder::minDocCount, new ParseField("min_doc_count"));
        PARSER.declareInt(ThresholdCardinalityCountAggregationBuilder::minBorderlineCount, new ParseField("min_borderline_count"));
    }

    public static ThresholdCardinalityCountAggregationBuilder parse(String name, XContentParser parser) throws IOException {
        ThresholdCardinalityCountAggregationBuilder builder = PARSER.parse(
            parser,
            new ThresholdCardinalityCountAggregationBuilder(name),
            null
        );
        return builder;
    }

    private String groupField;
    private String countField;
    private int threshold = 10;
    private int precisionThreshold = 3000;
    private int minDocCount = -1;
    private int minBorderlineCount = 1; // -1 = auto (threshold + 1)

    public ThresholdCardinalityCountAggregationBuilder(String name) {
        super(name);
    }

    private ThresholdCardinalityCountAggregationBuilder() {
        super("");
    }

    public ThresholdCardinalityCountAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        this.groupField = in.readString();
        this.countField = in.readString();
        this.threshold = in.readVInt();
        this.precisionThreshold = in.readVInt();
        this.minDocCount = in.readVInt();
        this.minBorderlineCount = in.readVInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(groupField);
        out.writeString(countField);
        out.writeVInt(threshold);
        out.writeVInt(precisionThreshold);
        out.writeVInt(minDocCount);
        out.writeVInt(minBorderlineCount);
    }

    public ThresholdCardinalityCountAggregationBuilder minBorderlineCount(int minBorderlineCount) {
        this.minBorderlineCount = minBorderlineCount;
        return this;
    }

    public ThresholdCardinalityCountAggregationBuilder minDocCount(int minDocCount) {
        this.minDocCount = minDocCount;
        return this;
    }

    public ThresholdCardinalityCountAggregationBuilder groupField(String groupField) {
        this.groupField = groupField;
        return this;
    }

    public ThresholdCardinalityCountAggregationBuilder countField(String countField) {
        this.countField = countField;
        return this;
    }

    public ThresholdCardinalityCountAggregationBuilder threshold(int threshold) {
        this.threshold = threshold;
        return this;
    }

    public ThresholdCardinalityCountAggregationBuilder precisionThreshold(int precisionThreshold) {
        this.precisionThreshold = precisionThreshold;
        return this;
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.NONE;
    }

    @Override
    protected AggregatorFactory doBuild(
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subfactoriesBuilder
    ) throws IOException {
        int precision = HyperLogLogPlusPlus.precisionFromThreshold(precisionThreshold);
        int effectiveMinDocCount = minDocCount > 0 ? minDocCount : threshold + 1;
        int effectiveMinBorderlineCount = minBorderlineCount;
        return new ThresholdCardinalityCountAggregatorFactory(
            name,
            groupField,
            countField,
            threshold,
            effectiveMinDocCount,
            effectiveMinBorderlineCount,
            precision,
            queryShardContext,
            parent,
            subfactoriesBuilder,
            metadata
        );
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("group_field", groupField);
        builder.field("count_field", countField);
        builder.field("threshold", threshold);
        if (minDocCount > 0) builder.field("min_doc_count", minDocCount);
        if (minBorderlineCount > 1) builder.field("min_borderline_count", minBorderlineCount);
        if (precisionThreshold != 3000) {
            builder.field("precision_threshold", precisionThreshold);
        }
        builder.endObject();
        return builder;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        ThresholdCardinalityCountAggregationBuilder clone = new ThresholdCardinalityCountAggregationBuilder(name);
        clone.groupField = groupField;
        clone.countField = countField;
        clone.threshold = threshold;
        clone.precisionThreshold = precisionThreshold;
        clone.minDocCount = minDocCount;
        clone.minBorderlineCount = minBorderlineCount;
        return clone;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        ThresholdCardinalityCountAggregationBuilder that = (ThresholdCardinalityCountAggregationBuilder) o;
        return threshold == that.threshold
            && precisionThreshold == that.precisionThreshold
            && minDocCount == that.minDocCount
            && minBorderlineCount == that.minBorderlineCount
            && Objects.equals(groupField, that.groupField)
            && Objects.equals(countField, that.countField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), groupField, countField, threshold, precisionThreshold, minDocCount, minBorderlineCount);
    }
}
