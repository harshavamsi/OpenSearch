/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
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
 * Builder for the {@code filtered_metric} aggregation. Encapsulates a bucket aggregation,
 * a metric aggregation, and a filter condition. Returns the count of buckets where the
 * metric meets the filter.
 *
 * @opensearch.internal
 */
public class FilteredMetricAggregationBuilder extends AbstractAggregationBuilder<FilteredMetricAggregationBuilder> {

    public static final String NAME = "filtered_metric";

    // Parsed sub-components
    private AggregationBuilder bucketsAgg;
    private AggregationBuilder metricAgg;
    private Double filterGt;
    private Double filterGte;
    private Double filterLt;
    private Double filterLte;

    // Tuning
    private int shardMinDocCount = -1; // auto
    private double shardPassValue = -1; // auto
    private double shardSendValue = -1; // auto
    private String executionHint; // "bfs" (default) or "dfs"

    public FilteredMetricAggregationBuilder(String name) {
        super(name);
    }

    public FilteredMetricAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        this.bucketsAgg = in.readNamedWriteable(AggregationBuilder.class);
        this.metricAgg = in.readNamedWriteable(AggregationBuilder.class);
        this.filterGt = in.readOptionalDouble();
        this.filterGte = in.readOptionalDouble();
        this.filterLt = in.readOptionalDouble();
        this.filterLte = in.readOptionalDouble();
        this.shardMinDocCount = in.readVInt();
        this.shardPassValue = in.readDouble();
        this.shardSendValue = in.readDouble();
        this.executionHint = in.readOptionalString();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(bucketsAgg);
        out.writeNamedWriteable(metricAgg);
        out.writeOptionalDouble(filterGt);
        out.writeOptionalDouble(filterGte);
        out.writeOptionalDouble(filterLt);
        out.writeOptionalDouble(filterLte);
        out.writeVInt(shardMinDocCount);
        out.writeDouble(shardPassValue);
        out.writeDouble(shardSendValue);
        out.writeOptionalString(executionHint);
    }

    public static FilteredMetricAggregationBuilder parse(String name, XContentParser parser) throws IOException {
        FilteredMetricAggregationBuilder builder = new FilteredMetricAggregationBuilder(name);
        String currentFieldName = null;
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("buckets".equals(currentFieldName)) {
                    builder.bucketsAgg = parseSingleAgg("_buckets", parser);
                } else if ("metric".equals(currentFieldName)) {
                    builder.metricAgg = parseSingleAgg("_metric", parser);
                } else if ("filter".equals(currentFieldName)) {
                    parseFilter(parser, builder);
                }
            } else if (token.isValue()) {
                if ("shard_min_doc_count".equals(currentFieldName)) {
                    builder.shardMinDocCount = parser.intValue();
                } else if ("shard_pass_value".equals(currentFieldName)) {
                    builder.shardPassValue = parser.doubleValue();
                } else if ("shard_send_value".equals(currentFieldName)) {
                    builder.shardSendValue = parser.doubleValue();
                } else if ("execution_hint".equals(currentFieldName)) {
                    builder.executionHint = parser.text();
                }
            }
        }

        if (builder.bucketsAgg == null) {
            throw new IllegalArgumentException("[filtered_metric] requires [buckets]");
        }
        if (builder.metricAgg == null) {
            throw new IllegalArgumentException("[filtered_metric] requires [metric]");
        }
        if (builder.filterGt == null && builder.filterGte == null && builder.filterLt == null && builder.filterLte == null) {
            throw new IllegalArgumentException("[filtered_metric] requires at least one [filter] condition");
        }

        return builder;
    }

    private static AggregationBuilder parseSingleAgg(String name, XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new IllegalArgumentException("Expected aggregation type name but got " + token);
        }
        String aggType = parser.currentName();
        parser.nextToken(); // START_OBJECT of agg params
        AggregationBuilder builder = (AggregationBuilder) parser.namedObject(
            org.opensearch.search.aggregations.BaseAggregationBuilder.class,
            aggType,
            name
        );
        // After namedObject, parser is at END_OBJECT of agg params.
        // Advance to END_OBJECT of the wrapper.
        parser.nextToken();
        return builder;
    }

    private static void parseFilter(XContentParser parser, FilteredMetricAggregationBuilder builder) throws IOException {
        XContentParser.Token token;
        String field;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                field = parser.currentName();
                parser.nextToken();
                double val = parser.doubleValue();
                switch (field) {
                    case "gt":
                        builder.filterGt = val;
                        break;
                    case "gte":
                        builder.filterGte = val;
                        break;
                    case "lt":
                        builder.filterLt = val;
                        break;
                    case "lte":
                        builder.filterLte = val;
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown filter condition: " + field);
                }
            }
        }
    }

    public AggregationBuilder getBucketsAgg() {
        return bucketsAgg;
    }

    public FilteredMetricAggregationBuilder bucketsAgg(AggregationBuilder bucketsAgg) {
        this.bucketsAgg = bucketsAgg;
        return this;
    }

    public FilteredMetricAggregationBuilder metricAgg(AggregationBuilder metricAgg) {
        this.metricAgg = metricAgg;
        return this;
    }

    public FilteredMetricAggregationBuilder shardMinDocCount(int v) {
        this.shardMinDocCount = v;
        return this;
    }

    public FilteredMetricAggregationBuilder executionHint(String hint) {
        this.executionHint = hint;
        return this;
    }

    public FilteredMetricAggregationBuilder filterGt(double value) {
        this.filterGt = value;
        return this;
    }

    public AggregationBuilder getMetricAgg() {
        return metricAgg;
    }

    public Double getFilterGt() {
        return filterGt;
    }

    public Double getFilterGte() {
        return filterGte;
    }

    public Double getFilterLt() {
        return filterLt;
    }

    public Double getFilterLte() {
        return filterLte;
    }

    public int getShardMinDocCount() {
        return shardMinDocCount;
    }

    public double getShardPassValue() {
        return shardPassValue;
    }

    public String getExecutionHint() {
        return executionHint;
    }

    public double getShardSendValue() {
        return shardSendValue;
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
        return new FilteredMetricAggregatorFactory(name, this, queryShardContext, parent, subfactoriesBuilder, metadata);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (bucketsAgg != null) {
            builder.field("buckets");
            bucketsAgg.toXContent(builder, params);
        }
        if (metricAgg != null) {
            builder.field("metric");
            metricAgg.toXContent(builder, params);
        }
        builder.startObject("filter");
        if (filterGt != null) builder.field("gt", filterGt);
        if (filterGte != null) builder.field("gte", filterGte);
        if (filterLt != null) builder.field("lt", filterLt);
        if (filterLte != null) builder.field("lte", filterLte);
        builder.endObject();
        if (shardMinDocCount > 0) builder.field("shard_min_doc_count", shardMinDocCount);
        if (shardPassValue >= 0) builder.field("shard_pass_value", shardPassValue);
        if (shardSendValue >= 0) builder.field("shard_send_value", shardSendValue);
        if (executionHint != null) builder.field("execution_hint", executionHint);
        builder.endObject();
        return builder;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        FilteredMetricAggregationBuilder clone = new FilteredMetricAggregationBuilder(name);
        clone.bucketsAgg = bucketsAgg;
        clone.metricAgg = metricAgg;
        clone.filterGt = filterGt;
        clone.filterGte = filterGte;
        clone.filterLt = filterLt;
        clone.filterLte = filterLte;
        clone.shardMinDocCount = shardMinDocCount;
        clone.shardPassValue = shardPassValue;
        clone.shardSendValue = shardSendValue;
        clone.executionHint = executionHint;
        return clone;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        FilteredMetricAggregationBuilder that = (FilteredMetricAggregationBuilder) o;
        return Objects.equals(bucketsAgg, that.bucketsAgg)
            && Objects.equals(metricAgg, that.metricAgg)
            && Objects.equals(filterGt, that.filterGt)
            && Objects.equals(filterGte, that.filterGte)
            && Objects.equals(filterLt, that.filterLt)
            && Objects.equals(filterLte, that.filterLte);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), bucketsAgg, metricAgg, filterGt, filterGte, filterLt, filterLte);
    }
}
