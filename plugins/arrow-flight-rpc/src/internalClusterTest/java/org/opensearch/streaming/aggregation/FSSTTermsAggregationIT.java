/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.streaming.aggregation;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.metrics.InternalCardinality;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedDynamicSettingsOpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.aggregations.AggregationBuilders.cardinality;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

/**
 * Integration tests for FSST-compressed keyword fields with streaming aggregations.
 * Verifies that terms and cardinality aggregations produce correct results when
 * the keyword field uses FSST compression for doc values.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class FSSTTermsAggregationIT extends ParameterizedDynamicSettingsOpenSearchIntegTestCase {

    public FSSTTermsAggregationIT(Settings dynamicSettings) {
        super(dynamicSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(STREAM_TRANSPORT, true).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(FlightStreamPlugin.class);
    }

    private void createFsstIndex(String index) {
        CreateIndexRequest req = new CreateIndexRequest(index).settings(
            Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0)
        );
        req.mapping(
            "{ \"properties\": {"
                + "\"status\": { \"type\": \"keyword\", \"compression\": \"fsst\" },"
                + "\"category\": { \"type\": \"keyword\", \"compression\": \"fsst\" },"
                + "\"tag\": { \"type\": \"keyword\" }"
                + "} }",
            XContentType.JSON
        );
        assertTrue(client().admin().indices().create(req).actionGet().isAcknowledged());
    }

    private void indexTestData(String index) {
        BulkRequest bulk = new BulkRequest();
        String[] statuses = { "active", "inactive", "pending", "archived" };
        String[] categories = { "electronics", "clothing", "food", "books", "toys" };
        for (int i = 0; i < 200; i++) {
            bulk.add(
                new IndexRequest(index).source(
                    "{ \"status\": \""
                        + statuses[i % statuses.length]
                        + "\", \"category\": \""
                        + categories[i % categories.length]
                        + "\", \"tag\": \"tag_"
                        + (i % 10)
                        + "\" }",
                    XContentType.JSON
                )
            );
        }
        client().bulk(bulk).actionGet();
        client().admin().indices().prepareRefresh(index).get();
    }

    public void testTermsAggregationWithFSST() throws Exception {
        String index = "fsst-terms-test";
        createFsstIndex(index);
        indexTestData(index);

        SearchResponse response = client().prepareSearch(index)
            .setSize(0)
            .addAggregation(terms("by_status").field("status").size(10))
            .get();
        assertSearchResponse(response);

        StringTerms agg = response.getAggregations().get("by_status");
        assertEquals(4, agg.getBuckets().size());
        for (StringTerms.Bucket bucket : agg.getBuckets()) {
            assertEquals(50, bucket.getDocCount());
        }
    }

    public void testTermsAggregationFSSTMatchesDefault() throws Exception {
        String fsstIndex = "fsst-compare-fsst";
        String defaultIndex = "fsst-compare-default";

        createFsstIndex(fsstIndex);
        // Create same index without FSST
        CreateIndexRequest defaultReq = new CreateIndexRequest(defaultIndex).settings(
            Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0)
        );
        defaultReq.mapping(
            "{ \"properties\": {"
                + "\"status\": { \"type\": \"keyword\" },"
                + "\"category\": { \"type\": \"keyword\" },"
                + "\"tag\": { \"type\": \"keyword\" }"
                + "} }",
            XContentType.JSON
        );
        assertTrue(client().admin().indices().create(defaultReq).actionGet().isAcknowledged());

        // Index same data to both
        indexTestData(fsstIndex);
        indexTestData(defaultIndex);

        // Compare terms aggregation results
        SearchResponse fsstResponse = client().prepareSearch(fsstIndex)
            .setSize(0)
            .addAggregation(terms("by_category").field("category").size(10).order(org.opensearch.search.aggregations.BucketOrder.key(true)))
            .get();
        SearchResponse defaultResponse = client().prepareSearch(defaultIndex)
            .setSize(0)
            .addAggregation(terms("by_category").field("category").size(10).order(org.opensearch.search.aggregations.BucketOrder.key(true)))
            .get();

        StringTerms fsstAgg = fsstResponse.getAggregations().get("by_category");
        StringTerms defaultAgg = defaultResponse.getAggregations().get("by_category");

        assertEquals(defaultAgg.getBuckets().size(), fsstAgg.getBuckets().size());
        List<? extends StringTerms.Bucket> fsstBuckets = fsstAgg.getBuckets();
        List<? extends StringTerms.Bucket> defaultBuckets = defaultAgg.getBuckets();
        for (int i = 0; i < fsstBuckets.size(); i++) {
            assertEquals(defaultBuckets.get(i).getKeyAsString(), fsstBuckets.get(i).getKeyAsString());
            assertEquals(defaultBuckets.get(i).getDocCount(), fsstBuckets.get(i).getDocCount());
        }
    }

    public void testCardinalityAggregationWithFSST() throws Exception {
        String index = "fsst-cardinality-test";
        createFsstIndex(index);
        indexTestData(index);

        SearchResponse response = client().prepareSearch(index)
            .setSize(0)
            .addAggregation(cardinality("status_cardinality").field("status"))
            .addAggregation(cardinality("category_cardinality").field("category"))
            .get();
        assertSearchResponse(response);

        InternalCardinality statusCard = response.getAggregations().get("status_cardinality");
        InternalCardinality categoryCard = response.getAggregations().get("category_cardinality");
        assertEquals(4, statusCard.getValue());
        assertEquals(5, categoryCard.getValue());
    }

    public void testMixedFSSTAndDefaultFields() throws Exception {
        String index = "fsst-mixed-test";
        createFsstIndex(index);
        indexTestData(index);

        // Aggregate on FSST field and non-FSST field in same query
        SearchResponse response = client().prepareSearch(index)
            .setSize(0)
            .addAggregation(terms("by_status").field("status").size(10))
            .addAggregation(terms("by_tag").field("tag").size(10))
            .get();
        assertSearchResponse(response);

        StringTerms statusAgg = response.getAggregations().get("by_status");
        StringTerms tagAgg = response.getAggregations().get("by_tag");
        assertEquals(4, statusAgg.getBuckets().size());
        assertEquals(10, tagAgg.getBuckets().size());
    }

    public void testSubAggregationWithFSST() throws Exception {
        String index = "fsst-subagg-test";
        createFsstIndex(index);
        indexTestData(index);

        SearchResponse response = client().prepareSearch(index)
            .setSize(0)
            .addAggregation(terms("by_status").field("status").size(10).subAggregation(cardinality("unique_categories").field("category")))
            .get();
        assertSearchResponse(response);

        StringTerms agg = response.getAggregations().get("by_status");
        assertEquals(4, agg.getBuckets().size());
        for (StringTerms.Bucket bucket : agg.getBuckets()) {
            InternalCardinality subCard = bucket.getAggregations().get("unique_categories");
            assertEquals(5, subCard.getValue());
        }
    }
}
