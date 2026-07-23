/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Batched/columnar leaf collection for streaming aggregations (POC): docid batching, bulk
 * doc_values reads via Lucene's {@code longValues}, and the Arrow-free column sink seam
 * implemented by the arrow plugins.
 */
package org.opensearch.search.streaming.collection;
