/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;

import java.io.IOException;
import java.util.List;

public class ArrowCollectorContext extends QueryCollectorContext {

    List<ProjectionField> projectionFields;

    ArrowCollectorContext(String profilerName, List<ProjectionField> projectionFields) {
        super(profilerName);
        this.projectionFields = projectionFields;
    }

    @Override
    Collector create(Collector in) throws IOException {
        return new ArrowCollector(in, projectionFields, 1000);
    }

    @Override
    CollectorManager<?, ReduceableSearchResult> createManager(CollectorManager<?, ReduceableSearchResult> in) throws IOException {
        return null;
    }
}
