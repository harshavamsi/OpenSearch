/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.fielddata.IndexNumericFieldData;

@ExperimentalApi
public class ProjectionField {
    String fieldName;
    IndexNumericFieldData.NumericType type;

    public ProjectionField(IndexNumericFieldData.NumericType type, String fieldName) {
        this.type = type;
        this.fieldName = fieldName;
    }
}
