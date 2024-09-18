/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query.stream;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.opensearch.arrow.FlightService;

import java.io.IOException;

import static java.util.Arrays.asList;
import static org.apache.lucene.search.ScoreMode.TOP_DOCS;

public class StreamResultCollector implements Collector {

    BufferAllocator allocator;
    Field docIDField;
    Field joinField;
    Schema schema;
    FlightService flightService;

    FlightDescriptor flightDescriptor;
    Collector in;
    StreamContext streamContext;

    public StreamResultCollector(Collector in, FlightService flightService, FlightDescriptor flightDescriptor) {
        this.in = in;
        this.flightService = flightService;
        allocator = flightService.getAllocator();
        docIDField = new Field("docID", FieldType.nullable(new ArrowType.Int(32, true)), null);
        joinField = new Field("joinField", FieldType.nullable(new ArrowType.Utf8()), null);
        schema = new Schema(asList(docIDField, joinField));
        this.flightDescriptor = flightDescriptor;
    }

    public StreamResultCollector(Collector in, StreamContext streamContext) {
        this.streamContext = streamContext;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        LeafCollector innerLeafCollector = (this.in != null? this.in.getLeafCollector(context) : null);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        VarCharVector joinFieldVector = (VarCharVector) root.getVector("joinField");
        IntVector docIDVector = (IntVector) root.getVector("docID");
        BinaryDocValues docValues = context.reader().getBinaryDocValues("joinField");
        root.getFieldVectors();
        int batchSize = 1000;
        docIDVector.allocateNew(batchSize);
        joinFieldVector.allocateNew(batchSize);
        final int[] i = {0};
        return new LeafCollector() {
            @Override
            public void setScorer(Scorable scorable) throws IOException {
                if (innerLeafCollector != null) {
                    innerLeafCollector.setScorer(scorable);
                }
            }

            @Override
            public void collect(int docId) throws IOException {
                if (innerLeafCollector != null) {
                    innerLeafCollector.collect(docId);
                }
                if (docValues != null) {
                    if (docValues.advanceExact(docId)) {
                        if (i[0] > batchSize) {
                            docIDVector.allocateNew(batchSize);
                            joinFieldVector.allocateNew(batchSize);
                        }
                        docIDVector.set(i[0], docId);
                        joinFieldVector.set(i[0], docValues.binaryValue().bytes);
                        i[0]++;
                    }
                }
            }

            @Override
            public void finish() throws IOException {
                if (innerLeafCollector != null) {
                    innerLeafCollector.finish();
                }
                root.setRowCount(i[0]);
                //flightService.getFlightProducer().addOutput(flightDescriptor, root);
            }
        };
    }

    @Override
    public ScoreMode scoreMode() {
        return TOP_DOCS;
    }

    public void setWeight(Weight weight) {
        if (in != null) {
            in.setWeight(weight);
        }
    }
}
