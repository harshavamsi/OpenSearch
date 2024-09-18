/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ExperimentalApi
public class ArrowCollector implements Collector {

    BufferAllocator allocator;
    Schema schema;
    List<ProjectionField> projectionFields;
    VectorSchemaRoot root;

    final int BATCH_SIZE = 1000;

    public ArrowCollector() {
        this(new ArrayList<>());
    }

    public ArrowCollector(List<ProjectionField> projectionFields) {
        // super(delegateCollector);
        allocator = new RootAllocator();
        this.projectionFields = projectionFields;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        // LeafCollector innerLeafCollector = this.in.getLeafCollector(context);
        Map<String, Field> arrowFields = new HashMap<>();
        Map<String, FieldVector> vectors = new HashMap<>();
        Map<String, NumericDocValues> iterators = new HashMap<>();
        final NumericDocValues[] numericDocValues = new NumericDocValues[1];
        projectionFields.forEach(field -> {
            switch (field.type) {
                case INT:
                    Field intField = new Field(field.fieldName, FieldType.nullable(new ArrowType.Int(32, true)), null);
                    IntVector intVector = new IntVector(intField, allocator);
                    intVector.allocateNew(BATCH_SIZE);
                    vectors.put(field.fieldName, intVector);
                    arrowFields.put(field.fieldName, intField);
                    break;
                case BOOLEAN:
                    Field boolField = new Field(field.fieldName, FieldType.nullable(new ArrowType.Bool()), null);
                    // vectors.put(field.fieldName, intVector);
                    arrowFields.put(field.fieldName, boolField);
                    break;
                case DATE:
                case DATE_NANOSECONDS:
                case LONG:
                    Field longField = new Field(field.fieldName, FieldType.nullable(new ArrowType.Int(64, true)), null);
                    BigIntVector bigIntVector = new BigIntVector(longField, allocator);
                    bigIntVector.allocateNew(BATCH_SIZE);
                    vectors.put(field.fieldName, bigIntVector);
                    arrowFields.put(field.fieldName, longField);
                    break;
                case UNSIGNED_LONG:
                    Field unsignedLongField = new Field(field.fieldName, FieldType.nullable(new ArrowType.Int(64, false)), null);
                    UInt8Vector uInt8Vector = new UInt8Vector(unsignedLongField, allocator);
                    uInt8Vector.allocateNew(BATCH_SIZE);
                    vectors.put(field.fieldName, uInt8Vector);
                    arrowFields.put(field.fieldName, unsignedLongField);
                    break;
                case HALF_FLOAT:
                    Field halfFoatField = new Field(
                        field.fieldName,
                        FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)),
                        null
                    );
                    Float2Vector float2Vector = new Float2Vector(halfFoatField, allocator);
                    float2Vector.allocateNew(BATCH_SIZE);
                    vectors.put(field.fieldName, float2Vector);
                    arrowFields.put(field.fieldName, halfFoatField);
                    break;
                case FLOAT:
                    Field floatField = new Field(
                        field.fieldName,
                        FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                        null
                    );
                    Float4Vector float4Vector = new Float4Vector(floatField, allocator);
                    float4Vector.allocateNew(BATCH_SIZE);
                    vectors.put(field.fieldName, float4Vector);
                    arrowFields.put(field.fieldName, floatField);
                    break;
                case DOUBLE:
                    Field doubleField = new Field(
                        field.fieldName,
                        FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                        null
                    );
                    Float8Vector float8Vector = new Float8Vector(doubleField, allocator);
                    float8Vector.allocateNew(BATCH_SIZE);
                    vectors.put(field.fieldName, float8Vector);
                    arrowFields.put(field.fieldName, doubleField);
                    break;
                case SHORT:
                    Field shortField = new Field(field.fieldName, FieldType.nullable(new ArrowType.Int(16, true)), null);
                    SmallIntVector smallIntVector = new SmallIntVector(shortField, allocator);
                    smallIntVector.allocateNew(BATCH_SIZE);
                    vectors.put(field.fieldName, smallIntVector);
                    arrowFields.put(field.fieldName, shortField);
                    break;
                case BYTE:
                    Field byteField = new Field(field.fieldName, FieldType.nullable(new ArrowType.Int(8, true)), null);
                    TinyIntVector tinyIntVector = new TinyIntVector(byteField, allocator);
                    tinyIntVector.allocateNew(BATCH_SIZE);
                    vectors.put(field.fieldName, tinyIntVector);
                    arrowFields.put(field.fieldName, byteField);
                    break;
                default:
                    throw new UnsupportedOperationException("Field type not supported");
            }
            ;
            try {
                numericDocValues[0] = context.reader().getNumericDocValues(field.fieldName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            iterators.put(field.fieldName, numericDocValues[0]);
        });
        schema = new Schema(arrowFields.values());
        root = new VectorSchemaRoot(new ArrayList<>(arrowFields.values()), new ArrayList<>(vectors.values()));
        final int[] i = { 0 };
        return new LeafCollector() {
            @Override
            public void setScorer(Scorable scorable) throws IOException {
                // innerLeafCollector.setScorer(scorable);
            }

            @Override
            public void collect(int docId) throws IOException {
                // innerLeafCollector.collect(docId);
                for (String field : iterators.keySet()) {
                    NumericDocValues iterator = iterators.get(field);
                    BigIntVector vector = (BigIntVector) vectors.get(field);
                    if (iterator == null) {
                        break;
                    }
                    if (iterator.advanceExact(docId)) {
                        if (i[0] > BATCH_SIZE) {
                            vector.allocateNew(BATCH_SIZE);
                        }
                        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                        buffer.putLong(iterator.longValue());
                        vector.set(i[0], iterator.longValue());
                        i[0]++;
                    } else {
                        break;
                    }
                }
            }

            @Override
            public void finish() throws IOException {
                // innerLeafCollector.finish();
                root.setRowCount(i[0]);
            }
        };
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    public VectorSchemaRoot getRootVector() {
        return root;
    }
}
