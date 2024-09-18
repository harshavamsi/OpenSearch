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
import org.apache.arrow.vector.BaseFixedWidthVector;
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
import org.opensearch.index.fielddata.IndexNumericFieldData;

import java.io.IOException;
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

    private Field createArrowField(String fieldName, IndexNumericFieldData.NumericType type) {
        switch (type) {
            case INT:
                return new Field(fieldName, FieldType.nullable(new ArrowType.Int(32, true)), null);
            case LONG:
            case DATE:
            case DATE_NANOSECONDS:
                return new Field(fieldName, FieldType.nullable(new ArrowType.Int(64, true)), null);
            case UNSIGNED_LONG:
                return new Field(fieldName, FieldType.nullable(new ArrowType.Int(64, false)), null);
            case HALF_FLOAT:
                return new Field(fieldName, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)), null);
            case FLOAT:
                return new Field(fieldName, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);
            case DOUBLE:
                return new Field(fieldName, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
            case SHORT:
                return new Field(fieldName, FieldType.nullable(new ArrowType.Int(16, true)), null);
            case BYTE:
                return new Field(fieldName, FieldType.nullable(new ArrowType.Int(8, true)), null);
            default:
                throw new UnsupportedOperationException("Field type not supported");
        }
    }

    private <T extends FieldVector> T createVector(IndexNumericFieldData.NumericType type, Field field, BufferAllocator allocator) {
        VectorFactory<T> factory = (VectorFactory<T>) createVectorFactory(type);
        return factory.createVector(field, allocator);
    }

    private VectorFactory<? extends FieldVector> createVectorFactory(IndexNumericFieldData.NumericType type) {
        switch (type) {
            case INT:
                return IntVector::new;
            case LONG:
            case DATE:
            case DATE_NANOSECONDS:
                return BigIntVector::new;
            case UNSIGNED_LONG:
                return UInt8Vector::new;
            case HALF_FLOAT:
                return Float2Vector::new;
            case FLOAT:
                return Float4Vector::new;
            case DOUBLE:
                return Float8Vector::new;
            case SHORT:
                return SmallIntVector::new;
            case BYTE:
                return TinyIntVector::new;
            default:
                throw new UnsupportedOperationException("Field type not supported");
        }
    }

    private interface VectorFactory<T extends FieldVector> {
        T createVector(Field field, BufferAllocator allocator);
    }

    private void setValue(FieldVector vector, int index, long value) {
        if (vector instanceof IntVector) {
            ((IntVector) vector).set(index, (int) value);
        } else if (vector instanceof BigIntVector) {
            ((BigIntVector) vector).set(index, value);
        } else if (vector instanceof UInt8Vector) {
            ((UInt8Vector) vector).set(index, value);
        } else if (vector instanceof Float2Vector) {
            ((Float2Vector) vector).set(index, (short) value);
        } else if (vector instanceof Float4Vector) {
            ((Float4Vector) vector).set(index, (float) value);
        } else if (vector instanceof Float8Vector) {
            ((Float8Vector) vector).set(index, (double) value);
        } else if (vector instanceof SmallIntVector) {
            ((SmallIntVector) vector).set(index, (short) value);
        } else if (vector instanceof TinyIntVector) {
            ((TinyIntVector) vector).set(index, (byte) value);
        } else {
            throw new UnsupportedOperationException("Field type not supported");
        }
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        // LeafCollector innerLeafCollector = this.in.getLeafCollector(context);
        Map<String, Field> arrowFields = new HashMap<>();
        Map<String, FieldVector> vectors = new HashMap<>();
        Map<String, NumericDocValues> iterators = new HashMap<>();
        final NumericDocValues[] numericDocValues = new NumericDocValues[1];
        projectionFields.forEach(field -> {
            Field arrowField = createArrowField(field.fieldName, field.type);
            arrowFields.put(field.fieldName, arrowField);
            FieldVector vector = createVector(field.type, arrowField, allocator);
            vectors.put(field.fieldName, vector);
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
        final int[] index = { 0 };
        return new LeafCollector() {
            @Override
            public void setScorer(Scorable scorable) throws IOException {
                // innerLeafCollector.setScorer(scorable);
            }

            @Override
            public void collect(int docId) throws IOException {
                // innerLeafCollector.collect(docId);
                for (Map.Entry<String, NumericDocValues> entry : iterators.entrySet()) {
                    String field = entry.getKey();
                    NumericDocValues iterator = entry.getValue();
                    BaseFixedWidthVector vector = (BaseFixedWidthVector) vectors.get(field);
                    if (iterator == null) {
                        break;
                    }
                    if (iterator.advanceExact(docId)) {
                        index[0] = i[0] / iterators.size();
                        if (index[0] > BATCH_SIZE || vector.getValueCapacity() == 0) {
                            vector.allocateNew(BATCH_SIZE);
                        }
                        setValue(vector, index[0], iterator.longValue());
                        i[0]++;
                    } else {
                        break;
                    }
                }
            }

            @Override
            public void finish() throws IOException {
                // innerLeafCollector.finish();
                root.setRowCount(index[0] + 1);
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
