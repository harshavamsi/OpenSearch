/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.arrow.FlightService;
import org.opensearch.index.fielddata.IndexNumericFieldData;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.TestSearchContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ArrowCollectorTests extends IndexShardTestCase {
    private IndexShard indexShard;
    private final QueryPhaseSearcher queryPhaseSearcher;

    @ParametersFactory
    public static Collection<Object[]> concurrency() {
        return Collections.singletonList(new Object[] { 0, QueryPhase.DEFAULT_QUERY_PHASE_SEARCHER });
    }

    public ArrowCollectorTests(int concurrency, QueryPhaseSearcher queryPhaseSearcher) {
        this.queryPhaseSearcher = queryPhaseSearcher;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        indexShard = newShard(true);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        closeShards(indexShard);
    }

    public void testArrow() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

        FlightService flightService = new FlightService();
        final int numDocs = scaledRandomIntBetween(100, 200);
        IndexReader reader = null;
        try {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                doc.add(new LongPoint("longpoint", i));
                doc.add(new NumericDocValuesField("longpoint", i));
                w.addDocument(doc);
            }
            w.close();
            reader = DirectoryReader.open(dir);
            flightService.start();
            TestSearchContext context = new TestSearchContext(null, indexShard, newContextSearcher(reader, null), null, flightService);
            context.setSize(1000);
            context.parsedQuery(new ParsedQuery(new MatchAllDocsQuery()));
            context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
            List<ProjectionField> projectionFields = new ArrayList<>();
            projectionFields.add(new ProjectionField(IndexNumericFieldData.NumericType.LONG, "longpoint"));
            // DEFAULT_STREAM_PHASE_SEARCHER.searchWith(
            // context,
            // context.searcher(),
            // context.query(),
            // new LinkedList<>(),
            // projectionFields,
            // false,
            // false
            // );
            // QueryPhase.executeStreamInternal(context.withCleanQueryResult(), QueryPhase.STREAM_QUERY_PHASE_SEARCHER, projectionFields);
            FlightStream flightStream = flightService.getFlightClient().getStream(new Ticket("id1".getBytes(StandardCharsets.UTF_8)));
            System.out.println(flightStream.getSchema());
            System.out.println(flightStream.next());
            System.out.println(flightStream.getRoot().contentToTSVString());
            System.out.println(flightStream.getRoot().getRowCount());
            System.out.println(flightStream.next());
            // VectorSchemaRoot vectorSchemaRoot = context.getArrowCollector().getVectorSchemaRoot(new RootAllocator(Integer.MAX_VALUE));
            // System.out.println(vectorSchemaRoot.getSchema());
            // Field longPoint = vectorSchemaRoot.getSchema().findField("longpoint");
            // assertEquals(longPoint, new Field("longpoint", FieldType.nullable(new ArrowType.Int(64, true)), null));
            // BigIntVector vector = (BigIntVector) vectorSchemaRoot.getVector("longpoint");
            // assertEquals(vector.getValueCount(), numDocs);
        } finally {
            if (reader != null) reader.close();
            dir.close();
        }
    }

    public void testArrowMultipleFields() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig();
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

        // FlightService flightService = new FlightService();
        final int numDocs = scaledRandomIntBetween(100, 200);
        IndexReader reader = null;
        try {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                doc.add(new LongPoint("longpoint", i));
                doc.add(new NumericDocValuesField("longpoint", i));

                doc.add(new IntPoint("intpoint", 100 + i));
                doc.add(new NumericDocValuesField("intpoint", 100 + i));
                w.addDocument(doc);
            }
            w.close();
            reader = DirectoryReader.open(dir);
            // flightService.start();
            TestSearchContext context = new TestSearchContext(null, indexShard, newContextSearcher(reader, null), null);
            context.setSize(1000);
            context.parsedQuery(new ParsedQuery(new MatchAllDocsQuery()));
            context.setTask(new SearchShardTask(123L, "", "", "", null, Collections.emptyMap()));
            List<ProjectionField> projectionFields = new ArrayList<>();
            projectionFields.add(new ProjectionField(IndexNumericFieldData.NumericType.LONG, "longpoint"));
            projectionFields.add(new ProjectionField(IndexNumericFieldData.NumericType.INT, "intpoint"));
            // DEFAULT_STREAM_PHASE_SEARCHER.searchWith(
            // context,
            // context.searcher(),
            // context.query(),
            // new LinkedList<>(),
            // projectionFields,
            // false,
            // false
            // );
            VectorSchemaRoot vectorSchemaRoot = context.getArrowCollector().getVectorSchemaRoot(new RootAllocator(Integer.MAX_VALUE));
            System.out.println(vectorSchemaRoot.getSchema());
            Field longPoint = vectorSchemaRoot.getSchema().findField("longpoint");
            assertEquals(longPoint, new Field("longpoint", FieldType.nullable(new ArrowType.Int(64, true)), null));
            BigIntVector vector = (BigIntVector) vectorSchemaRoot.getVector("longpoint");
            assertEquals(vector.getValueCount(), numDocs);
            IntVector intVector = (IntVector) vectorSchemaRoot.getVector("intpoint");
            assertEquals(intVector.getValueCount(), numDocs);
        } finally {
            if (reader != null) reader.close();
            dir.close();
            // flightService.stop();
            // flightService.close();
        }
    }

    private static ContextIndexSearcher newContextSearcher(IndexReader reader, ExecutorService executor) throws IOException {
        SearchContext searchContext = mock(SearchContext.class);
        IndexShard indexShard = mock(IndexShard.class);
        when(searchContext.indexShard()).thenReturn(indexShard);
        SearchOperationListener searchOperationListener = new SearchOperationListener() {
        };
        when(indexShard.getSearchOperationListener()).thenReturn(searchOperationListener);
        when(searchContext.bucketCollectorProcessor()).thenReturn(SearchContext.NO_OP_BUCKET_COLLECTOR_PROCESSOR);
        when(searchContext.shouldUseConcurrentSearch()).thenReturn(executor != null);
        if (executor != null) {
            when(searchContext.getTargetMaxSliceCount()).thenReturn(randomIntBetween(0, 2));
        } else {
            when(searchContext.getTargetMaxSliceCount()).thenThrow(IllegalStateException.class);
        }
        return new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            executor,
            searchContext
        );
    }
}
