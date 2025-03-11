/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight;

import org.apache.arrow.flight.CallOptions;
<<<<<<< HEAD
<<<<<<< HEAD
import org.apache.arrow.flight.FlightClient;
import org.opensearch.arrow.flight.bootstrap.FlightClientManager;
import org.opensearch.arrow.flight.bootstrap.FlightService;
import org.opensearch.arrow.flight.bootstrap.FlightStreamPlugin;
=======
=======
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
<<<<<<< HEAD
>>>>>>> 1c6fcc2042e (Flight Producer changes and integration)
=======
import org.apache.arrow.flight.FlightRuntimeException;
>>>>>>> 2a6590fecdc (Fix concurrency issues in FlightClientManager and FlightStreamManager)
import org.apache.arrow.flight.OSFlightClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.arrow.flight.bootstrap.FlightClientManager;
import org.opensearch.arrow.flight.bootstrap.FlightService;
<<<<<<< HEAD
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
=======
import org.opensearch.arrow.spi.StreamManager;
import org.opensearch.arrow.spi.StreamProducer;
import org.opensearch.arrow.spi.StreamReader;
import org.opensearch.arrow.spi.StreamTicket;
>>>>>>> 1c6fcc2042e (Flight Producer changes and integration)
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 3)
public class ArrowFlightServerIT extends OpenSearchIntegTestCase {

    @BeforeClass
    public static void setupFeatureFlags() {
        FeatureFlagSetter.set(FeatureFlags.ARROW_STREAMS_SETTING.getKey());
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(FlightStreamPlugin.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ensureGreen();
        Thread.sleep(1000);
    }

    public void testArrowFlightEndpoint() {
        for (DiscoveryNode node : getClusterState().nodes()) {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
            try (FlightClient flightClient = flightClientManager.getFlightClient(node.getId()).get()) {
=======
            try (OSFlightClient flightClient = flightClientManager.getFlightClient(node.getId())) {
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
                assertNotNull(flightClient);
                flightClient.handshake(CallOptions.timeout(5000L, TimeUnit.MILLISECONDS));
            }
=======
            OSFlightClient flightClient = flightClientManager.getFlightClient(node.getId());
=======
            FlightService flightService = internalCluster().getInstance(FlightService.class, node.getName());
            FlightClientManager flightClientManager = flightService.getFlightClientManager();
            OSFlightClient flightClient = flightClientManager.getFlightClient(node.getId()).get();
>>>>>>> 2a6590fecdc (Fix concurrency issues in FlightClientManager and FlightStreamManager)
            assertNotNull(flightClient);
            flightClient.handshake(CallOptions.timeout(5000L, TimeUnit.MILLISECONDS));
>>>>>>> 1c6fcc2042e (Flight Producer changes and integration)
        }
    }

    public void testFlightStreamReader() throws Exception {
        for (DiscoveryNode node : getClusterState().nodes()) {
            StreamManager streamManagerRandomNode = getStreamManagerRandomNode();
            StreamTicket ticket = streamManagerRandomNode.registerStream(getStreamProducer(), null);
            StreamManager streamManagerCurrentNode = getStreamManager(node.getName());
            // reader should be accessible from any node in the cluster due to the use ProxyStreamProducer
            try (StreamReader reader = streamManagerCurrentNode.getStreamReader(ticket)) {
                int totalBatches = 0;
                while (reader.next()) {
                    IntVector docIDVector = (IntVector) reader.getRoot().getVector("docID");
                    assertEquals(10, docIDVector.getValueCount());
                    for (int i = 0; i < 10; i++) {
                        assertEquals(docIDVector.toString(), i + (totalBatches * 10L), docIDVector.get(i));
                    }
                    totalBatches++;
                }
                assertEquals(10, totalBatches);
            }
        }
    }

    public void testEarlyCancel() throws Exception {
        DiscoveryNode previousNode = null;
        for (DiscoveryNode node : getClusterState().nodes()) {
            if (previousNode == null) {
                previousNode = node;
                continue;
            }
            StreamManager streamManagerServer = getStreamManager(node.getName());
            TestStreamProducer streamProducer = getStreamProducer();
            StreamTicket ticket = streamManagerServer.registerStream(streamProducer, null);
            StreamManager streamManagerClient = getStreamManager(previousNode.getName());

            CountDownLatch readerComplete = new CountDownLatch(1);
            AtomicReference<Exception> readerException = new AtomicReference<>();
            AtomicReference<StreamReader> readerRef = new AtomicReference<>();

            // Start reader thread
            Thread readerThread = new Thread(() -> {
                try {
                    StreamReader reader = streamManagerClient.getStreamReader(ticket);
                    readerRef.set(reader);
                    assertNotNull(reader.getRoot());
                    IntVector docIDVector = (IntVector) reader.getRoot().getVector("docID");
                    assertNotNull(docIDVector);

                    // Read first batch
                    reader.next();
                    assertEquals(10, docIDVector.getValueCount());
                    for (int i = 0; i < 10; i++) {
                        assertEquals(docIDVector.toString(), i, docIDVector.get(i));
                    }
                    reader.close();
                } catch (Exception e) {
                    readerException.set(e);
                } finally {
                    readerComplete.countDown();
                }
            }, "flight-reader-thread");

            readerThread.start();
            assertTrue("Reader thread did not complete in time", readerComplete.await(1, TimeUnit.SECONDS));

            // Check for any exceptions in reader thread
            if (readerException.get() != null) {
                throw readerException.get();
            }

            StreamReader reader = readerRef.get();

            try {
                reader.next();
                fail("Expected FlightRuntimeException");
            } catch (FlightRuntimeException e) {
                assertEquals("CANCELLED", e.status().code().name());
                assertEquals("Stream closed before end", e.getMessage());
                reader.close();
            }

            // Wait for onCancel to complete
            // Due to https://github.com/grpc/grpc-java/issues/5882, there is a logic in FlightStream.java
            // where it exhausts the stream on the server side before it is actually cancelled.
            assertTrue(
                "Timeout waiting for stream cancellation on server [" + node.getName() + "]",
                streamProducer.waitForClose(2, TimeUnit.SECONDS)
            );
            previousNode = node;
        }
    }

    public void testFlightGetInfo() throws Exception {
        StreamTicket ticket = null;
        for (DiscoveryNode node : getClusterState().nodes()) {
            FlightService flightService = internalCluster().getInstance(FlightService.class, node.getName());
            StreamManager streamManager = flightService.getStreamManager();
            if (ticket == null) {
                ticket = streamManager.registerStream(getStreamProducer(), null);
            }
            FlightClientManager flightClientManager = flightService.getFlightClientManager();
            OSFlightClient flightClient = flightClientManager.getFlightClient(node.getId()).get();
            assertNotNull(flightClient);
            FlightDescriptor flightDescriptor = FlightDescriptor.command(ticket.toBytes());
            FlightInfo flightInfo = flightClient.getInfo(flightDescriptor, CallOptions.timeout(5000L, TimeUnit.MILLISECONDS));
            assertNotNull(flightInfo);
            assertEquals(100, flightInfo.getRecords());
        }
    }

    private StreamManager getStreamManager(String nodeName) {
        FlightService flightService = internalCluster().getInstance(FlightService.class, nodeName);
        return flightService.getStreamManager();
    }

    private StreamManager getStreamManagerRandomNode() {
        FlightService flightService = internalCluster().getInstance(FlightService.class);
        return flightService.getStreamManager();
    }

    private TestStreamProducer getStreamProducer() {
        return new TestStreamProducer();
    }

    private static class TestStreamProducer implements StreamProducer {
        volatile boolean isClosed = false;
        private final CountDownLatch closeLatch = new CountDownLatch(1);

        VectorSchemaRoot root;

        @Override
        public VectorSchemaRoot createRoot(BufferAllocator allocator) {
            IntVector docIDVector = new IntVector("docID", allocator);
            FieldVector[] vectors = new FieldVector[] { docIDVector };
            root = new VectorSchemaRoot(Arrays.asList(vectors));
            return root;
        }

        @Override
        public BatchedJob createJob(BufferAllocator allocator) {
            return new BatchedJob() {
                @Override
                public void run(VectorSchemaRoot root, FlushSignal flushSignal) {
                    IntVector docIDVector = (IntVector) root.getVector("docID");
                    root.setRowCount(10);
                    for (int i = 0; i < 100; i++) {
                        docIDVector.setSafe(i % 10, i);
                        if ((i + 1) % 10 == 0) {
                            flushSignal.awaitConsumption(1000);
                            docIDVector.clear();
                            root.setRowCount(10);
                        }
                    }
                }

                @Override
                public void onCancel() {
                    root.close();
                    isClosed = true;
                }

                @Override
                public boolean isCancelled() {
                    return isClosed;
                }
            };
        }

        @Override
        public TimeValue getJobDeadline() {
            return TimeValue.timeValueSeconds(5);
        }

        @Override
        public int estimatedRowCount() {
            return 100;
        }

        @Override
        public String getAction() {
            return "";
        }

        @Override
        public void close() {
            root.close();
            closeLatch.countDown();
            isClosed = true;
        }

        public boolean waitForClose(long timeout, TimeUnit unit) throws InterruptedException {
            return closeLatch.await(timeout, unit);
        }
    }
}
