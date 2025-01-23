/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.arrow.flight.bootstrap;

<<<<<<< HEAD
import org.apache.arrow.flight.FlightClient;
=======
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.OSFlightClient;
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.opensearch.Version;
import org.opensearch.arrow.flight.api.NodeFlightInfo;
import org.opensearch.arrow.flight.api.NodesFlightInfoAction;
import org.opensearch.arrow.flight.api.NodesFlightInfoRequest;
import org.opensearch.arrow.flight.api.NodesFlightInfoResponse;
import org.opensearch.arrow.flight.bootstrap.tls.SslContextProvider;
<<<<<<< HEAD
=======
import org.opensearch.client.Client;
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
<<<<<<< HEAD
import org.opensearch.transport.client.Client;
=======
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

<<<<<<< HEAD
<<<<<<< HEAD
import io.netty.channel.EventLoopGroup;
import io.netty.util.NettyRuntime;
=======
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
=======
>>>>>>> 0643e3c6ded (Fix security policy and FlightClientManagerTests)
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.util.NettyRuntime;
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)

import static org.opensearch.arrow.flight.bootstrap.FlightClientManager.LOCATION_TIMEOUT_MS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class FlightClientManagerTests extends OpenSearchTestCase {

    private static BufferAllocator allocator;
    private static EventLoopGroup elg;
    private static ExecutorService executorService;
    private static final AtomicInteger port = new AtomicInteger(0);

    private ClusterService clusterService;
    private Client client;
    private ClusterState state;
    private FlightClientManager clientManager;
    private ScheduledExecutorService locationUpdaterExecutor;

    @BeforeClass
    public static void setupClass() throws Exception {
        ServerConfig.init(Settings.EMPTY);
        allocator = new RootAllocator();
        elg = ServerConfig.createELG("test-grpc-worker-elg", NettyRuntime.availableProcessors() * 2);
        executorService = ServerConfig.createELG("test-grpc-worker", NettyRuntime.availableProcessors() * 2);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        locationUpdaterExecutor = Executors.newScheduledThreadPool(1);

        FeatureFlagSetter.set(FeatureFlags.ARROW_STREAMS_SETTING.getKey());
        clusterService = mock(ClusterService.class);
        client = mock(Client.class);
        state = getDefaultState();
        when(clusterService.state()).thenReturn(state);

        mockFlightInfoResponse(state.nodes(), 0);

<<<<<<< HEAD
        SslContextProvider sslContextProvider = null;
=======
        SslContextProvider sslContextProvider = mock(SslContextProvider.class);
<<<<<<< HEAD
        SslContext clientSslContext = GrpcSslContexts.configure(SslContextBuilder.forClient()).build();
        when(sslContextProvider.isSslEnabled()).thenReturn(true);
        when(sslContextProvider.getClientSslContext()).thenReturn(clientSslContext);
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
=======
        when(sslContextProvider.isSslEnabled()).thenReturn(false);
>>>>>>> 0643e3c6ded (Fix security policy and FlightClientManagerTests)

        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(ServerConfig.FLIGHT_CLIENT_THREAD_POOL_NAME)).thenReturn(executorService);
        clientManager = new FlightClientManager(allocator, clusterService, sslContextProvider, elg, threadPool, client);
        ClusterChangedEvent event = new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE);
        clientManager.clusterChanged(event);
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
        assertBusy(() -> {
            assertEquals("Flight client isn't built in time limit", 2, clientManager.getFlightClients().size());
            assertNotNull("local_node should exist", clientManager.getFlightClient("local_node").get());
            assertNotNull("remote_node should exist", clientManager.getFlightClient("remote_node").get());
        }, 2, TimeUnit.SECONDS);
=======

        clientManager.updateFlightClients();
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
=======
        clientManager.updateFlightClients();
        assertBusy(
            () -> { assertFalse("Flight client isn't built in time limit", clientManager.getClients().isEmpty()); },
            2,
            TimeUnit.SECONDS
        );
>>>>>>> 7c0193005be (Fix the issue with single node ClientManager)
=======
=======
>>>>>>> 6b8a6e94e0d (Fix the issue with single node ClientManager)
        assertBusy(() -> {
            assertEquals("Flight client isn't built in time limit", 2, clientManager.getClients().size());
            assertNotNull("local_node should exist", clientManager.getFlightClient("local_node"));
            assertNotNull("remote_node should exist", clientManager.getFlightClient("remote_node"));
        }, 2, TimeUnit.SECONDS);
<<<<<<< HEAD
>>>>>>> 0643e3c6ded (Fix security policy and FlightClientManagerTests)
=======
=======
        clientManager.updateFlightClients();
        assertBusy(
            () -> { assertFalse("Flight client isn't built in time limit", clientManager.getClients().isEmpty()); },
            2,
            TimeUnit.SECONDS
        );
>>>>>>> 8cc555604ef (Fix the issue with single node ClientManager)
>>>>>>> 6b8a6e94e0d (Fix the issue with single node ClientManager)
    }

    private void mockFlightInfoResponse(DiscoveryNodes nodes, int sleepDuration) {
        doAnswer(invocation -> {
            locationUpdaterExecutor.schedule(() -> {
                try {
                    NodesFlightInfoRequest request = invocation.getArgument(1);
                    ActionListener<NodesFlightInfoResponse> listener = invocation.getArgument(2);

                    List<NodeFlightInfo> nodeInfos = new ArrayList<>();
                    for (DiscoveryNode node : nodes) {
                        if (request.nodesIds().length == 0 || Arrays.asList(request.nodesIds()).contains(node.getId())) {
                            int flightPort = getBaseStreamPort() + port.addAndGet(2);
                            TransportAddress address = new TransportAddress(
                                InetAddress.getByName(node.getAddress().getAddress()),
                                flightPort
                            );
                            BoundTransportAddress boundAddress = new BoundTransportAddress(new TransportAddress[] { address }, address);
                            NodeFlightInfo nodeInfo = new NodeFlightInfo(node, boundAddress);
                            nodeInfos.add(nodeInfo);
                        }
                    }
                    NodesFlightInfoResponse response = new NodesFlightInfoResponse(ClusterName.DEFAULT, nodeInfos, Collections.emptyList());
                    listener.onResponse(response);
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
            }, sleepDuration, TimeUnit.MILLISECONDS);
            return null;
        }).when(client).execute(eq(NodesFlightInfoAction.INSTANCE), any(NodesFlightInfoRequest.class), any(ActionListener.class));

    }

    @Override
    public void tearDown() throws Exception {
        locationUpdaterExecutor.shutdown();
        super.tearDown();
        clientManager.close();
    }

    private ClusterState getDefaultState() throws Exception {
        int testPort = getBasePort() + port.addAndGet(2);

        DiscoveryNode localNode = createNode("local_node", "127.0.0.1", testPort);
        DiscoveryNode remoteNode = createNode("remote_node", "127.0.0.2", testPort + 1);

        // Setup initial cluster state
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(remoteNode);
        nodesBuilder.add(localNode);
        nodesBuilder.localNodeId(localNode.getId());
        DiscoveryNodes nodes = nodesBuilder.build();

        return ClusterState.builder(new ClusterName("test")).nodes(nodes).build();
    }

    private DiscoveryNode createNode(String nodeId, String host, int port) throws Exception {
        TransportAddress address = new TransportAddress(InetAddress.getByName(host), port);
        Map<String, String> attributes = new HashMap<>();
        attributes.put("arrow.streams.enabled", "true");
        Set<DiscoveryNodeRole> roles = Collections.singleton(DiscoveryNodeRole.DATA_ROLE);
        return new DiscoveryNode(nodeId, address, attributes, roles, Version.CURRENT);
    }

    @AfterClass
    public static void tearClass() {
        allocator.close();
    }

    public void testGetFlightClientForExistingNode() {
        validateNodes();
    }

<<<<<<< HEAD
    public void testGetFlightClientForNonExistentNode() throws Exception {
        assertFalse(clientManager.getFlightClient("non_existent_node").isPresent());
=======
    public void testGetFlightClientLocation() {
        for (DiscoveryNode node : state.nodes()) {
            Location location = clientManager.getFlightClientLocation(node.getId());
            assertNotNull("Flight client location should be returned", location);
            assertEquals("Location host should match", node.getHostAddress(), location.getUri().getHost());
        }
    }

    public void testGetFlightClientForNonExistentNode() throws Exception {
        assertNull(clientManager.getFlightClient("non_existent_node"));
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
    }

    public void testClusterChangedWithNodesChanged() throws Exception {
        DiscoveryNode newNode = createNode("new_node", "127.0.0.3", getBasePort() + port.addAndGet(1));
        DiscoveryNodes.Builder newNodesBuilder = DiscoveryNodes.builder();

        for (DiscoveryNode node : state.nodes()) {
            newNodesBuilder.add(node);
        }
        newNodesBuilder.localNodeId("local_node");
        // Update cluster state with new node
        newNodesBuilder.add(newNode);
        DiscoveryNodes newNodes = newNodesBuilder.build();

        ClusterState newState = ClusterState.builder(new ClusterName("test")).nodes(newNodes).build();
        mockFlightInfoResponse(newNodes, 0);
        when(clusterService.state()).thenReturn(newState);
        clientManager.clusterChanged(new ClusterChangedEvent("test", newState, state));
<<<<<<< HEAD
<<<<<<< HEAD

        for (DiscoveryNode node : newState.nodes()) {
            assertBusy(
                () -> { assertTrue("Flight client isn't built in time limit", clientManager.getFlightClient(node.getId()).isPresent()); },
                2,
                TimeUnit.SECONDS
            );
=======
        clientManager.updateFlightClients();
=======
>>>>>>> 0643e3c6ded (Fix security policy and FlightClientManagerTests)

        for (DiscoveryNode node : newState.nodes()) {
<<<<<<< HEAD
            assertNotNull(clientManager.getFlightClient(node.getId()));
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
=======
            assertBusy(
                () -> { assertNotNull("Flight client isn't built in time limit", clientManager.getFlightClient(node.getId())); },
                2,
                TimeUnit.SECONDS
            );
>>>>>>> 7c0193005be (Fix the issue with single node ClientManager)
        }
    }

    public void testClusterChangedWithNoNodesChanged() throws Exception {
        ClusterChangedEvent event = new ClusterChangedEvent("test", state, state);
        clientManager.clusterChanged(event);

        // Verify original client still exists
        for (DiscoveryNode node : state.nodes()) {
<<<<<<< HEAD
            assertNotNull(clientManager.getFlightClient(node.getId()).get());
=======
            assertNotNull(clientManager.getFlightClient(node.getId()));
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
        }
    }

    public void testGetLocalNodeId() throws Exception {
        assertEquals("Local node ID should match", "local_node", clientManager.getLocalNodeId());
    }

    public void testCloseWithActiveClients() throws Exception {
        for (DiscoveryNode node : state.nodes()) {
<<<<<<< HEAD
            FlightClient client = clientManager.getFlightClient(node.getId()).get();
=======
            OSFlightClient client = clientManager.getFlightClient(node.getId());
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
            assertNotNull(client);
        }

        clientManager.close();
        assertEquals(0, clientManager.getFlightClients().size());
    }

    public void testIncompatibleNodeVersion() throws Exception {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("arrow.streams.enabled", "true");
        DiscoveryNode oldVersionNode = new DiscoveryNode(
            "old_version_node",
            new TransportAddress(InetAddress.getByName("127.0.0.3"), getBasePort() + port.addAndGet(1)),
            attributes,
            Collections.singleton(DiscoveryNodeRole.DATA_ROLE),
            Version.fromString("2.18.0")  // Version before Arrow Flight introduction
        );

        // Update cluster state with old version node
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(oldVersionNode);
        nodesBuilder.localNodeId("local_node");
        DiscoveryNodes nodes = nodesBuilder.build();
        ClusterState oldVersionState = ClusterState.builder(new ClusterName("test")).nodes(nodes).build();

        when(clusterService.state()).thenReturn(oldVersionState);
        mockFlightInfoResponse(nodes, 0);

<<<<<<< HEAD
        assertFalse(clientManager.getFlightClient(oldVersionNode.getId()).isPresent());
=======
        assertNull(clientManager.getFlightClient(oldVersionNode.getId()));
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
    }

    public void testGetFlightClientLocationTimeout() throws Exception {
        reset(client);

        String nodeId = "test_node";
        DiscoveryNode testNode = createNode(nodeId, "127.0.0.1", getBasePort() + port.addAndGet(2));

        // Update cluster state with the test node
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(testNode);
        nodesBuilder.localNodeId(nodeId);
        ClusterState newState = ClusterState.builder(new ClusterName("test")).nodes(nodesBuilder.build()).build();
        when(clusterService.state()).thenReturn(newState);
        // Mock a delayed response that will cause timeout
        mockFlightInfoResponse(newState.nodes(), LOCATION_TIMEOUT_MS + 100);

        ClusterChangedEvent event = new ClusterChangedEvent("test", newState, ClusterState.EMPTY_STATE);
        clientManager.clusterChanged(event);
<<<<<<< HEAD
<<<<<<< HEAD
        assertFalse(clientManager.getFlightClient(nodeId).isPresent());
=======

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> { clientManager.getFlightClient(nodeId); });
        assertTrue(exception.getMessage().contains("Timeout waiting for Flight server location"));
=======
        assertNull(clientManager.getFlightClient(nodeId));
>>>>>>> 7c0193005be (Fix the issue with single node ClientManager)
    }

<<<<<<< HEAD
    public void testGetFlightClientLocationInterrupted() throws Exception {
        reset(client);

        String nodeId = "test_node";
        DiscoveryNode testNode = createNode(nodeId, "127.0.0.1", getBasePort() + port.addAndGet(2));

        // Update cluster state with the test node
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(testNode);
        nodesBuilder.localNodeId(nodeId);
        ClusterState newState = ClusterState.builder(new ClusterName("test")).nodes(nodesBuilder.build()).build();

        when(clusterService.state()).thenReturn(newState);

        // Mock an interrupted response
        doAnswer(invocation -> {
            Thread currentThread = Thread.currentThread();
            locationUpdaterExecutor.schedule(currentThread::interrupt, 100, TimeUnit.MILLISECONDS);
            return null;
        }).when(client).execute(eq(NodesFlightInfoAction.INSTANCE), any(NodesFlightInfoRequest.class), any(ActionListener.class));

        ClusterChangedEvent event = new ClusterChangedEvent("test", newState, ClusterState.EMPTY_STATE);
        clientManager.clusterChanged(event);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> { clientManager.getFlightClient(nodeId); });
        assertTrue(exception.getMessage().contains("Interrupted while waiting for Flight server location"));
        assertTrue(Thread.interrupted());
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
    }

=======
>>>>>>> 9cba7faf7a6 (remove testGetFlightClientLocationExecutionError as thread interruption was causing client close failure)
    public void testGetFlightClientLocationExecutionError() throws Exception {
        reset(client);

        String nodeId = "test_node";
        DiscoveryNode testNode = createNode(nodeId, "127.0.0.1", getBasePort() + port.addAndGet(2));

        // Update cluster state with the test node
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(testNode);
        nodesBuilder.localNodeId(nodeId);
        ClusterState newState = ClusterState.builder(new ClusterName("test")).nodes(nodesBuilder.build()).build();

        when(clusterService.state()).thenReturn(newState);

        // Mock failure
        doAnswer(invocation -> {
            ActionListener<NodesFlightInfoResponse> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException("Test execution error"));
            return null;
        }).when(client).execute(eq(NodesFlightInfoAction.INSTANCE), any(NodesFlightInfoRequest.class), any(ActionListener.class));

        ClusterChangedEvent event = new ClusterChangedEvent("test", newState, ClusterState.EMPTY_STATE);
        clientManager.clusterChanged(event);

<<<<<<< HEAD
<<<<<<< HEAD
        assertFalse(clientManager.getFlightClient(nodeId).isPresent());
=======
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> { clientManager.getFlightClient(nodeId); });
        assertTrue(exception.getMessage().contains("Error getting Flight server location"));
        assertTrue(exception.getCause() instanceof RuntimeException);
        assertEquals("Test execution error", exception.getCause().getMessage());
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
=======
        assertNull(clientManager.getFlightClient(nodeId));
>>>>>>> 7c0193005be (Fix the issue with single node ClientManager)
    }

    public void testFailedClusterUpdateButSuccessfulDirectRequest() throws Exception {
        reset(client);

        String nodeId = "test_node";
        DiscoveryNode testNode = createNode(nodeId, "127.0.0.1", getBasePort() + port.addAndGet(2));

        // Update cluster state with the test node
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(testNode);
        nodesBuilder.localNodeId(nodeId);
        ClusterState newState = ClusterState.builder(new ClusterName("test")).nodes(nodesBuilder.build()).build();

        when(clusterService.state()).thenReturn(newState);

        // First mock call fails during cluster update
        AtomicBoolean firstCall = new AtomicBoolean(true);
        doAnswer(invocation -> {
            locationUpdaterExecutor.schedule(() -> {
                ActionListener<NodesFlightInfoResponse> listener = invocation.getArgument(2);
                if (firstCall.getAndSet(false)) {
                    // Fail on first call (during cluster update)
                    listener.onFailure(new RuntimeException("Failed during cluster update"));
                } else {
                    // Succeed on second call (direct request)
                    try {
                        NodesFlightInfoRequest request = invocation.getArgument(1);
                        List<NodeFlightInfo> nodeInfos = new ArrayList<>();
                        for (DiscoveryNode node : newState.nodes()) {
                            if (request.nodesIds().length == 0 || Arrays.asList(request.nodesIds()).contains(node.getId())) {
                                int flightPort = getBaseStreamPort() + port.addAndGet(2);
                                TransportAddress address = new TransportAddress(
                                    InetAddress.getByName(node.getAddress().getAddress()),
                                    flightPort
                                );
                                BoundTransportAddress boundAddress = new BoundTransportAddress(new TransportAddress[] { address }, address);
                                NodeFlightInfo nodeInfo = new NodeFlightInfo(node, boundAddress);
                                nodeInfos.add(nodeInfo);
                            }
                        }
                        NodesFlightInfoResponse response = new NodesFlightInfoResponse(
                            ClusterName.DEFAULT,
                            nodeInfos,
                            Collections.emptyList()
                        );
                        listener.onResponse(response);
                    } catch (UnknownHostException e) {
                        throw new RuntimeException(e);
                    }
                }
            }, 0, TimeUnit.MICROSECONDS);
            return null;
        }).when(client).execute(eq(NodesFlightInfoAction.INSTANCE), any(NodesFlightInfoRequest.class), any(ActionListener.class));

        ClusterChangedEvent event = new ClusterChangedEvent("test", newState, ClusterState.EMPTY_STATE);
        clientManager.clusterChanged(event);

        // Verify that the client can still be created successfully on direct request
<<<<<<< HEAD
<<<<<<< HEAD
        clientManager.buildClientAsync(nodeId);
        assertBusy(() -> {
            assertTrue("Flight client should be created successfully on direct request", clientManager.getFlightClient(nodeId).isPresent());
        }, 2, TimeUnit.SECONDS);
        assertFalse("first call should be invoked", firstCall.get());
=======
        OSFlightClient flightClient = clientManager.getFlightClient(nodeId);
        assertFalse("first call should be invoked", firstCall.get());
        assertNotNull("Flight client should be created successfully on direct request", flightClient);
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
=======
        clientManager.buildClientAsync(nodeId);
        assertBusy(
            () -> {
                assertNotNull("Flight client should be created successfully on direct request", clientManager.getFlightClient(nodeId));
            },
            2,
            TimeUnit.SECONDS
        );
        assertFalse("first call should be invoked", firstCall.get());
>>>>>>> 7c0193005be (Fix the issue with single node ClientManager)
    }

    private void validateNodes() {
        for (DiscoveryNode node : state.nodes()) {
<<<<<<< HEAD
            FlightClient client = clientManager.getFlightClient(node.getId()).get();
=======
            OSFlightClient client = clientManager.getFlightClient(node.getId());
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
            assertNotNull("Flight client should be created for existing node", client);
        }
    }

    protected static int getBaseStreamPort() {
<<<<<<< HEAD
<<<<<<< HEAD
        return getBasePort(9401);
=======
        return generateBasePort(9401);
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
=======
        return getBasePort(9401);
>>>>>>> 0643e3c6ded (Fix security policy and FlightClientManagerTests)
    }
}
