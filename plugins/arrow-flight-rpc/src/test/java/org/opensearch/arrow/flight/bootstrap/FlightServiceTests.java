/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.arrow.flight.bootstrap;

import org.opensearch.Version;
<<<<<<< HEAD
import org.opensearch.arrow.flight.bootstrap.tls.SslContextProvider;
=======
import org.opensearch.arrow.flight.bootstrap.tls.DisabledSslContextProvider;
import org.opensearch.arrow.flight.bootstrap.tls.SslContextProvider;
import org.opensearch.client.Client;
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.transport.TransportAddress;
<<<<<<< HEAD
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
=======
import org.opensearch.plugins.SecureTransportSettingsProvider;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlightServiceTests extends OpenSearchTestCase {

    private Settings settings;
    private ClusterService clusterService;
    private NetworkService networkService;
    private ThreadPool threadPool;
<<<<<<< HEAD
=======
    private SecureTransportSettingsProvider secureTransportSettingsProvider;
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
    private final AtomicInteger port = new AtomicInteger(0);
    private DiscoveryNode localNode;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FeatureFlagSetter.set(FeatureFlags.ARROW_STREAMS_SETTING.getKey());
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
        int availablePort = getBasePort(9500) + port.addAndGet(1);
=======
        int availablePort = getBaseStreamPort() + port.addAndGet(1);
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
=======
        int availablePort = generateBasePort(9500) + port.addAndGet(1);
>>>>>>> c538c5739c6 (Fix permissions and other runtime issues)
=======
        int availablePort = getBasePort(9500) + port.addAndGet(1);
>>>>>>> 0643e3c6ded (Fix security policy and FlightClientManagerTests)
        settings = Settings.EMPTY;
        localNode = createNode(availablePort);

        // Setup initial cluster state
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.localNodeId(localNode.getId());
        nodesBuilder.add(localNode);
        DiscoveryNodes nodes = nodesBuilder.build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(nodes).build();
        clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        threadPool = mock(ThreadPool.class);
        when(threadPool.executor(ServerConfig.FLIGHT_SERVER_THREAD_POOL_NAME)).thenReturn(mock(ExecutorService.class));
        when(threadPool.executor(ServerConfig.FLIGHT_CLIENT_THREAD_POOL_NAME)).thenReturn(mock(ExecutorService.class));
<<<<<<< HEAD
=======
        secureTransportSettingsProvider = mock(SecureTransportSettingsProvider.class);
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
        networkService = new NetworkService(Collections.emptyList());
    }

    public void testInitializeWithSslDisabled() throws Exception {
<<<<<<< HEAD
<<<<<<< HEAD

        Settings noSslSettings = Settings.builder().put("arrow.ssl.enable", false).build();
=======
        int testPort = getBaseStreamPort() + port.addAndGet(1);

        Settings noSslSettings = Settings.builder()
            .put("node.attr.transport.stream.port", String.valueOf(testPort))
            .put("arrow.ssl.enable", false)
            .build();
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
=======

        Settings noSslSettings = Settings.builder().put("arrow.ssl.enable", false).build();
>>>>>>> c538c5739c6 (Fix permissions and other runtime issues)

        try (FlightService noSslService = new FlightService(noSslSettings)) {
            noSslService.setClusterService(clusterService);
            noSslService.setThreadPool(threadPool);
            noSslService.setClient(mock(Client.class));
            noSslService.setNetworkService(networkService);
            noSslService.start();
<<<<<<< HEAD
            SslContextProvider sslContextProvider = noSslService.getSslContextProvider();
            assertNull("SSL context provider should be null", sslContextProvider);
=======
            // Verify SSL is properly disabled
            SslContextProvider sslContextProvider = noSslService.getSslContextProvider();
            assertNotNull("SSL context provider should not be null", sslContextProvider);
            assertTrue(
                "SSL context provider should be DisabledSslContextProvider",
                sslContextProvider instanceof DisabledSslContextProvider
            );
            assertFalse("SSL should be disabled", sslContextProvider.isSslEnabled());
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
            assertNotNull(noSslService.getFlightClientManager());
            assertNotNull(noSslService.getBoundAddress());
        }
    }

    public void testStartAndStop() throws Exception {
        try (FlightService testService = new FlightService(Settings.EMPTY)) {
            testService.setClusterService(clusterService);
            testService.setThreadPool(threadPool);
            testService.setClient(mock(Client.class));
            testService.setNetworkService(networkService);
            testService.start();
            testService.stop();
            testService.start();
            assertNotNull(testService.getStreamManager());
        }
    }

    public void testInitializeWithoutSecureTransportSettingsProvider() {
        Settings sslSettings = Settings.builder().put(settings).put("arrow.ssl.enable", true).build();

        try (FlightService sslService = new FlightService(sslSettings)) {
            // Should throw exception when initializing without provider
            expectThrows(RuntimeException.class, () -> {
                sslService.setClusterService(clusterService);
                sslService.setThreadPool(threadPool);
                sslService.setClient(mock(Client.class));
                sslService.setNetworkService(networkService);
                sslService.start();
            });
        }
    }

    public void testServerStartupFailure() {
        Settings invalidSettings = Settings.builder()
            .put(ServerComponents.SETTING_FLIGHT_PUBLISH_PORT.getKey(), "-100") // Invalid port
            .build();
        try (FlightService invalidService = new FlightService(invalidSettings)) {
            invalidService.setClusterService(clusterService);
            invalidService.setThreadPool(threadPool);
            invalidService.setClient(mock(Client.class));
            invalidService.setNetworkService(networkService);
            expectThrows(RuntimeException.class, () -> { invalidService.doStart(); });
        }
    }

    public void testLifecycleStateTransitions() throws Exception {
        // Find new port for this test
        try (FlightService testService = new FlightService(Settings.EMPTY)) {
            testService.setClusterService(clusterService);
            testService.setThreadPool(threadPool);
            testService.setClient(mock(Client.class));
            testService.setNetworkService(networkService);
            // Test all state transitions
            testService.start();
            assertEquals("STARTED", testService.lifecycleState().toString());

            testService.stop();
            assertEquals("STOPPED", testService.lifecycleState().toString());

            testService.close();
            assertEquals("CLOSED", testService.lifecycleState().toString());
        }
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    private DiscoveryNode createNode(int port) throws Exception {
        TransportAddress address = new TransportAddress(InetAddress.getByName("127.0.0.1"), port);
        Map<String, String> attributes = new HashMap<>();
<<<<<<< HEAD
<<<<<<< HEAD
=======
        attributes.put("transport.stream.port", String.valueOf(port));
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
=======
>>>>>>> c538c5739c6 (Fix permissions and other runtime issues)
        attributes.put("arrow.streams.enabled", "true");

        Set<DiscoveryNodeRole> roles = Collections.singleton(DiscoveryNodeRole.DATA_ROLE);
        return new DiscoveryNode("local_node", address, attributes, roles, Version.CURRENT);
    }
<<<<<<< HEAD
<<<<<<< HEAD
=======

    protected static int getBaseStreamPort() {
        return generateBasePort(9401);
    }
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
=======
>>>>>>> c538c5739c6 (Fix permissions and other runtime issues)
}
