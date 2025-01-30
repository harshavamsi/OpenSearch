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
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.OSFlightClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
<<<<<<< HEAD
import org.opensearch.common.Nullable;
=======
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.threadpool.ThreadPool;
<<<<<<< HEAD
import org.opensearch.transport.client.Client;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import io.netty.channel.EventLoopGroup;
=======

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)

import static org.opensearch.common.util.FeatureFlags.ARROW_STREAMS_SETTING;

/**
 * Manages Flight client connections to OpenSearch nodes in a cluster.
 * This class maintains a pool of Flight clients for internode communication,
 * handles client lifecycle, and responds to cluster state changes.
 *
 * <p>The manager implements ClusterStateListener to automatically update
 * client connections when nodes join or leave the cluster. </p>
 */
public class FlightClientManager implements ClusterStateListener, AutoCloseable {
<<<<<<< HEAD
<<<<<<< HEAD
    private static final Version MIN_SUPPORTED_VERSION = Version.V_3_0_0;
=======
    private static final Version MIN_SUPPORTED_VERSION = Version.fromString("2.19.0");
>>>>>>> 0643e3c6ded (Fix security policy and FlightClientManagerTests)
    private static final Logger logger = LogManager.getLogger(FlightClientManager.class);
    static final int LOCATION_TIMEOUT_MS = 1000;
    private final ExecutorService grpcExecutor;
    private final ClientConfiguration clientConfig;
    private final Map<String, FlightClient> flightClients = new ConcurrentHashMap<>();
    private final Client client;
=======
    private static final Version MIN_SUPPORTED_VERSION = Version.fromString("3.0.0");
    private static final Logger logger = LogManager.getLogger(FlightClientManager.class);
    static final int LOCATION_TIMEOUT_MS = 1000;
    private final ExecutorService grpcExecutor;
<<<<<<< HEAD
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
=======
    private final ClientConfiguration clientConfig;
    private final Map<String, ClientHolder> flightClients = new ConcurrentHashMap<>();
    private final Client client;
<<<<<<< HEAD
>>>>>>> 7c0193005be (Fix the issue with single node ClientManager)
=======
    private static final long CLIENT_BUILD_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(1);
>>>>>>> 2a6590fecdc (Fix concurrency issues in FlightClientManager and FlightStreamManager)

    /**
     * Creates a new FlightClientManager instance.
     *
     * @param allocator          Supplier for buffer allocation
     * @param clusterService     Service for cluster state management
     * @param sslContextProvider Provider for SSL/TLS context configuration
     * @param elg                Event loop group for network operations
     * @param threadPool         Thread pool for executing tasks asynchronously
     * @param client             OpenSearch client
     */
    public FlightClientManager(
        BufferAllocator allocator,
        ClusterService clusterService,
<<<<<<< HEAD
        @Nullable SslContextProvider sslContextProvider,
=======
        SslContextProvider sslContextProvider,
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
        EventLoopGroup elg,
        ThreadPool threadPool,
        Client client
    ) {
        grpcExecutor = threadPool.executor(ServerConfig.FLIGHT_CLIENT_THREAD_POOL_NAME);
        this.clientConfig = new ClientConfiguration(
            Objects.requireNonNull(allocator, "BufferAllocator cannot be null"),
            Objects.requireNonNull(clusterService, "ClusterService cannot be null"),
<<<<<<< HEAD
            sslContextProvider,
=======
            Objects.requireNonNull(sslContextProvider, "SslContextProvider cannot be null"),
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
            Objects.requireNonNull(elg, "EventLoopGroup cannot be null"),
            Objects.requireNonNull(grpcExecutor, "ExecutorService cannot be null")
        );
        this.client = Objects.requireNonNull(client, "Client cannot be null");
<<<<<<< HEAD
<<<<<<< HEAD
=======
        this.clientPool = new ClientPool();
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
=======
>>>>>>> 7c0193005be (Fix the issue with single node ClientManager)
        clusterService.addListener(this);
    }

    /**
<<<<<<< HEAD
     * Returns a Flight client for a given node ID.
     *
     * @param nodeId The ID of the node for which to retrieve the Flight client
     * @return An OpenSearchFlightClient instance for the specified node
     */
<<<<<<< HEAD
    public Optional<FlightClient> getFlightClient(String nodeId) {
        return Optional.ofNullable(flightClients.get(nodeId));
    }

    /**
     * Builds a client for a given nodeId in asynchronous manner
     * @param nodeId nodeId of the node to build client for
     */
    public void buildClientAsync(String nodeId) {
        CompletableFuture<Location> locationFuture = new CompletableFuture<>();
        locationFuture.thenAccept(location -> {
            DiscoveryNode node = getNodeFromClusterState(nodeId);
            buildClientAndAddToPool(location, node);
        }).exceptionally(throwable -> {
            logger.error("Failed to get Flight server location for node: [{}] {}", nodeId, throwable);
            throw new RuntimeException(throwable);
        });
        requestNodeLocation(nodeId, locationFuture);
    }

    private void buildClientAndAddToPool(Location location, DiscoveryNode node) {
        if (!isValidNode(node)) {
            logger.warn(
                "Unable to build FlightClient for node [{}] with role [{}] on version [{}]",
                node.getId(),
                node.getRoles(),
                node.getVersion()
            );
            return;
        }
        flightClients.computeIfAbsent(node.getId(), key -> buildClient(location));
    }

    private void requestNodeLocation(String nodeId, CompletableFuture<Location> future) {
        NodesFlightInfoRequest request = new NodesFlightInfoRequest(nodeId);
        try {

            client.execute(NodesFlightInfoAction.INSTANCE, request, new ActionListener<>() {
                @Override
                public void onResponse(NodesFlightInfoResponse response) {
                    NodeFlightInfo nodeInfo = response.getNodesMap().get(nodeId);
                    if (nodeInfo != null) {
                        TransportAddress publishAddress = nodeInfo.getBoundAddress().publishAddress();
                        String address = publishAddress.getAddress();
                        int flightPort = publishAddress.address().getPort();
                        Location location = clientConfig.sslContextProvider != null
                            ? Location.forGrpcTls(address, flightPort)
                            : Location.forGrpcInsecure(address, flightPort);

                        future.complete(location);
                    } else {
                        future.completeExceptionally(new IllegalStateException("No Flight info received for node: [" + nodeId + "]"));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    future.completeExceptionally(e);
                    logger.error("Failed to get Flight server info for node: [{}] {}", nodeId, e);
                }
            });
        } catch (final Exception ex) {
            future.completeExceptionally(ex);
        }
    }

    private FlightClient buildClient(Location location) {
        return OSFlightClient.builder()
            .allocator(clientConfig.allocator)
            .location(location)
            .channelType(ServerConfig.clientChannelType())
            .eventLoopGroup(clientConfig.workerELG)
            .sslContext(clientConfig.sslContextProvider != null ? clientConfig.sslContextProvider.getClientSslContext() : null)
            .executor(clientConfig.grpcExecutor)
            .build();
    }

    private DiscoveryNode getNodeFromClusterState(String nodeId) {
        return Objects.requireNonNull(clientConfig.clusterService).state().nodes().get(nodeId);
    }

    /**
     * Closes the FlightClientManager and all associated Flight clients.
     */
    @Override
    public void close() throws Exception {
        for (FlightClient flightClient : flightClients.values()) {
            flightClient.close();
        }
        flightClients.clear();
        grpcExecutor.shutdown();
        grpcExecutor.awaitTermination(5, TimeUnit.SECONDS);
        clientConfig.clusterService.removeListener(this);
=======
    public OSFlightClient getFlightClient(String nodeId) {
        ClientHolder clientHolder = flightClients.getOrDefault(nodeId, null);
        return clientHolder != null ? clientHolder.flightClient : null;
    }

    /**
=======
>>>>>>> 2a6590fecdc (Fix concurrency issues in FlightClientManager and FlightStreamManager)
     * Returns the location of a Flight client for a given node ID.
     *
     * @param nodeId The ID of the node for which to retrieve the location
     * @return The Location of the Flight client for the specified node
     */
    public Location getFlightClientLocation(String nodeId) {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
        return nodeLocations.get(nodeId);
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
=======
        return flightClients.containsKey(nodeId) ? flightClients.get(nodeId).location : null;
>>>>>>> 7c0193005be (Fix the issue with single node ClientManager)
=======
        ClientHolder clientHolder = flightClients.getOrDefault(nodeId, null);
        return clientHolder != null ? clientHolder.location : null;
>>>>>>> 0643e3c6ded (Fix security policy and FlightClientManagerTests)
=======
        ClientHolder clientHolder = flightClients.get(nodeId);
        if (clientHolder != null && clientHolder.location != null) {
            return clientHolder.location;
        }
        buildClientAsync(nodeId);
        return null;
>>>>>>> 2a6590fecdc (Fix concurrency issues in FlightClientManager and FlightStreamManager)
    }

    /**
     * Returns a Flight client for a given node ID.
     *
     * @param nodeId The ID of the node for which to retrieve the Flight client
     * @return An OpenSearchFlightClient instance for the specified node
     */
<<<<<<< HEAD
<<<<<<< HEAD
    public String getLocalNodeId() {
        return Objects.requireNonNull(clientConfig.clusterService).state().nodes().getLocalNodeId();
    }

    /**
<<<<<<< HEAD
=======
     * Closes the FlightClientManager and all associated Flight clients.
     */
    @Override
    public void close() throws Exception {
        nodeLocations.clear();
        clientPool.close();
        grpcExecutor.shutdown();
    }

    /**
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
     * Handles cluster state changes by updating node locations and managing client connections.
     *
     * @param event The ClusterChangedEvent containing information about the cluster state change
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesChanged()) {
<<<<<<< HEAD
            DiscoveryNodes nodes = event.state().nodes();
            flightClients.keySet().removeIf(nodeId -> !nodes.nodeExists(nodeId));
            for (DiscoveryNode node : nodes) {
                if (!flightClients.containsKey(node.getId()) && isValidNode(node)) {
                    buildClientAsync(node.getId());
                }
=======
            updateNodeLocations(event.state().nodes());
        }
    }

    private void updateNodeLocations(DiscoveryNodes nodes) {
        nodeLocations.keySet().removeIf(nodeId -> !nodes.nodeExists(nodeId));
        for (DiscoveryNode node : nodes) {
            if (!nodeLocations.containsKey(node.getId()) && isValidNode(node)) {
                CompletableFuture<Location> locationFuture = new CompletableFuture<>();
                requestNodeLocation(node, locationFuture);
                locationFuture.thenAccept(location -> { nodeLocations.put(node.getId(), location); }).exceptionally(throwable -> {
                    logger.error("Failed to get Flight server location for node: {}{}", node.getId(), throwable);
                    return null;
                });
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
=======
    public Optional<OSFlightClient> getFlightClient(String nodeId) {
        if (nodeId == null || nodeId.isEmpty()) {
            throw new IllegalArgumentException("Node ID cannot be null or empty");
        }

        ClientHolder holder = flightClients.get(nodeId);

        if (holder == null) {
            buildClientAsync(nodeId);
            return Optional.empty();
        }

        if (holder.state == BuildState.COMPLETE) {
            return Optional.ofNullable(holder.flightClient);
        }

        if (holder.isStale()) {
            logger.warn("Detected stale building state for node [{}], triggering rebuild", nodeId);
            if (flightClients.remove(nodeId, holder)) {
                try {
                    holder.close();
                } catch (Exception e) {
                    logger.warn("Error closing stale client holder for node [{}]. {}", nodeId, e.getMessage());
                }
                buildClientAsync(nodeId);
            }
        }

        return Optional.empty();
    }

    /**
     * Represents the state and metadata of a Flight client
     */
    private record ClientHolder(OSFlightClient flightClient, Location location, long buildStartTime, BuildState state)
        implements
            AutoCloseable {

        private static ClientHolder building() {
            return new ClientHolder(null, null, System.currentTimeMillis(), BuildState.BUILDING);
        }

        private static ClientHolder complete(OSFlightClient client, Location location) {
            return new ClientHolder(client, location, System.currentTimeMillis(), BuildState.COMPLETE);
        }

        boolean isStale() {
            return state == BuildState.BUILDING && (System.currentTimeMillis() - buildStartTime) > CLIENT_BUILD_TIMEOUT_MS;
        }

        /**
         * Closes the client holder and logs the operation
         * @param nodeId The ID of the node this holder belongs to
         * @param reason The reason for closing
         */
        public void close(String nodeId, String reason) {
            try {
                if (flightClient != null) {
                    flightClient.close();
                }
                if (state == BuildState.BUILDING) {
                    logger.info("Cleaned up building state for node [{}]: {}", nodeId, reason);
                } else {
                    logger.info("Closed client for node [{}]: {}", nodeId, reason);
                }
            } catch (Exception e) {
                logger.error("Failed to close client for node [{}] ({}): {}", nodeId, reason, e.getMessage());
            }
        }

        @Override
        public void close() throws Exception {
            if (flightClient != null) {
                flightClient.close();
>>>>>>> 2a6590fecdc (Fix concurrency issues in FlightClientManager and FlightStreamManager)
            }
        }
    }

<<<<<<< HEAD
<<<<<<< HEAD
=======
    private OSFlightClient buildFlightClient(String nodeId) {
        DiscoveryNode node = getNodeFromCluster(nodeId);
        if (!isValidNode(node)) {
            return null;
        }

        Location location = nodeLocations.get(nodeId);
        if (location != null) {
            return buildClient(location);
        }

        // If location is not available, request it
=======
    public void buildClientAsync(String nodeId) {
>>>>>>> 7c0193005be (Fix the issue with single node ClientManager)
=======
    private enum BuildState {
        BUILDING,
        COMPLETE
    }

    /**
     * Initiates async build of a flight client for the given node
     */
    void buildClientAsync(String nodeId) {
        // Try to put a building placeholder
        ClientHolder placeholder = ClientHolder.building();
        if (flightClients.putIfAbsent(nodeId, placeholder) != null) {
            return; // Another thread is already handling this node
        }

>>>>>>> 2a6590fecdc (Fix concurrency issues in FlightClientManager and FlightStreamManager)
        CompletableFuture<Location> locationFuture = new CompletableFuture<>();
        locationFuture.thenAccept(location -> {
            try {
                DiscoveryNode node = getNodeFromClusterState(nodeId);
                if (!isValidNode(node)) {
                    logger.warn("Node [{}] is not valid for client creation", nodeId);
                    flightClients.remove(nodeId, placeholder);
                    return;
                }

                OSFlightClient flightClient = buildClient(location);
                ClientHolder newHolder = ClientHolder.complete(flightClient, location);

                if (!flightClients.replace(nodeId, placeholder, newHolder)) {
                    // Something changed while we were building
                    logger.warn("Failed to store new client for node [{}], state changed during build", nodeId);
                    flightClient.close();
                }
            } catch (Exception e) {
                logger.error("Failed to build Flight client for node [{}]. {}", nodeId, e);
                flightClients.remove(nodeId, placeholder);
                throw new RuntimeException(e);
            }
        }).exceptionally(throwable -> {
            flightClients.remove(nodeId, placeholder);
            logger.error("Failed to get Flight server location for node [{}] {}", nodeId, throwable);
            throw new CompletionException(throwable);
        });

        requestNodeLocation(nodeId, locationFuture);
    }

    Collection<ClientHolder> getClients() {
        return flightClients.values();
    }

    private void requestNodeLocation(String nodeId, CompletableFuture<Location> future) {
        NodesFlightInfoRequest request = new NodesFlightInfoRequest(nodeId);
        client.execute(NodesFlightInfoAction.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(NodesFlightInfoResponse response) {
                NodeFlightInfo nodeInfo = response.getNodesMap().get(nodeId);
                if (nodeInfo != null) {
                    TransportAddress publishAddress = nodeInfo.getBoundAddress().publishAddress();
                    String address = publishAddress.getAddress();
                    int flightPort = publishAddress.address().getPort();
                    Location location = clientConfig.sslContextProvider.isSslEnabled()
                        ? Location.forGrpcTls(address, flightPort)
                        : Location.forGrpcInsecure(address, flightPort);

                    future.complete(location);
                } else {
                    future.completeExceptionally(new IllegalStateException("No Flight info received for node: [" + nodeId + "]"));
                }
            }

            @Override
            public void onFailure(Exception e) {
                future.completeExceptionally(e);
                logger.error("Failed to get Flight server info for node: [{}] {}", nodeId, e);
            }
        });
    }

<<<<<<< HEAD
    private DiscoveryNode getNodeFromCluster(String nodeId) {
        return Objects.requireNonNull(clientConfig.clusterService).state().nodes().get(nodeId);
    }

>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
    private static boolean isValidNode(DiscoveryNode node) {
        return node != null && !node.getVersion().before(MIN_SUPPORTED_VERSION) && FeatureFlags.isEnabled(ARROW_STREAMS_SETTING);
    }

<<<<<<< HEAD
=======
=======
>>>>>>> 7c0193005be (Fix the issue with single node ClientManager)
    private OSFlightClient buildClient(Location location) {
        return OSFlightClient.builder(
            clientConfig.allocator,
            location,
            ServerConfig.clientChannelType(),
            clientConfig.grpcExecutor,
            clientConfig.workerELG,
            clientConfig.sslContextProvider.getClientSslContext()
        ).build();
    }

    private DiscoveryNode getNodeFromClusterState(String nodeId) {
        return Objects.requireNonNull(clientConfig.clusterService).state().nodes().get(nodeId);
    }

    /**
     * Closes the FlightClientManager and all associated Flight clients.
     */
    @Override
    public void close() throws Exception {
        for (ClientHolder clientHolder : flightClients.values()) {
            clientHolder.close();
        }
        flightClients.clear();
        grpcExecutor.shutdown();
    }

    /**
     * Returns the ID of the local node in the cluster.
     *
     * @return String representing the local node ID
     */
    public String getLocalNodeId() {
        return Objects.requireNonNull(clientConfig.clusterService).state().nodes().getLocalNodeId();
    }

    /**
     * Handles cluster state changes by updating node locations and managing client connections.
     *
     * @param event The ClusterChangedEvent containing information about the cluster state change
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (!event.nodesChanged()) {
            return;
        }

        final DiscoveryNodes nodes = event.state().nodes();

        cleanupStaleBuilding();
        removeStaleClients(nodes);
        updateExistingClients(nodes);
    }

    private void removeStaleClients(DiscoveryNodes nodes) {
        flightClients.entrySet().removeIf(entry -> {
            String nodeId = entry.getKey();
            ClientHolder holder = entry.getValue();

            if (!nodes.nodeExists(nodeId)) {
                holder.close(nodeId, "node no longer exists");
                return true;
            }

            if (holder.state == BuildState.BUILDING && holder.isStale()) {
                holder.close(nodeId, "client build state is stale");
                return true;
            }

            return false;
        });
    }

    /**
     * Updates clients for existing nodes based on their validity
     */
    private void updateExistingClients(DiscoveryNodes nodes) {
        for (DiscoveryNode node : nodes) {
            String nodeId = node.getId();

            if (isValidNode(node)) {
                ClientHolder existingHolder = flightClients.get(nodeId);

                if (existingHolder == null) {
                    buildClientAsync(nodeId);
                } else if (existingHolder.state == BuildState.BUILDING && existingHolder.isStale()) {
                    if (flightClients.remove(nodeId, existingHolder)) {
                        existingHolder.close(nodeId, "rebuilding stale client");
                        buildClientAsync(nodeId);
                    }
                }
            } else {
                ClientHolder holder = flightClients.remove(nodeId);
                if (holder != null) {
                    holder.close(nodeId, "node is no longer valid");
                }
            }
        }
    }

    /**
     * Cleans up any clients that are in a stale BUILDING state
     */
    private void cleanupStaleBuilding() {
        flightClients.entrySet().removeIf(entry -> {
            ClientHolder holder = entry.getValue();
            if (holder.state == BuildState.BUILDING && holder.isStale()) {
                holder.close(entry.getKey(), "cleaning up stale building state");
                return true;
            }
            return false;
        });
    }

<<<<<<< HEAD
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
    private Set<String> getCurrentClusterNodes() {
        return Objects.requireNonNull(clientConfig.clusterService).state().nodes().getNodes().keySet();
=======
    private static boolean isValidNode(DiscoveryNode node) {
        return node != null && !node.getVersion().before(MIN_SUPPORTED_VERSION) && FeatureFlags.isEnabled(ARROW_STREAMS_SETTING);
>>>>>>> 2a6590fecdc (Fix concurrency issues in FlightClientManager and FlightStreamManager)
    }

<<<<<<< HEAD
<<<<<<< HEAD
    @VisibleForTesting
    Map<String, FlightClient> getFlightClients() {
        return flightClients;
    }

    private record ClientConfiguration(BufferAllocator allocator, ClusterService clusterService, SslContextProvider sslContextProvider,
        EventLoopGroup workerELG, ExecutorService grpcExecutor) {
        private ClientConfiguration(
            BufferAllocator allocator,
            ClusterService clusterService,
            @Nullable SslContextProvider sslContextProvider,
=======
    private void initializeFlightClients() {
        for (DiscoveryNode node : Objects.requireNonNull(clientConfig.clusterService).state().nodes()) {
            getFlightClient(node.getId());
        }
    }

=======
>>>>>>> 7c0193005be (Fix the issue with single node ClientManager)
    @VisibleForTesting
    Map<String, ClientHolder> getFlightClients() {
        return flightClients;
    }

<<<<<<< HEAD
    private static class ClientConfiguration {
        private final BufferAllocator allocator;
        private final ClusterService clusterService;
        private final SslContextProvider sslContextProvider;
        private final EventLoopGroup workerELG;
        private final ExecutorService grpcExecutor;

        ClientConfiguration(
            BufferAllocator allocator,
            ClusterService clusterService,
            SslContextProvider sslContextProvider,
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
            EventLoopGroup workerELG,
            ExecutorService grpcExecutor
        ) {
            this.allocator = allocator;
            this.clusterService = clusterService;
            this.sslContextProvider = sslContextProvider;
            this.workerELG = workerELG;
            this.grpcExecutor = grpcExecutor;
        }
=======
    private record ClientConfiguration(BufferAllocator allocator, ClusterService clusterService, SslContextProvider sslContextProvider,
        EventLoopGroup workerELG, ExecutorService grpcExecutor) {
>>>>>>> 2a6590fecdc (Fix concurrency issues in FlightClientManager and FlightStreamManager)
    }
<<<<<<< HEAD
<<<<<<< HEAD
=======

    /**
     * Manages the pool of Flight clients
     */
    private static class ClientPool implements AutoCloseable {
        private final Map<String, OSFlightClient> flightClients = new ConcurrentHashMap<>();

        OSFlightClient getOrCreateClient(String nodeId, Function<String, OSFlightClient> clientBuilder) {
            return flightClients.computeIfAbsent(nodeId, clientBuilder);
        }

        void removeStaleClients(Set<String> currentNodes) {
            flightClients.keySet().removeIf(nodeId -> !currentNodes.contains(nodeId));
        }

        Map<String, OSFlightClient> getClients() {
            return flightClients;
        }

        @Override
        public void close() throws Exception {
            for (OSFlightClient flightClient : flightClients.values()) {
                flightClient.close();
            }
            flightClients.clear();
        }
    }
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
=======
>>>>>>> 7c0193005be (Fix the issue with single node ClientManager)
}
