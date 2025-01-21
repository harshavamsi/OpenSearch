/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.bootstrap;

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
import org.apache.arrow.flight.NoOpFlightProducer;
=======
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
=======
import org.apache.arrow.flight.NoOpFlightProducer;
>>>>>>> 34ae62cbeaa (Remove StreamManagerWrapper and Node.java changes from PR)
=======
>>>>>>> 1c6fcc2042e (Flight Producer changes and integration)
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.arrow.flight.bootstrap.tls.DefaultSslContextProvider;
<<<<<<< HEAD
import org.opensearch.arrow.flight.bootstrap.tls.SslContextProvider;
import org.opensearch.arrow.spi.StreamManager;
=======
import org.opensearch.arrow.flight.bootstrap.tls.DisabledSslContextProvider;
import org.opensearch.arrow.flight.bootstrap.tls.SslContextProvider;
import org.opensearch.arrow.flight.impl.BaseFlightProducer;
import org.opensearch.arrow.flight.impl.FlightStreamManager;
import org.opensearch.arrow.spi.StreamManager;
import org.opensearch.client.Client;
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.SecureTransportSettingsProvider;
import org.opensearch.threadpool.ThreadPool;
<<<<<<< HEAD
import org.opensearch.transport.client.Client;

import java.security.AccessController;
import java.security.PrivilegedAction;
<<<<<<< HEAD
=======

>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
=======
>>>>>>> c538c5739c6 (Fix permissions and other runtime issues)
import java.util.Objects;

/**
 * FlightService manages the Arrow Flight server and client for OpenSearch.
 * It handles the initialization, startup, and shutdown of the Flight server and client,
 * as well as managing the stream operations through a FlightStreamManager.
 */
public class FlightService extends NetworkPlugin.AuxTransport {
    private static final Logger logger = LogManager.getLogger(FlightService.class);
    private final ServerComponents serverComponents;
    private StreamManager streamManager;
    private Client client;
    private FlightClientManager clientManager;
    private SecureTransportSettingsProvider secureTransportSettingsProvider;
    private BufferAllocator allocator;
    private ThreadPool threadPool;

    /**
     * Constructor for FlightService.
     * @param settings The settings for the FlightService.
     */
    public FlightService(Settings settings) {
        Objects.requireNonNull(settings, "Settings cannot be null");
        try {
            ServerConfig.init(settings);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Arrow Flight server", e);
        }
        this.serverComponents = new ServerComponents(settings);
    }

    void setClusterService(ClusterService clusterService) {
        serverComponents.setClusterService(Objects.requireNonNull(clusterService, "ClusterService cannot be null"));
    }

    void setNetworkService(NetworkService networkService) {
        serverComponents.setNetworkService(Objects.requireNonNull(networkService, "NetworkService cannot be null"));
    }

    void setThreadPool(ThreadPool threadPool) {
        this.threadPool = Objects.requireNonNull(threadPool, "ThreadPool cannot be null");
        serverComponents.setThreadPool(threadPool);
    }

    void setClient(Client client) {
        this.client = client;
    }

    void setSecureTransportSettingsProvider(SecureTransportSettingsProvider secureTransportSettingsProvider) {
        this.secureTransportSettingsProvider = secureTransportSettingsProvider;
    }

    /**
     * Starts the FlightService by initializing the stream manager.
     */
<<<<<<< HEAD
<<<<<<< HEAD
    @SuppressWarnings("removal")
    @Override
    protected void doStart() {
        try {
            allocator = AccessController.doPrivileged((PrivilegedAction<BufferAllocator>) () -> new RootAllocator(Integer.MAX_VALUE));
            serverComponents.setAllocator(allocator);
            SslContextProvider sslContextProvider = ServerConfig.isSslEnabled()
                ? new DefaultSslContextProvider(secureTransportSettingsProvider)
                : null;
            serverComponents.setSslContextProvider(sslContextProvider);
            serverComponents.initComponents();
=======
=======
    @SuppressWarnings("removal")
>>>>>>> 6f1f435f3b9 (suppress JSM removal warning)
    @Override
    protected void doStart() {
        try {
            allocator = AccessController.doPrivileged((PrivilegedAction<BufferAllocator>) () -> new RootAllocator(Integer.MAX_VALUE));
            serverComponents.setAllocator(allocator);
            SslContextProvider sslContextProvider = ServerConfig.isSslEnabled()
                ? new DefaultSslContextProvider(secureTransportSettingsProvider)
                : new DisabledSslContextProvider();
            serverComponents.setSslContextProvider(sslContextProvider);
            serverComponents.initComponents();
<<<<<<< HEAD
            serverComponents.start();
            initializeStreamManager();
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
=======
>>>>>>> c538c5739c6 (Fix permissions and other runtime issues)
            clientManager = new FlightClientManager(
                allocator, // sharing the same allocator between server and client
                serverComponents.clusterService,
                sslContextProvider,
                serverComponents.workerEventLoopGroup, // sharing the same worker ELG between server and client
                threadPool,
                client
            );
<<<<<<< HEAD
<<<<<<< HEAD
            initializeStreamManager(clientManager);
            serverComponents.setFlightProducer(new BaseFlightProducer(clientManager, (FlightStreamManager) streamManager, allocator));
            serverComponents.start();

=======
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
=======
            initializeStreamManager(clientManager);
            serverComponents.setFlightProducer(new NoOpFlightProducer());
            serverComponents.start();

>>>>>>> c538c5739c6 (Fix permissions and other runtime issues)
        } catch (Exception e) {
            logger.error("Failed to start Flight server", e);
            doClose();
            throw new RuntimeException("Failed to start Flight server", e);
        }
    }

    /**
     * Retrieves the FlightClientManager used by the FlightService.
     * @return The FlightClientManager instance.
     */
    public FlightClientManager getFlightClientManager() {
        return clientManager;
    }

<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> c538c5739c6 (Fix permissions and other runtime issues)
    /**
     * Retrieves the StreamManager used by the FlightService.
     * @return The StreamManager instance.
     */
    public StreamManager getStreamManager() {
<<<<<<< HEAD
=======
    StreamManager getStreamManager() {
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
=======
>>>>>>> c538c5739c6 (Fix permissions and other runtime issues)
        return streamManager;
    }

    /**
     * Retrieves the bound address of the FlightService.
     * @return The BoundTransportAddress instance.
     */
    public BoundTransportAddress getBoundAddress() {
        return serverComponents.getBoundAddress();
    }

    @VisibleForTesting
    SslContextProvider getSslContextProvider() {
        return serverComponents.getSslContextProvider();
    }

    /**
     * Stops the FlightService by closing the server components and network resources.
     */
    @Override
    protected void doStop() {
        try {
            AutoCloseables.close(serverComponents, streamManager, clientManager, allocator);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * doStop() ensures all resources are cleaned up and resources are recreated on
     * doStart()
     */
    @Override
    protected void doClose() {
        doStop();
    }

<<<<<<< HEAD
<<<<<<< HEAD
    private void initializeStreamManager(FlightClientManager clientManager) {
<<<<<<< HEAD
=======
    private void initializeStreamManager() {
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
        streamManager = null;
=======
    private void initializeStreamManager(FlightClientManager clientManager) {
<<<<<<< HEAD
        streamManager = new FlightStreamManager(() -> allocator);
        ((FlightStreamManager) streamManager).setClientManager(clientManager);
>>>>>>> c538c5739c6 (Fix permissions and other runtime issues)
=======
        streamManager = null;
>>>>>>> 34ae62cbeaa (Remove StreamManagerWrapper and Node.java changes from PR)
=======
        streamManager = new FlightStreamManager(() -> allocator);
        ((FlightStreamManager) streamManager).setClientManager(clientManager);
>>>>>>> 1c6fcc2042e (Flight Producer changes and integration)
    }
}
