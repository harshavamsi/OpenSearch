/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.bootstrap.tls;

import org.opensearch.plugins.SecureTransportSettingsProvider;

import javax.net.ssl.SSLException;

import java.util.Locale;

<<<<<<< HEAD
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
=======
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolNames;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import io.grpc.netty.shaded.io.netty.handler.ssl.SupportedCipherSuiteFilter;
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)

/**
 * DefaultSslContextProvider is an implementation of the SslContextProvider interface that provides SSL contexts based on the provided SecureTransportSettingsProvider.
 */
public class DefaultSslContextProvider implements SslContextProvider {

    private final SecureTransportSettingsProvider secureTransportSettingsProvider;

    /**
     * Constructor for DefaultSslContextProvider.
     * @param secureTransportSettingsProvider The SecureTransportSettingsProvider instance.
     */
    public DefaultSslContextProvider(SecureTransportSettingsProvider secureTransportSettingsProvider) {
        this.secureTransportSettingsProvider = secureTransportSettingsProvider;
    }

<<<<<<< HEAD
=======
    /**
     * Returns true to indicate that SSL is enabled.
     * @return true
     */
    @Override
    public boolean isSslEnabled() {
        return true;
    }

>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
    // TODO - handle certificates reload
    /**
     * Creates and returns the server SSL context based on the provided SecureTransportSettingsProvider.
     * @return The server SSL context.
     */
    @Override
    public SslContext getServerSslContext() {
        try {
            SecureTransportSettingsProvider.SecureTransportParameters parameters = secureTransportSettingsProvider.parameters(null).get();
<<<<<<< HEAD
            return SslContextBuilder.forServer(parameters.keyManagerFactory().get())
                .sslProvider(SslProvider.valueOf(parameters.sslProvider().get().toUpperCase(Locale.ROOT)))
                .clientAuth(ClientAuth.valueOf(parameters.clientAuth().get().toUpperCase(Locale.ROOT)))
=======
            return io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder.forServer(parameters.keyManagerFactory())
                .sslProvider(SslProvider.valueOf(parameters.sslProvider().toUpperCase(Locale.ROOT)))
                .clientAuth(ClientAuth.valueOf(parameters.clientAuth().toUpperCase(Locale.ROOT)))
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
                .protocols(parameters.protocols())
                .ciphers(parameters.cipherSuites(), SupportedCipherSuiteFilter.INSTANCE)
                .sessionCacheSize(0)
                .sessionTimeout(0)
                .applicationProtocolConfig(
                    new ApplicationProtocolConfig(
                        ApplicationProtocolConfig.Protocol.ALPN,
                        // NO_ADVERTISE is currently the only mode supported by both OpenSsl and JDK providers.
                        ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                        // ACCEPT is currently the only mode supported by both OpenSsl and JDK providers.
                        ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                        ApplicationProtocolNames.HTTP_2,
                        ApplicationProtocolNames.HTTP_1_1
                    )
                )
<<<<<<< HEAD
                .trustManager(parameters.trustManagerFactory().get())
=======
                .trustManager(parameters.trustManagerFactory())
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
                .build();
        } catch (SSLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
    * Returns the client SSL context based on the provided SecureTransportSettingsProvider.
    * @return The client SSL context.
    */
    @Override
    public SslContext getClientSslContext() {
        try {
            SecureTransportSettingsProvider.SecureTransportParameters parameters = secureTransportSettingsProvider.parameters(null).get();
<<<<<<< HEAD
            return SslContextBuilder.forClient()
                .sslProvider(SslProvider.valueOf(parameters.sslProvider().get().toUpperCase(Locale.ROOT)))
=======
            return io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder.forClient()
                .sslProvider(SslProvider.valueOf(parameters.sslProvider().toUpperCase(Locale.ROOT)))
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
                .protocols(parameters.protocols())
                .ciphers(parameters.cipherSuites(), SupportedCipherSuiteFilter.INSTANCE)
                .applicationProtocolConfig(
                    new ApplicationProtocolConfig(
                        ApplicationProtocolConfig.Protocol.ALPN,
                        ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                        ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                        ApplicationProtocolNames.HTTP_2,
                        ApplicationProtocolNames.HTTP_1_1
                    )
                )
                .sessionCacheSize(0)
                .sessionTimeout(0)
<<<<<<< HEAD
                .keyManager(parameters.keyManagerFactory().get())
                .trustManager(parameters.trustManagerFactory().get())
=======
                .keyManager(parameters.keyManagerFactory())
                .trustManager(parameters.trustManagerFactory())
>>>>>>> be77c688f30 (Move arrow-flight-rpc from module to plugin)
                .build();
        } catch (SSLException e) {
            throw new RuntimeException(e);
        }
    }
}
