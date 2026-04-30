/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.opensearch.OpenSearchWrapperException;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Tests for {@link FlightServerChannel#buildErrorDescription(Throwable)}. The helper unwraps
 * {@link OpenSearchWrapperException}s (which is what ExceptionsHelper.unwrapCause targets) so clients
 * see the root cause in the Flight CallStatus description instead of just the outer wrapper message.
 */
public class FlightServerChannelErrorDescriptionTests extends OpenSearchTestCase {

    /** A minimal test-only wrapper that triggers ExceptionsHelper.unwrapCause. */
    private static class TestWrapper extends RuntimeException implements OpenSearchWrapperException {
        TestWrapper(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    public void testWrapperExceptionExposesRootCause() {
        Throwable root = new IOException("disk full");
        Throwable wrapped = new TestWrapper("Query Failed [Failed to execute main query]", root);
        String desc = FlightServerChannel.buildErrorDescription(wrapped);
        assertEquals("Query Failed [Failed to execute main query] | cause: IOException: disk full", desc);
    }

    public void testNonWrapperExceptionEmitsBaseMessageOnly() {
        // Plain RuntimeException is not an OpenSearchWrapperException, so unwrapCause returns it
        // as-is. We shouldn't append a pointless "| cause: RuntimeException: outer" tail.
        Throwable solo = new RuntimeException("standalone message", new IllegalStateException("inner"));
        String desc = FlightServerChannel.buildErrorDescription(solo);
        assertEquals("standalone message", desc);
    }

    public void testWrapperWithNullRootMessageShowsClassOnly() {
        Throwable root = new IllegalStateException();
        Throwable wrapped = new TestWrapper("outer", root);
        String desc = FlightServerChannel.buildErrorDescription(wrapped);
        assertEquals("outer | cause: IllegalStateException", desc);
    }

    public void testNullOuterMessageFallsBackToStreamErrorLabel() {
        Throwable root = new IOException("bad disk");
        Throwable wrapped = new TestWrapper(null, root);
        String desc = FlightServerChannel.buildErrorDescription(wrapped);
        assertEquals("Stream error | cause: IOException: bad disk", desc);
    }

    public void testTruncatesLongDescriptionAtOneKb() {
        // gRPC status messages cap around 8 KB; we cap at 1 KB to stay well under.
        String longMsg = "x".repeat(5000);
        Throwable err = new RuntimeException(longMsg);
        String desc = FlightServerChannel.buildErrorDescription(err);
        assertEquals(1024, desc.length());
        assertTrue(desc.endsWith("..."));
    }
}
