/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.stream;

import org.apache.arrow.flight.Ticket;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@ExperimentalApi
public class OSTicket extends Ticket implements Writeable, ToXContentFragment {
    public OSTicket(byte[] cmd) {
        super(cmd);
    }

    public OSTicket(StreamInput in) throws IOException {
        super(in.readByteArray());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.value(new String(this.getBytes(), StandardCharsets.UTF_8));
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByteArray(this.getBytes());
    }
}
