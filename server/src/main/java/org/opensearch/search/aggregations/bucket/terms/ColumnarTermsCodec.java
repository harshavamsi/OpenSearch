/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.util.BytesRef;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalOrder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Serialization bridge for Arrow-columnar streaming of terms aggregations.
 *
 * <p>Writes/reads the non-per-bucket state of an {@link InternalMappedTerms} as a
 * standalone blob, plus exposes public constructors on {@link StringTerms} and
 * {@link LongTerms} for rebuilding the final aggregation from columnar bucket lists.
 *
 * <p>This codec lives in the server package because {@link InternalTerms} and
 * {@link InternalMappedTerms} keep most of their state package-private. Having a
 * single, minimal bridge keeps the plugin (arrow-flight-rpc) from needing visibility
 * widenings on the core agg classes.
 *
 * @opensearch.internal
 */
public final class ColumnarTermsCodec {

    private ColumnarTermsCodec() {}

    /**
     * Non-bucket state of an {@link InternalMappedTerms}, in the order it goes over the wire.
     * Mirrors the shape that {@link InternalTerms#doWriteTo} + {@link InternalMappedTerms#writeTermTypeInfoTo}
     * already use, minus the bucket list.
     */
    public static final class TermsHeader {
        public final String name;
        public final Map<String, Object> metadata;
        public final BucketOrder reduceOrder;
        public final BucketOrder order;
        public final int requiredSize;
        public final long minDocCount;
        public final long docCountError;
        public final DocValueFormat format;
        public final int shardSize;
        public final boolean showTermDocCountError;
        public final long otherDocCount;

        public TermsHeader(
            String name,
            Map<String, Object> metadata,
            BucketOrder reduceOrder,
            BucketOrder order,
            int requiredSize,
            long minDocCount,
            long docCountError,
            DocValueFormat format,
            int shardSize,
            boolean showTermDocCountError,
            long otherDocCount
        ) {
            this.name = name;
            this.metadata = metadata;
            this.reduceOrder = reduceOrder;
            this.order = order;
            this.requiredSize = requiredSize;
            this.minDocCount = minDocCount;
            this.docCountError = docCountError;
            this.format = format;
            this.shardSize = shardSize;
            this.showTermDocCountError = showTermDocCountError;
            this.otherDocCount = otherDocCount;
        }
    }

    /**
     * Writes the non-bucket portion of an {@link InternalMappedTerms} to {@code out}.
     * Exactly what {@link InternalTerms#writeTo} and {@link InternalMappedTerms#writeTermTypeInfoTo}
     * emit, minus the trailing bucket list.
     */
    public static void writeTermsHeader(InternalMappedTerms<?, ?> terms, StreamOutput out) throws IOException {
        out.writeString(terms.getName());
        out.writeGenericValue(terms.getMetadata());
        terms.reduceOrder.writeTo(out);
        terms.order.writeTo(out);
        writeSize(terms.bucketCountThresholds.getRequiredSize(), out);
        out.writeVLong(terms.bucketCountThresholds.getMinDocCount());
        out.writeZLong(terms.docCountError);
        out.writeNamedWriteable(terms.format);
        writeSize(terms.shardSize, out);
        out.writeBoolean(terms.showTermDocCountError);
        out.writeVLong(terms.otherDocCount);
    }

    // Local copies of InternalAggregation.readSize / writeSize — those are protected-static
    // and live in a different package. 5 lines each, not worth widening core API.
    private static int readSize(StreamInput in) throws IOException {
        final int size = in.readVInt();
        return size == 0 ? Integer.MAX_VALUE : size;
    }

    private static void writeSize(int size, StreamOutput out) throws IOException {
        if (size == Integer.MAX_VALUE) size = 0;
        out.writeVInt(size);
    }

    /**
     * Reads a {@link TermsHeader} from {@code in} in the shape written by
     * {@link #writeTermsHeader}.
     */
    public static TermsHeader readTermsHeader(StreamInput in) throws IOException {
        String name = in.readString();
        @SuppressWarnings("unchecked")
        Map<String, Object> metadata = (Map<String, Object>) in.readGenericValue();
        BucketOrder reduceOrder = InternalOrder.Streams.readOrder(in);
        BucketOrder order = InternalOrder.Streams.readOrder(in);
        int requiredSize = readSize(in);
        long minDocCount = in.readVLong();
        long docCountError = in.readZLong();
        DocValueFormat format = in.readNamedWriteable(DocValueFormat.class);
        int shardSize = readSize(in);
        boolean showTermDocCountError = in.readBoolean();
        long otherDocCount = in.readVLong();
        return new TermsHeader(
            name,
            metadata,
            reduceOrder,
            order,
            requiredSize,
            minDocCount,
            docCountError,
            format,
            shardSize,
            showTermDocCountError,
            otherDocCount
        );
    }

    /** Build a {@link StringTerms} using the public constructor. */
    public static StringTerms buildStringTerms(TermsHeader h, List<StringTerms.Bucket> buckets) {
        return new StringTerms(
            h.name,
            h.reduceOrder,
            h.order,
            h.metadata,
            h.format,
            h.shardSize,
            h.showTermDocCountError,
            h.otherDocCount,
            buckets,
            h.docCountError,
            new TermsAggregator.BucketCountThresholds(h.minDocCount, 0, h.requiredSize, h.shardSize)
        );
    }

    /** Build a {@link LongTerms} using the public constructor. */
    public static LongTerms buildLongTerms(TermsHeader h, List<LongTerms.Bucket> buckets) {
        return new LongTerms(
            h.name,
            h.reduceOrder,
            h.order,
            h.metadata,
            h.format,
            h.shardSize,
            h.showTermDocCountError,
            h.otherDocCount,
            buckets,
            h.docCountError,
            new TermsAggregator.BucketCountThresholds(h.minDocCount, 0, h.requiredSize, h.shardSize)
        );
    }

    /** Construct a {@link StringTerms.Bucket} from raw fields. */
    public static StringTerms.Bucket buildStringBucket(
        BytesRef term,
        long docCount,
        InternalAggregations aggregations,
        boolean showDocCountError,
        long docCountError,
        DocValueFormat format
    ) {
        return new StringTerms.Bucket(term, docCount, aggregations, showDocCountError, docCountError, format);
    }

    /** Construct a {@link LongTerms.Bucket} from raw fields. */
    public static LongTerms.Bucket buildLongBucket(
        long term,
        long docCount,
        InternalAggregations aggregations,
        boolean showDocCountError,
        long docCountError,
        DocValueFormat format
    ) {
        return new LongTerms.Bucket(term, docCount, aggregations, showDocCountError, docCountError, format);
    }

    /**
     * Multi-terms header bundle. {@link InternalMultiTerms} has a different non-bucket layout
     * than {@link InternalMappedTerms}: it has a list of term formats instead of a single one,
     * plus no {@code minDocCount} (inherited from thresholds), and no per-format doc-count-error
     * treatment.
     */
    public static final class MultiTermsHeader {
        public final String name;
        public final Map<String, Object> metadata;
        public final BucketOrder reduceOrder;
        public final BucketOrder order;
        public final int requiredSize;
        public final long minDocCount;
        public final int shardSize;
        public final boolean showTermDocCountError;
        public final long otherDocCount;
        public final long docCountError;
        public final List<DocValueFormat> termFormats;

        public MultiTermsHeader(
            String name,
            Map<String, Object> metadata,
            BucketOrder reduceOrder,
            BucketOrder order,
            int requiredSize,
            long minDocCount,
            int shardSize,
            boolean showTermDocCountError,
            long otherDocCount,
            long docCountError,
            List<DocValueFormat> termFormats
        ) {
            this.name = name;
            this.metadata = metadata;
            this.reduceOrder = reduceOrder;
            this.order = order;
            this.requiredSize = requiredSize;
            this.minDocCount = minDocCount;
            this.shardSize = shardSize;
            this.showTermDocCountError = showTermDocCountError;
            this.otherDocCount = otherDocCount;
            this.docCountError = docCountError;
            this.termFormats = termFormats;
        }
    }

    public static void writeMultiTermsHeader(InternalMultiTerms terms, StreamOutput out) throws IOException {
        // Direct field access — same package, fields are package-private / protected.
        out.writeString(terms.getName());
        out.writeGenericValue(terms.getMetadata());
        terms.reduceOrder.writeTo(out);
        terms.order.writeTo(out);
        writeSize(terms.requiredSize, out);
        out.writeVLong(terms.minDocCount);
        writeSize(terms.getShardSize(), out);
        out.writeBoolean(terms.getShowTermDocCountError());
        out.writeVLong(terms.getSumOfOtherDocCounts());
        out.writeZLong(terms.getDocCountError());
        out.writeCollection(terms.getTermFormats(), StreamOutput::writeNamedWriteable);
    }

    public static MultiTermsHeader readMultiTermsHeader(StreamInput in) throws IOException {
        String name = in.readString();
        @SuppressWarnings("unchecked")
        Map<String, Object> metadata = (Map<String, Object>) in.readGenericValue();
        BucketOrder reduceOrder = InternalOrder.Streams.readOrder(in);
        BucketOrder order = InternalOrder.Streams.readOrder(in);
        int requiredSize = readSize(in);
        long minDocCount = in.readVLong();
        int shardSize = readSize(in);
        boolean showTermDocCountError = in.readBoolean();
        long otherDocCount = in.readVLong();
        long docCountError = in.readZLong();
        List<DocValueFormat> termFormats = in.readList(stream -> stream.readNamedWriteable(DocValueFormat.class));
        return new MultiTermsHeader(
            name,
            metadata,
            reduceOrder,
            order,
            requiredSize,
            minDocCount,
            shardSize,
            showTermDocCountError,
            otherDocCount,
            docCountError,
            termFormats
        );
    }

    public static InternalMultiTerms buildMultiTerms(MultiTermsHeader h, List<InternalMultiTerms.Bucket> buckets) {
        return new InternalMultiTerms(
            h.name,
            h.reduceOrder,
            h.order,
            h.metadata,
            h.shardSize,
            h.showTermDocCountError,
            h.otherDocCount,
            h.docCountError,
            h.termFormats,
            buckets,
            new TermsAggregator.BucketCountThresholds(h.minDocCount, 0, h.requiredSize, h.shardSize)
        );
    }

    public static InternalMultiTerms.Bucket buildMultiTermsBucket(
        List<Object> termValues,
        long docCount,
        InternalAggregations aggregations,
        boolean showDocCountError,
        long docCountError,
        List<DocValueFormat> termFormats
    ) {
        return new InternalMultiTerms.Bucket(termValues, docCount, aggregations, showDocCountError, docCountError, termFormats);
    }
}
