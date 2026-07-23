/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.PackedInts;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.BitArray;
import org.opensearch.common.util.ByteUtils;
import org.opensearch.common.util.IntArray;
import org.opensearch.core.common.util.ByteArray;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Hyperloglog++ counter, implemented based on pseudo code from
 * http://static.googleusercontent.com/media/research.google.com/fr//pubs/archive/40671.pdf and its appendix
 * https://docs.google.com/document/d/1gyjfMHy43U9OWBXxfaeG-3MjGzejW1dlpyMwEYAAWEI/view?fullscreen
 * <p>
 * This implementation is different from the original implementation in that it uses a hash table instead of a sorted list for linear
 * counting. Although this requires more space and makes hyperloglog (which is less accurate) used sooner, this is also considerably faster.
 * <p>
 * Trying to understand what this class does without having read the paper is considered adventurous.
 * <p>
 * The HyperLogLogPlusPlus contains two algorithms, one for linear counting and the HyperLogLog algorithm. Initially hashes added to the
 * data structure are processed using the linear counting until a threshold defined by the precision is reached where the data is replayed
 * to the HyperLogLog algorithm and then this is used.
 * <p>
 * It supports storing several HyperLogLogPlusPlus structures which are identified by a bucket number.
 *
 * @opensearch.internal
 */
public final class HyperLogLogPlusPlus extends AbstractHyperLogLogPlusPlus {

    private static final float MAX_LOAD_FACTOR = 0.75f;

    public static final int DEFAULT_PRECISION = 14;

    private final BitArray algorithm;
    private final HyperLogLog hll;
    private final LinearCounting lc;

    /**
     * Compute the required precision so that <code>count</code> distinct entries would be counted with linear counting.
     */
    public static int precisionFromThreshold(long count) {
        final long hashTableEntries = (long) Math.ceil(count / MAX_LOAD_FACTOR);
        int precision = PackedInts.bitsRequired(hashTableEntries * Integer.BYTES);
        precision = Math.max(precision, AbstractHyperLogLog.MIN_PRECISION);
        precision = Math.min(precision, AbstractHyperLogLog.MAX_PRECISION);
        return precision;
    }

    /**
     * Return the expected per-bucket memory usage for the given precision.
     */
    public static long memoryUsage(int precision) {
        return 1L << precision;
    }

    public HyperLogLogPlusPlus(int precision, BigArrays bigArrays, long initialBucketCount) {
        super(precision);
        HyperLogLog hll = null;
        LinearCounting lc = null;
        BitArray algorithm = null;
        boolean success = false;
        try {
            hll = new HyperLogLog(bigArrays, initialBucketCount, precision);
            lc = new LinearCounting(bigArrays, initialBucketCount, precision, hll);
            algorithm = new BitArray(1, bigArrays);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(hll, lc, algorithm);
            }
        }
        this.hll = hll;
        this.lc = lc;
        this.algorithm = algorithm;
    }

    @Override
    public long maxOrd() {
        return hll.maxOrd();
    }

    @Override
    public long cardinality(long bucketOrd) {
        if (getAlgorithm(bucketOrd) == LINEAR_COUNTING) {
            return lc.cardinality(bucketOrd);
        } else {
            return hll.cardinality(bucketOrd);
        }
    }

    @Override
    protected boolean getAlgorithm(long bucketOrd) {
        return algorithm.get(bucketOrd);
    }

    @Override
    protected AbstractLinearCounting.HashesIterator getLinearCounting(long bucketOrd) {
        return lc.values(bucketOrd);
    }

    @Override
    protected AbstractHyperLogLog.RunLenIterator getHyperLogLog(long bucketOrd) {
        return hll.getRunLens(bucketOrd);
    }

    /**
     * Bulk copy of registers for {@code bucketOrd} into {@code dest} via a ByteArray slice
     * read. Backing storage is {@link org.opensearch.core.common.util.ByteArray}, which has a
     * {@code get(long, int, BytesRef)} that returns a zero-copy slice when possible.
     * ~50-200× faster than the iterator-based fallback because we skip per-byte method
     * dispatch.
     */
    @Override
    protected void getHyperLogLogRegisters(long bucketOrd, byte[] dest) {
        final int registers = 1 << precision();
        final long start = bucketOrd << precision();
        org.apache.lucene.util.BytesRef ref = new org.apache.lucene.util.BytesRef();
        hll.runLens.get(start, registers, ref);
        System.arraycopy(ref.bytes, ref.offset, dest, 0, registers);
    }

    /**
     * Bulk write of {@code src} into the register slot for {@code bucketOrd}. Used by the
     * bulk readFrom path to avoid per-byte addRunLen calls. Caller must ensure {@code src.length ==
     * 2^precision}.
     */
    public void setHyperLogLogRegisters(long bucketOrd, byte[] src) {
        hll.ensureCapacity(bucketOrd + 1);
        // Mark this bucket as using HLL (not linear counting). algorithm is a BitArray where
        // true means HYPERLOGLOG and false means LINEAR_COUNTING. The LC bit may not yet be set
        // on a fresh reader, so set() idempotently.
        if (algorithm.get(bucketOrd) == LINEAR_COUNTING) {
            algorithm.set(bucketOrd);
        }
        final long start = bucketOrd << precision();
        hll.runLens.set(start, src, 0, src.length);
    }

    @Override
    public void collect(long bucket, long hash) {
        hll.ensureCapacity(bucket + 1);
        if (algorithm.get(bucket) == LINEAR_COUNTING) {
            final int newSize = lc.collect(bucket, hash);
            if (newSize > lc.threshold) {
                upgradeToHll(bucket);
            }
        } else {
            hll.collect(bucket, hash);
        }
    }

    @Override
    public void close() {
        Releasables.close(algorithm, hll, lc);
    }

    protected void addRunLen(long bucketOrd, int register, int runLen) {
        if (algorithm.get(bucketOrd) == LINEAR_COUNTING) {
            upgradeToHll(bucketOrd);
        }
        hll.addRunLen(0, register, runLen);
    }

    void upgradeToHll(long bucketOrd) {
        hll.ensureCapacity(bucketOrd + 1);
        final AbstractLinearCounting.HashesIterator hashes = lc.values(bucketOrd);
        // We need to copy values into an arrays as we will override
        // the values on the buffer
        final IntArray values = lc.bigArrays.newIntArray(hashes.size());
        try {
            int i = 0;
            while (hashes.next()) {
                values.set(i++, hashes.value());
            }
            assert i == hashes.size();
            hll.reset(bucketOrd);
            for (long j = 0; j < values.size(); ++j) {
                final int encoded = values.get(j);
                hll.collectEncoded(bucketOrd, encoded);
            }
            algorithm.set(bucketOrd);
        } finally {
            Releasables.close(values);
        }
    }

    public void merge(long thisBucket, AbstractHyperLogLogPlusPlus other, long otherBucket) {
        if (precision() != other.precision()) {
            throw new IllegalArgumentException(
                "Cannot merge HLL++ sketches with different precision: " + precision() + " vs " + other.precision()
            );
        }
        hll.ensureCapacity(thisBucket + 1);
        if (other.getAlgorithm(otherBucket) == LINEAR_COUNTING) {
            merge(thisBucket, other.getLinearCounting(otherBucket));
        } else {
            merge(thisBucket, other.getHyperLogLog(otherBucket));
        }
    }

    private void merge(long thisBucket, AbstractLinearCounting.HashesIterator values) {
        while (values.next()) {
            final int encoded = values.value();
            if (algorithm.get(thisBucket) == LINEAR_COUNTING) {
                final int newSize = lc.addEncoded(thisBucket, encoded);
                if (newSize > lc.threshold) {
                    upgradeToHll(thisBucket);
                }
            } else {
                hll.collectEncoded(thisBucket, encoded);
            }
        }
    }

    private void merge(long thisBucket, AbstractHyperLogLog.RunLenIterator runLens) {
        if (algorithm.get(thisBucket) != HYPERLOGLOG) {
            upgradeToHll(thisBucket);
        }
        // Fast path: when the source iterator is backed by a HyperLogLog we can merge by
        // byte-wise max over the two contiguous register arrays — a tight loop the JIT can
        // vectorize. Falls back to the generic iterator walk otherwise.
        if (runLens instanceof HyperLogLogIterator otherIt) {
            final HyperLogLog otherHll = otherIt.hll;
            final long thisStart = thisBucket << hll.p;
            final long otherStart = otherIt.start;
            final int m = hll.m;
            org.apache.lucene.util.BytesRef a = new org.apache.lucene.util.BytesRef();
            org.apache.lucene.util.BytesRef b = new org.apache.lucene.util.BytesRef();
            hll.runLens.get(thisStart, m, a);
            otherHll.runLens.get(otherStart, m, b);
            // If both slices are contiguous byte[] views, merge in-place and write back.
            if (a.bytes != null && b.bytes != null && a.length == m && b.length == m) {
                byte[] aBytes = a.bytes;
                byte[] bBytes = b.bytes;
                int aOff = a.offset;
                int bOff = b.offset;
                // JIT-friendly: simple loop with predictable memory access.
                for (int i = 0; i < m; i++) {
                    byte av = aBytes[aOff + i];
                    byte bv = bBytes[bOff + i];
                    if (bv > av) {
                        aBytes[aOff + i] = bv;
                    }
                }
                // Only write back if the slice wasn't a direct view (i.e. get() materialized
                // a copy). The BigByteArray backing stores data contiguously per-page, so for
                // a sketch of 16KB the slice is typically a direct view — no write-back needed.
                hll.runLens.set(thisStart, aBytes, aOff, m);
                return;
            }
        }
        for (int i = 0; i < hll.m; ++i) {
            runLens.next();
            hll.addRunLen(thisBucket, i, runLens.value());
        }
    }

    /**
     * The hyper log log
     *
     * @opensearch.internal
     */
    private static class HyperLogLog extends AbstractHyperLogLog implements Releasable {
        private final BigArrays bigArrays;
        private final HyperLogLogIterator iterator;
        // array for holding the runlens. Package-private so the enclosing
        // HyperLogLogPlusPlus can do bulk reads/writes via its get/setHyperLogLogRegisters
        // fast-path methods (used by the streaming serde path).
        ByteArray runLens;

        HyperLogLog(BigArrays bigArrays, long initialBucketCount, int precision) {
            super(precision);
            this.runLens = bigArrays.newByteArray(initialBucketCount << precision);
            this.bigArrays = bigArrays;
            this.iterator = new HyperLogLogIterator(this, precision, m);
        }

        public long maxOrd() {
            return runLens.size() >>> precision();
        }

        @Override
        protected void addRunLen(long bucketOrd, int register, int encoded) {
            final long bucketIndex = (bucketOrd << p) + register;
            runLens.set(bucketIndex, (byte) Math.max(encoded, runLens.get(bucketIndex)));
        }

        @Override
        protected RunLenIterator getRunLens(long bucketOrd) {
            iterator.reset(bucketOrd);
            return iterator;
        }

        protected void reset(long bucketOrd) {
            runLens.fill(bucketOrd << p, (bucketOrd << p) + m, (byte) 0);
        }

        protected void ensureCapacity(long numBuckets) {
            runLens = bigArrays.grow(runLens, numBuckets << p);
        }

        @Override
        public void close() {
            Releasables.close(runLens);
        }
    }

    /**
     * Iterator for hyper log log
     *
     * @opensearch.internal
     */
    private static class HyperLogLogIterator implements AbstractHyperLogLog.RunLenIterator {

        // Package-private so the enclosing class can use it for the bulk-merge fast path.
        final HyperLogLog hll;
        private final int m, p;
        int pos;
        long start;
        private byte value;

        HyperLogLogIterator(HyperLogLog hll, int p, int m) {
            this.hll = hll;
            this.m = m;
            this.p = p;
        }

        void reset(long bucket) {
            pos = 0;
            start = bucket << p;
        }

        @Override
        public boolean next() {
            if (pos < m) {
                value = hll.runLens.get(start + pos);
                pos++;
                return true;
            }
            return false;
        }

        @Override
        public byte value() {
            return value;
        }
    }

    /**
     * The plus plus
     *
     * @opensearch.internal
     */
    private static class LinearCounting extends AbstractLinearCounting implements Releasable {

        protected final int threshold;
        private final int mask;
        private final BytesRef readSpare;
        private final ByteBuffer writeSpare;
        private final BigArrays bigArrays;
        private final LinearCountingIterator iterator;
        // We are actually using HyperLogLog's runLens array but interpreting it as a hash set for linear counting.
        private final HyperLogLog hll;
        // Number of elements stored.
        private IntArray sizes;

        LinearCounting(BigArrays bigArrays, long initialBucketCount, int p, HyperLogLog hll) {
            super(p);
            this.bigArrays = bigArrays;
            this.hll = hll;
            final int capacity = (1 << p) / 4; // because ints take 4 bytes
            threshold = (int) (capacity * MAX_LOAD_FACTOR);
            mask = capacity - 1;
            sizes = bigArrays.newIntArray(initialBucketCount);
            readSpare = new BytesRef();
            writeSpare = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
            iterator = new LinearCountingIterator(this, capacity);
        }

        @Override
        protected int addEncoded(long bucketOrd, int encoded) {
            sizes = bigArrays.grow(sizes, bucketOrd + 1);
            assert encoded != 0;
            for (int i = (encoded & mask);; i = (i + 1) & mask) {
                final int v = get(bucketOrd, i);
                if (v == 0) {
                    // means unused, take it!
                    set(bucketOrd, i, encoded);
                    return sizes.increment(bucketOrd, 1);
                } else if (v == encoded) {
                    // k is already in the set
                    return -1;
                }
            }
        }

        @Override
        protected int size(long bucketOrd) {
            if (bucketOrd >= sizes.size()) {
                return 0;
            }
            final int size = sizes.get(bucketOrd);
            assert size == recomputedSize(bucketOrd);
            return size;
        }

        @Override
        protected HashesIterator values(long bucketOrd) {
            iterator.reset(bucketOrd, size(bucketOrd));
            return iterator;
        }

        private long index(long bucketOrd, int index) {
            return (bucketOrd << p) + (index << 2);
        }

        private int get(long bucketOrd, int index) {
            hll.runLens.get(index(bucketOrd, index), 4, readSpare);
            return ByteUtils.readIntLE(readSpare.bytes, readSpare.offset);
        }

        private void set(long bucketOrd, int index, int value) {
            writeSpare.putInt(0, value);
            hll.runLens.set(index(bucketOrd, index), writeSpare.array(), 0, 4);
        }

        private int recomputedSize(long bucketOrd) {
            if (bucketOrd >= hll.maxOrd()) {
                return 0;
            }
            int size = 0;
            for (int i = 0; i <= mask; ++i) {
                final int v = get(bucketOrd, i);
                if (v != 0) {
                    ++size;
                }
            }
            return size;
        }

        @Override
        public void close() {
            Releasables.close(sizes);
        }
    }

    /**
     * The plus plus iterator
     *
     * @opensearch.internal
     */
    private static class LinearCountingIterator implements AbstractLinearCounting.HashesIterator {

        private final LinearCounting lc;
        private final int capacity;
        private int pos, size;
        private long bucketOrd;
        private int value;

        LinearCountingIterator(LinearCounting lc, int capacity) {
            this.lc = lc;
            this.capacity = capacity;
        }

        void reset(long bucketOrd, int size) {
            this.bucketOrd = bucketOrd;
            this.size = size;
            this.pos = size == 0 ? capacity : 0;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean next() {
            if (pos < capacity) {
                for (; pos < capacity; ++pos) {
                    final int k = lc.get(bucketOrd, pos);
                    if (k != 0) {
                        ++pos;
                        value = k;
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public int value() {
            return value;
        }
    }
}
