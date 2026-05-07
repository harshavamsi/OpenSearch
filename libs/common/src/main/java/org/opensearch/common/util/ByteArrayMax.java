/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.common.annotation.InternalApi;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorSpecies;

/**
 * Vectorized elementwise byte array max.
 *
 * <p>Used by {@code HyperLogLogPlusPlus} to merge register arrays (2^precision bytes, default
 * 16 KB). HLL register values are unsigned in [0, 64], so signed SIMD max produces the same
 * result as unsigned max.
 *
 * <p>On aarch64/Graviton 3, {@link ByteVector#SPECIES_PREFERRED} selects SVE 256-bit or NEON
 * 128-bit (16-32 lanes per op). On x86 with AVX-512 it's 64 lanes. Scalar tail handles the
 * remainder; at precision 14 (m=16384) the tail is 0 for any preferred lane count that
 * divides 16384.
 *
 * @opensearch.internal
 */
@InternalApi
public final class ByteArrayMax {

    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_PREFERRED;
    private static final int LANES = BYTE_SPECIES.length();

    private ByteArrayMax() {}

    /**
     * In-place elementwise {@code a[aOff + i] = max(a[aOff + i], b[bOff + i])} for {@code i}
     * in {@code [0, length)}.
     *
     * @param a     destination array (modified in place)
     * @param aOff  offset into {@code a}
     * @param b     source array to take max against (unmodified)
     * @param bOff  offset into {@code b}
     * @param length number of bytes to process
     */
    public static void maxInto(byte[] a, int aOff, byte[] b, int bOff, int length) {
        final int bound = length - (length % LANES);
        int i = 0;
        for (; i < bound; i += LANES) {
            ByteVector va = ByteVector.fromArray(BYTE_SPECIES, a, aOff + i);
            ByteVector vb = ByteVector.fromArray(BYTE_SPECIES, b, bOff + i);
            va.max(vb).intoArray(a, aOff + i);
        }
        for (; i < length; i++) {
            byte av = a[aOff + i];
            byte bv = b[bOff + i];
            if (bv > av) {
                a[aOff + i] = bv;
            }
        }
    }
}
