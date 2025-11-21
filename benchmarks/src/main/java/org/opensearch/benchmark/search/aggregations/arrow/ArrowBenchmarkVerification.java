/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.search.aggregations.arrow;

/**
 * Simple verification runner to test that all reduce methods produce identical results.
 */
public class ArrowBenchmarkVerification {
    public static void main(String[] args) throws Exception {
        System.out.println("Starting ArrowBenchmark Verification...\n");

        ArrowBenchmark benchmark = new ArrowBenchmark();

        // Use reflection to set private fields (matching the benchmark parameters)
        java.lang.reflect.Field totalDocsField = ArrowBenchmark.class.getDeclaredField("totalDocuments");
        totalDocsField.setAccessible(true);
        totalDocsField.set(benchmark, 10000000);

        java.lang.reflect.Field uniqueTermsField = ArrowBenchmark.class.getDeclaredField("uniqueTerms");
        uniqueTermsField.setAccessible(true);
        uniqueTermsField.set(benchmark, 100000);

        java.lang.reflect.Field numShardsField = ArrowBenchmark.class.getDeclaredField("numShards");
        numShardsField.setAccessible(true);
        numShardsField.set(benchmark, 10);

        java.lang.reflect.Field avgKeyLengthField = ArrowBenchmark.class.getDeclaredField("avgKeyLength");
        avgKeyLengthField.setAccessible(true);
        avgKeyLengthField.set(benchmark, 100);

        try {
            // Setup
            System.out.println("Setting up benchmark data...");
            benchmark.setup();
            System.out.println("Setup complete.\n");

            // Run verification
            benchmark.verifyResults();

            // Teardown
            System.out.println("Cleaning up...");
            benchmark.tearDown();
            System.out.println("Done!");

        } catch (Exception e) {
            System.err.println("Verification failed with error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
