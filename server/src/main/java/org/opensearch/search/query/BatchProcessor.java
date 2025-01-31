/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.search.aggregations.bucket.terms.StringTerms;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class BatchProcessor {
    private final BlockingQueue<List<StringTerms.Bucket>> batchQueue;
    private final List<StringTerms.Bucket> mergedResult;
    private volatile boolean producersComplete;

    public BatchProcessor() {
        this.batchQueue = new LinkedBlockingQueue<>();
        this.mergedResult = new ArrayList<>();
        this.producersComplete = false;

    }

    public BlockingQueue<List<StringTerms.Bucket>> getBatchQueue() {
        return batchQueue;
    }

    public void processBatches() {
        int cnt = 0;
        while (!producersComplete || !batchQueue.isEmpty()) {
            try {
                List<StringTerms.Bucket> currentBatch = batchQueue.poll(100, TimeUnit.MILLISECONDS);
                if (currentBatch != null) {
                    mergeSortedLists(currentBatch);
                    cnt++;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while processing batches", e);
            }
        }
        System.out.println("##Total batches processed: " + cnt);
    }

    public void markProducersComplete() {
        this.producersComplete = true;
    }

    public List<StringTerms.Bucket> getMergedBuckets() {
        return mergedResult;
    }

    private void mergeSortedLists(List<StringTerms.Bucket> newBatch) {
        // System.out.println("##Merging a new batch of size: " + newBatch.size());

        List<StringTerms.Bucket> merged = new ArrayList<>();
        int i = 0;
        int j = 0;

        while (i < mergedResult.size() && j < newBatch.size()) {
            StringTerms.Bucket bucket1 = mergedResult.get(i);
            StringTerms.Bucket bucket2 = newBatch.get(j);

            int comparison = bucket1.compareKey(bucket2);
            if (comparison < 0) {
                merged.add(bucket1);
                i++;
            } else if (comparison > 0) {
                merged.add(bucket2);
                j++;
            } else {
                // merge doc counts
                bucket2.setDocCount(bucket1.getDocCount() + bucket2.getDocCount());
                merged.add(bucket2);
                i++;
                j++;
            }
        }

        while (i < mergedResult.size()) {
            merged.add(mergedResult.get(i++));
        }

        while (j < newBatch.size()) {
            merged.add(newBatch.get(j++));
        }

        mergedResult.clear();
        mergedResult.addAll(merged);
    }

}
