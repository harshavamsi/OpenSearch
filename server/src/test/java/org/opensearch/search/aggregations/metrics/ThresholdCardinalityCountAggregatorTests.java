/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ThresholdCardinalityCountAggregatorTests extends OpenSearchTestCase {

    public void testGroupTrackerBasic() {
        var bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        try (var tracker = new ThresholdCardinalityCountAggregator.GroupTracker(bigArrays, 3)) {
            assertFalse(tracker.addCountOrdAndCheck(0, 10));
            assertFalse(tracker.addCountOrdAndCheck(0, 20));
            assertFalse(tracker.addCountOrdAndCheck(0, 30));
            assertFalse(tracker.hasPassed(0));

            assertTrue(tracker.addCountOrdAndCheck(0, 40)); // 4th distinct → exceeds 3
            tracker.markPassed(0);
            assertTrue(tracker.hasPassed(0));

            // Duplicates don't trigger
            assertFalse(tracker.addCountOrdAndCheck(1, 100));
            assertFalse(tracker.addCountOrdAndCheck(1, 100));
            assertFalse(tracker.addCountOrdAndCheck(1, 100));
        }
    }

    public void testGroupTrackerBorderline() throws IOException {
        var bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        try (var tracker = new ThresholdCardinalityCountAggregator.GroupTracker(bigArrays, 2)) {
            // Group 0: 2 distinct (at threshold)
            tracker.addCountOrdAndCheck(0, 10);
            tracker.addCountOrdAndCheck(0, 20);

            // Group 1: 3 distinct (exceeded)
            tracker.addCountOrdAndCheck(1, 30);
            tracker.addCountOrdAndCheck(1, 40);
            tracker.addCountOrdAndCheck(1, 50);
            tracker.markPassed(1);

            // Borderline should only contain group 0
            final int[] borderlineCount = { 0 };
            tracker.forEachBorderline((groupOrd, bitmap) -> borderlineCount[0]++);
            assertEquals(1, borderlineCount[0]);
        }
    }

    public void testEndToEnd() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                addDoc(w, "ios", "d1", "d1__app1");
                addDoc(w, "ios", "d1", "d1__app2");
                addDoc(w, "ios", "d1", "d1__app3"); // d1: 3 apps → passes threshold=2
                addDoc(w, "ios", "d2", "d2__app1"); // d2: 1 app → borderline
                addDoc(w, "android", "d3", "d3__app1");
                addDoc(w, "android", "d3", "d3__app2");
                addDoc(w, "android", "d3", "d3__app3");
                addDoc(w, "android", "d3", "d3__app4"); // d3: 4 apps → passes
                addDoc(w, "android", "d4", "d4__app1");
                addDoc(w, "android", "d4", "d4__app2"); // d4: 2 apps → borderline
                w.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                var bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
                LeafReaderContext leaf = reader.leaves().get(0);

                try (var tracker = new ThresholdCardinalityCountAggregator.GroupTracker(bigArrays, 2)) {
                    SortedSetDocValues groupOrds = leaf.reader().getSortedSetDocValues("device");
                    SortedSetDocValues countOrds = leaf.reader().getSortedSetDocValues("device_app");

                    for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                        if (groupOrds.advanceExact(doc) == false) continue;
                        if (countOrds.advanceExact(doc) == false) continue;

                        long groupOrd = groupOrds.nextOrd();
                        if (tracker.hasPassed(groupOrd)) continue;

                        int countCount = countOrds.docValueCount();
                        boolean exceeded = false;
                        for (int i = 0; i < countCount; i++) {
                            exceeded = tracker.addCountOrdAndCheck(groupOrd, (int) countOrds.nextOrd());
                            if (exceeded) break;
                        }
                        if (exceeded) {
                            tracker.markPassed(groupOrd);
                        }
                    }

                    // d1 (3 apps) and d3 (4 apps) passed
                    assertEquals(2, tracker.getPassedGroups().getCardinality());

                    // d2 and d4 are borderline
                    final int[] borderlineCount = { 0 };
                    tracker.forEachBorderline((groupOrd, bitmap) -> borderlineCount[0]++);
                    assertEquals(2, borderlineCount[0]);
                }
            }
        }
    }

    public void testReduceCrossShard() {
        var bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        long d1Hash = hashString("d1");
        long d2Hash = hashString("d2");
        long d3Hash = hashString("d3");

        // Shard A: d2 passed, d1 borderline with 2 apps
        HyperLogLogPlusPlus shardAHLL = new HyperLogLogPlusPlus(14, bigArrays, 1);
        shardAHLL.collect(0, d2Hash);
        Map<Long, Set<Long>> shardABorderline = new HashMap<>();
        shardABorderline.put(d1Hash, new HashSet<>(Set.of(hashString("app1"), hashString("app2"))));

        var shardA = new InternalThresholdCardinalityCount("test", shardAHLL, shardABorderline, 3, 14, Collections.emptyMap());

        // Shard B: d3 passed, d1 borderline with 2 more apps
        HyperLogLogPlusPlus shardBHLL = new HyperLogLogPlusPlus(14, bigArrays, 1);
        shardBHLL.collect(0, d3Hash);
        Map<Long, Set<Long>> shardBBorderline = new HashMap<>();
        shardBBorderline.put(d1Hash, new HashSet<>(Set.of(hashString("app3"), hashString("app4"))));

        var shardB = new InternalThresholdCardinalityCount("test", shardBHLL, shardBBorderline, 3, 14, Collections.emptyMap());

        // d1: 4 apps across shards > threshold 3 → resolved
        var result = (InternalThresholdCardinalityCount) shardA.reduce(List.of(shardA, shardB), null);
        assertEquals(3.0, result.value(), 0.0); // d1 + d2 + d3
    }

    public void testReduceBorderlineNotExceeding() {
        var bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        long d1Hash = hashString("d1");

        Map<Long, Set<Long>> shardABorderline = new HashMap<>();
        shardABorderline.put(d1Hash, new HashSet<>(Set.of(hashString("app1"))));
        var shardA = new InternalThresholdCardinalityCount("test", null, shardABorderline, 3, 14, Collections.emptyMap());

        Map<Long, Set<Long>> shardBBorderline = new HashMap<>();
        shardBBorderline.put(d1Hash, new HashSet<>(Set.of(hashString("app1"), hashString("app2"))));
        var shardB = new InternalThresholdCardinalityCount("test", null, shardBBorderline, 3, 14, Collections.emptyMap());

        // d1: 2 distinct apps total ≤ threshold 3
        var result = (InternalThresholdCardinalityCount) shardA.reduce(List.of(shardA, shardB), null);
        assertEquals(0.0, result.value(), 0.0);
    }

    private static long hashString(String s) {
        byte[] bytes = s.getBytes();
        MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
        MurmurHash3.hash128(bytes, 0, bytes.length, 0, hash);
        return hash.h1;
    }

    private void addDoc(IndexWriter w, String platform, String device, String deviceApp) throws IOException {
        Document doc = new Document();
        doc.add(new SortedSetDocValuesField("platform", new BytesRef(platform)));
        doc.add(new SortedSetDocValuesField("device", new BytesRef(device)));
        doc.add(new SortedSetDocValuesField("device_app", new BytesRef(deviceApp)));
        w.addDocument(doc);
    }
}
