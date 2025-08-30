package org.greengrapes;

import org.greengrapes.proto.WALEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.greengrapes.Fixtures.buildWALEntry;
import static org.junit.jupiter.api.Assertions.*;

class WALSegmentTest {

    private File tempFile;

    @BeforeEach
    void setup() {
        try {
            tempFile = File.createTempFile("wal", ".log");
        } catch (IOException e) {
            fail("Exception: " + e);
        }
    }

    @AfterEach
    void cleanup() {
        if (tempFile != null && tempFile.exists()) {
            tempFile.delete();
        }
    }

    @Test
    void testWriteAndReadSingleEntry() {
        WALSegment wal = new WALSegment(1, tempFile.getAbsolutePath(), false);

        WALEntry entry = buildWALEntry(1, "hello");
        wal.write(entry);
        wal.close();

        WALSegment reader = new WALSegment(1, tempFile.getAbsolutePath(), false);
        List<WALEntry> entries = reader.readAll();
        reader.close();

        assertEquals(1, entries.size());
        assertEquals("hello", entries.get(0).getData().toStringUtf8());
        assertEquals(1, entries.get(0).getLogSequenceNumber());
    }

    @Test
    void testWriteAndReadMultipleEntries() {
        WALSegment wal = new WALSegment(1, tempFile.getAbsolutePath(), true);

        for (int i = 0; i < 5; i++) {
            wal.write(buildWALEntry(i, "entry-" + i));
        }
        wal.close();

        WALSegment reader = new WALSegment(1, tempFile.getAbsolutePath(), false);
        List<WALEntry> entries = reader.readAll();
        reader.close();

        assertEquals(5, entries.size());
        for (int i = 0; i < 5; i++) {
            assertEquals("entry-" + i, entries.get(i).getData().toStringUtf8());
            assertEquals(i, entries.get(i).getLogSequenceNumber());
        }
    }

    @Test
    void testConcurrentWritesShouldSucceedWithThreadSafety() throws Exception {
        WALSegment wal = new WALSegment(1, tempFile.getAbsolutePath(), false);

        int threadCount = 5;
        int writesPerThread = 50;
        CountDownLatch latch = new CountDownLatch(threadCount);

        var pool = Executors.newFixedThreadPool(threadCount);
        for (int t = 0; t < threadCount; t++) {
            final int id = t;
            pool.submit(() -> {
                try {
                    for (int i = 0; i < writesPerThread; i++) {
                        wal.write(buildWALEntry(id * writesPerThread + i, "t" + id + "-e" + i));
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        wal.close();
        pool.shutdown();

        WALSegment reader = new WALSegment(1, tempFile.getAbsolutePath(), false);
        List<WALEntry> entries = reader.readAll();
        reader.close();

        // Expected = 5 threads * 50 entries = 250
        assertEquals(250, entries.size(), "All entries must be written safely with thread-safety enabled");

        Set<Long> actualLsns = entries.stream()
                .map(WALEntry::getLogSequenceNumber)
                .collect(Collectors.toSet());

        for (int i = 0; i < threadCount * writesPerThread; i++) {
            assertTrue(actualLsns.contains((long) i), "Missing entry with LSN=" + i);
        }
    }
}
