package org.greengrapes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class WALTest {

    private Path tempDir;

    @BeforeEach
    void setup() throws Exception {
        tempDir = Files.createTempDirectory("wal_test");
    }

    @AfterEach
    void cleanup() throws Exception {
        if (Files.exists(tempDir)) {
            Files.walk(tempDir)
                    .map(Path::toFile)
                    .forEach(f -> f.delete());
        }
    }

    @Test
    void testWriteAndReadSingleEntry() {
        WAL wal = WAL.openWAL(tempDir.toString(), false, 1024 * 1024, 10);

        byte[] data = "hello world".getBytes();
        wal.write(data);

        List<WALRecord> records = wal.read();
        assertEquals(1, records.size());
        assertEquals(1, records.get(0).getLogSequenceNumber());
        assertArrayEquals(data, records.get(0).getData());

        wal.close();
    }

    @Test
    void testWriteMultipleEntriesAndRead() {
        WAL wal = WAL.openWAL(tempDir.toString(), false, 1024 * 1024, 10);

        byte[] d1 = "first".getBytes();
        byte[] d2 = "second".getBytes();

        wal.write(d1);
        wal.write(d2);

        List<WALRecord> records = wal.read();
        assertEquals(2, records.size());

        assertEquals(1, records.get(0).getLogSequenceNumber());
        assertArrayEquals(d1, records.get(0).getData());

        assertEquals(2, records.get(1).getLogSequenceNumber());
        assertArrayEquals(d2, records.get(1).getData());

        wal.close();
    }

    @Test
    void testSegmentRotation() {
        // set very small maxSegmentSize to force rotation
        WAL wal = WAL.openWAL(tempDir.toString(), false, 50, 10);

        byte[] data1 = "1234567890".getBytes();
        byte[] data2 = "abcdefghij".getBytes();

        wal.write(data1);
        wal.write(data2); // should trigger rotation

        // The current segment should not be the first one
        assertTrue(wal.read().size() > 0);

        wal.close();
    }

    @Test
    void testFlushAndCommit() {
        WAL wal = WAL.openWAL(tempDir.toString(), false, 1024 * 1024, 10);

        byte[] data = "flush-test".getBytes();
        wal.write(data);

        // flush should not throw
        assertDoesNotThrow(wal::flush);

        // commit should not throw
        assertDoesNotThrow(wal::commit);

        wal.close();
    }

    @Test
    void testCloseMultipleTimes() {
        WAL wal = WAL.openWAL(tempDir.toString(), false, 1024 * 1024, 10);

        byte[] data = "close-test".getBytes();
        wal.write(data);

        // closing multiple times should not throw
        wal.close();
        assertDoesNotThrow(wal::close);
    }

    @Test
    void testConcurrentWritesWithRotation() throws InterruptedException, ExecutionException {
        final WAL wal = WAL.openWAL(tempDir.toString(), false, 100, 50); // small segment size to force rotation
        ExecutorService executor = Executors.newFixedThreadPool(2);

        int numWritesPerThread = 20;
        Callable<Void> writerTask = () -> {
            for (int i = 0; i < numWritesPerThread; i++) {
                wal.write(("data-" + Thread.currentThread().getId() + "-" + i).getBytes());
            }
            return null;
        };

        Future<Void> f1 = executor.submit(writerTask);
        Future<Void> f2 = executor.submit(writerTask);

        f1.get();
        f2.get();

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        wal.commit();

        // Read all records and ensure total count is correct
        List<WALRecord> records = wal.readAll();
        assertEquals(numWritesPerThread * 2, records.size());

        // Optional: verify each entry is non-null
        records.forEach(r -> assertNotNull(r.getData()));

        wal.close();
    }

    @Test
    void testConcurrentWritesWithRotationAndDeletion() throws InterruptedException, IOException {
        final int maxSegmentSize = 50; // very small to force multiple rotations
        final int maxSegments = 3;     // old segments will be deleted
        final WAL wal = WAL.openWAL(tempDir.toString(), false, maxSegmentSize, maxSegments);

        ExecutorService executor = Executors.newFixedThreadPool(3);
        int numWritesPerThread = 10;

        CountDownLatch latch = new CountDownLatch(3);
        for (int i = 0; i < 3; i++) {
            executor.submit(() -> {
                for (int j = 0; j < numWritesPerThread; j++) {
                    wal.write(("data-" + Thread.currentThread().getId() + "-" + j).getBytes());
                }

                latch.countDown();
            });
        }

        // Wait for all threads
        latch.await();
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        wal.commit();

        // Verify segment files (no more than maxSegments should exist)
        long segmentFiles = Files.list(tempDir)
                .filter(f -> f.getFileName().toString().startsWith("wal_segment_"))
                .count();
        assertTrue(segmentFiles <= maxSegments, "Segment files exceeded maxSegments");

        wal.close();
    }
}
