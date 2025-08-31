package org.greengrapes;

import org.greengrapes.proto.WALEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.greengrapes.Fixtures.buildWALEntry;
import static org.junit.jupiter.api.Assertions.*;

class WALSegmentTest {

    private File tempFile;
    private Checksum checksum;

    @BeforeEach
    void setup() {
        checksum = new Checksum();
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
        WALSegment wal = new WALSegment(1, tempFile.getAbsolutePath(), false, checksum);

        WALEntry entry = buildWALEntry(1, "hello");
        wal.write(entry);
        wal.close();

        WALSegment reader = new WALSegment(1, tempFile.getAbsolutePath(), false, checksum);
        List<WALEntry> entries = reader.readAll();
        reader.close();

        assertEquals(1, entries.size());
        assertEquals("hello", entries.get(0).getData().toStringUtf8());
        assertEquals(1, entries.get(0).getLogSequenceNumber());
    }

    @Test
    void testWriteAndReadMultipleEntries() {
        WALSegment wal = new WALSegment(1, tempFile.getAbsolutePath(), true, checksum);

        for (int i = 0; i < 5; i++) {
            wal.write(buildWALEntry(i, "entry-" + i));
        }
        wal.close();

        WALSegment reader = new WALSegment(1, tempFile.getAbsolutePath(), false, checksum);
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
        WALSegment wal = new WALSegment(1, tempFile.getAbsolutePath(), false, checksum);

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

        WALSegment reader = new WALSegment(1, tempFile.getAbsolutePath(), false, checksum);
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

    @Test
    void testSizeReflectsBufferedWrites() {
        WALSegment wal = new WALSegment(1, tempFile.getAbsolutePath(), false, checksum);

        long before = wal.size();
        wal.write(buildWALEntry(1, "abc"));
        long after = wal.size();

        assertTrue(after > before, "size() should increase after write");
        wal.close();
    }

    @Test
    void testReadLastEntry() throws Exception {
        WALSegment wal = new WALSegment(1, tempFile.getAbsolutePath(), true, checksum);

        wal.write(buildWALEntry(1, "first"));
        wal.write(buildWALEntry(2, "second"));
        wal.write(buildWALEntry(3, "last"));
        wal.close();

        WALSegment reader = new WALSegment(1, tempFile.getAbsolutePath(), false, checksum);
        WALEntry last = reader.readLastEntry();
        reader.close();

        assertNotNull(last);
        assertEquals(3, last.getLogSequenceNumber());
        assertEquals("last", last.getData().toStringUtf8());
    }

    @Test
    void testReadAllFailsOnSegmentCorruption() throws Exception {
        WALSegment wal = new WALSegment(1, tempFile.getAbsolutePath(), true, checksum);

        wal.write(buildWALEntry(1, "valid"));
        wal.close();

        // Corrupt the file manually
        try (FileOutputStream out = new FileOutputStream(tempFile, true)) {
            out.write(new byte[]{99, 100, 101, 102}); // random junk
        }

        WALSegment reader = new WALSegment(1, tempFile.getAbsolutePath(), false, checksum);

        assertThrows(RuntimeException.class, reader::readAll);

        reader.close();
    }

    @Test
    void testChecksumFailureOnCorruptedEntry() throws Exception {
        WALSegment wal = new WALSegment(1, tempFile.toString(), true, checksum);

        wal.write(buildWALEntry(1, "hellp"));
        wal.close();

        // Corrupt the file (flip one byte in the data or CRC)
        RandomAccessFile raf = new RandomAccessFile(tempFile.toString(), "rw");
        raf.seek(raf.length() - 2); // last 2 bytes, inside CRC or data
        raf.writeByte(raf.readByte() ^ 0xFF); // flip a bit
        raf.close();

        WALSegment wal2 = new WALSegment(1, tempFile.toString(), true, checksum);
        assertThrows(IllegalStateException.class, wal2::readAll);
    }
}
