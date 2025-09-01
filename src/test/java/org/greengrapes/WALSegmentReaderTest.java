package org.greengrapes;

import com.google.protobuf.ByteString;
import org.greengrapes.proto.WALEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WALSegmentReaderTest {

    @TempDir
    Path tempDir;

    Checksum checksum = new Checksum();

    private void writeEntry(DataOutputStream out, WALEntry entry) throws Exception {
        byte[] data = entry.toByteArray();

        // Write size prefix (little endian)
        byte[] sizeBytes = ByteBuffer.allocate(4)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(data.length)
                .array();
        out.write(sizeBytes);

        // Write serialized entry
        out.write(data);
    }

    @Test
    void testReadAllEntries() throws Exception {
        Path walFile = tempDir.resolve("test.wal");

        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(walFile.toFile()))) {
            WALEntry entry1 = WALEntry.newBuilder()
                    .setLogSequenceNumber(1)
                    .setData(ByteString.copyFromUtf8("first"))
                    .setCRC(checksum.compute(1, "first".getBytes()))
                    .build();

            WALEntry entry2 = WALEntry.newBuilder()
                    .setLogSequenceNumber(2)
                    .setData(ByteString.copyFromUtf8("second"))
                    .setCRC(checksum.compute(2, "second".getBytes()))
                    .build();

            writeEntry(out, entry1);
            writeEntry(out, entry2);
        }

        WALSegmentReader reader = new WALSegmentReader(walFile.toString(), checksum);
        List<WALEntry> entries = reader.readAll();

        assertEquals(2, entries.size());
        assertEquals("first", entries.get(0).getData().toStringUtf8());
        assertEquals("second", entries.get(1).getData().toStringUtf8());
    }

    @Test
    void testReadLastEntry() throws Exception {
        Path walFile = tempDir.resolve("lastEntry.wal");

        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(walFile.toFile()))) {
            WALEntry entry1 = WALEntry.newBuilder()
                    .setLogSequenceNumber(1)
                    .setData(ByteString.copyFromUtf8("alpha"))
                    .setCRC(checksum.compute(1, "alpha".getBytes()))
                    .build();

            WALEntry entry2 = WALEntry.newBuilder()
                    .setLogSequenceNumber(2)
                    .setData(ByteString.copyFromUtf8("beta"))
                    .setCRC(checksum.compute(2, "beta".getBytes()))
                    .build();

            writeEntry(out, entry1);
            writeEntry(out, entry2);
        }

        WALSegmentReader reader = new WALSegmentReader(walFile.toString(), checksum);
        WALEntry last = reader.readLastEntry();

        assertNotNull(last);
        assertEquals(2, last.getLogSequenceNumber());
        assertEquals("beta", last.getData().toStringUtf8());
    }

    @Test
    void testEmptyFileReadLastEntryReturnsNull() throws Exception {
        Path walFile = tempDir.resolve("empty.wal");
        walFile.toFile().createNewFile();

        WALSegmentReader reader = new WALSegmentReader(walFile.toString(), checksum);
        assertNull(reader.readLastEntry());
    }
}
