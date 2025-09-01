package org.greengrapes;

import org.greengrapes.proto.WALEntry;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class WALSegmentReader {
    private static final int MAX_ENTRY_SIZE = 10 * 1024 * 1024; // 10 MB

    private final Path path;
    private final Checksum checksum;

    public WALSegmentReader(String filePath, Checksum checksum) {
        this.checksum = checksum;
        path = Path.of(filePath);
    }

    public List<WALEntry> readAll() {
        List<WALEntry> entries = new ArrayList<>();

        try (DataInputStream in = new DataInputStream(new FileInputStream(path.toFile()))) {
            while (true) {
                try {
                    byte[] sizeBytes = new byte[4];
                    in.readFully(sizeBytes);
                    int size = ByteBuffer.wrap(sizeBytes)
                            .order(ByteOrder.LITTLE_ENDIAN)
                            .getInt();

                    if (size <= 0 || size > MAX_ENTRY_SIZE) {
                        throw new IOException("Invalid WAL entry size: " + size);
                    }

                    byte[] data = new byte[size];
                    in.readFully(data);

                    WALEntry entry = WALEntry.parseFrom(data);
                    checksum.verify(entry.getLogSequenceNumber(), entry.getData().toByteArray(), entry.getCRC());
                    entries.add(entry);
                } catch (EOFException eof) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return entries;
    }

    public WALEntry readLastEntry() throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
            long lastOffset = 0;
            int lastSize = 0;

            while (true) {
                // Read the size prefix (4 bytes little-endian)
                byte[] sizeBytes = new byte[4];
                int bytesRead = raf.read(sizeBytes);
                if (bytesRead == -1) {
                    // EOF reached
                    if (lastOffset == 0) {
                        return null; // file was empty
                    }

                    // Seek back to last entry
                    raf.seek(lastOffset);

                    byte[] data = new byte[lastSize];
                    raf.readFully(data);

                    // Unmarshal only once at the end
                    WALEntry entry = WALEntry.parseFrom(data);
                    checksum.verify(entry.getLogSequenceNumber(), entry.getData().toByteArray(), entry.getCRC());
                    return entry;
                }

                if (bytesRead < 4) {
                    // Corrupted/truncated entry
                    throw new EOFException("Unexpected EOF while reading entry size");
                }

                int size = ByteBuffer.wrap(sizeBytes)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .getInt();

                if (size <= 0 || size > MAX_ENTRY_SIZE) {
                    throw new IOException("Invalid WAL entry size: " + size);
                }

                // Record the starting offset of this entry
                lastOffset = raf.getFilePointer();

                // Skip forward by entry size (don’t read yet)
                if (raf.skipBytes(size) < size) {
                    throw new EOFException("Unexpected EOF while skipping entry data");
                }

                lastSize = size;
            }
        }
    }
}
