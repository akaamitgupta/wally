package org.greengrapes;

import org.greengrapes.proto.WALEntry;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class WALSegment {
    private static final int MAX_ENTRY_SIZE = 10 * 1024 * 1024; // 10 MB
    private final long segmentNumber;
    private final boolean immediateFsync;
    private final Checksum checksum;
    private final Path path;
    private final BufferedOutputStream bufferedStream;
    private final FileChannel channel;
    private final ScheduledExecutorService scheduler;
    private final ReentrantLock lock = new ReentrantLock(true);
    private final long SYNC_INTERVAL = 200L; // milliseconds
    private long logicalBufferSize = 0;

    public WALSegment(long segmentNumber, String filePath, Boolean immediateFsync, Checksum checksum) {
        this.segmentNumber = segmentNumber;
        this.immediateFsync = immediateFsync;
        this.checksum = checksum;

        path = Path.of(filePath);
        try {
            bufferedStream = new BufferedOutputStream(Files.newOutputStream(path,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND));

            channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleWithFixedDelay(this::flush, SYNC_INTERVAL, SYNC_INTERVAL, TimeUnit.MILLISECONDS);
    }

    public long getSegmentNumber() {
        return segmentNumber;
    }

    @Override
    public String toString() {
        return "WALSegment{" +
                "segmentNumber=" + segmentNumber +
                ", path=" + path +
                ", immediateFsync=" + immediateFsync +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WALSegment that)) return false;
        return segmentNumber == that.segmentNumber &&
                Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(segmentNumber, path);
    }

    public long size() {
        try {
            return Files.size(path) + logicalBufferSize;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void write(WALEntry entry) {
        lock.lock();

        try {
            byte[] data = entry.toBuilder()
                    .setCRC(checksum.compute(entry.getLogSequenceNumber(), entry.getData().toByteArray()))
                    .build()
                    .toByteArray();
            byte[] sizeBytes = ByteBuffer
                    .allocate(4)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .putInt(data.length)
                    .array();

            bufferedStream.write(sizeBytes);
            bufferedStream.write(data);

            logicalBufferSize += sizeBytes.length + data.length;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    public List<WALEntry> readAll() {
        flush();

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

    public void close() {
        flush();

        try {
            bufferedStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            channel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void flush() {
        lock.lock();

        try {
            bufferedStream.flush();
            logicalBufferSize = 0;

            if (immediateFsync) {
                channel.force(true);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }
}
