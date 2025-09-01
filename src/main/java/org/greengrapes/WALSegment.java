package org.greengrapes;

import org.greengrapes.proto.WALEntry;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class WALSegment {

    private static final long DEFAULT_SYNC_INTERVAL = 200L; // milliseconds

    private final long segmentNumber;
    private final boolean immediateFsync;
    private final Checksum checksum;
    private final Path path;
    private final BufferedOutputStream bufferedStream;
    private final FileChannel channel;
    private final WALSegmentReader reader;
    private final ScheduledExecutorService scheduler;
    private final ReentrantLock lock = new ReentrantLock(true);

    private long logicalBufferSize = 0;

    public WALSegment(long segmentNumber, String filePath, Boolean immediateFsync, Checksum checksum) {
        this.segmentNumber = segmentNumber;
        this.immediateFsync = immediateFsync;
        this.checksum = checksum;

        this.path = Path.of(filePath);
        try {
            this.bufferedStream = new BufferedOutputStream(
                    Files.newOutputStream(path,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.WRITE,
                            StandardOpenOption.APPEND)
            );

            this.channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.reader = new WALSegmentReader(path.toString(), checksum);

        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.scheduler.scheduleWithFixedDelay(this::flush, DEFAULT_SYNC_INTERVAL, DEFAULT_SYNC_INTERVAL, TimeUnit.MILLISECONDS);
    }

    public long getSegmentNumber() {
        return segmentNumber;
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
        return reader.readAll();
    }

    public WALEntry readLastEntry() throws IOException {
        flush();
        return reader.readLastEntry();
    }

    public void flush() {
        flush(false);
    }

    public void commit() {
        flush(true);
    }

    public void flush(boolean forceSync) {
        lock.lock();
        try {
            bufferedStream.flush();
            logicalBufferSize = 0;

            if (forceSync || immediateFsync) {
                channel.force(true);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    public void close() {
        lock.lock();
        try {
            commit();

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
        } finally {
            lock.unlock();
        }
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
}
