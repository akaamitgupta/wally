package org.greengrapes;

import org.greengrapes.proto.WALEntry;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
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
    private final long segmentNumber;
    private final boolean immediateFsync;
    private final Path path;
    private final BufferedOutputStream bufferedStream;
    private final FileChannel channel;
    private final ScheduledExecutorService scheduler;
    private final ReentrantLock lock = new ReentrantLock(true);
    private final long SYNC_INTERVAL = 200L; // milliseconds

    public WALSegment(long segmentNumber, String filePath, Boolean immediateFsync) {
        this.segmentNumber = segmentNumber;
        this.immediateFsync = immediateFsync;

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

    public void write(WALEntry entry) {
        lock.lock();

        try {
            byte[] data = entry.toByteArray();
            byte[] sizeBytes = ByteBuffer
                    .allocate(4)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .putInt(data.length)
                    .array();

            bufferedStream.write(sizeBytes);
            bufferedStream.write(data);
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

                    byte[] data = new byte[size];
                    in.readFully(data);

                    WALEntry entry = WALEntry.parseFrom(data);
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

    private void flush() {
        lock.lock();

        try {
            bufferedStream.flush();

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
