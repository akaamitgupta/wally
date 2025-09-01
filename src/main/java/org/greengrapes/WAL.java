package org.greengrapes;

import com.google.protobuf.ByteString;
import org.greengrapes.proto.WALEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.greengrapes.WALUtils.findLatestSegmentNumber;
import static org.greengrapes.WALUtils.findOldestSegmentNumber;
import static org.greengrapes.WALUtils.getAllSegmentNumbers;
import static org.greengrapes.WALUtils.getSegmentFilePath;

public class WAL {
    private static final Logger logger = LoggerFactory.getLogger(WAL.class);
    private static final long DEFAULT_SEGMENT_NUMBER = 1L;
    private static final long DEFAULT_LOG_SEQUENCE_NUMBER = 1L;
    private static final Checksum CHECKSUM = new Checksum();

    private final String directoryPath;
    private final boolean immediateFsync;
    private final long maxSegmentSize; // in bytes
    private final long maxSegments;
    private final Lock lock = new ReentrantLock(true);

    private WALSegment currentSegment;
    private long lastLogSequenceNumber;

    private WAL(String directoryPath, boolean immediateFsync, long maxSegmentSize, long maxSegments, WALSegment currentSegment) {
        this.directoryPath = directoryPath;
        this.immediateFsync = immediateFsync;
        this.maxSegmentSize = maxSegmentSize;
        this.maxSegments = maxSegments;
        this.currentSegment = currentSegment;
        this.lastLogSequenceNumber = getLastLogSequenceNumber(currentSegment);
    }

    public static WAL openWAL(String directoryPath, boolean immediateFsync, long maxSegmentSize, long maxSegments) {
        return new WAL(
                directoryPath,
                immediateFsync,
                maxSegmentSize,
                maxSegments,
                openLatestSegment(directoryPath, immediateFsync)
        );
    }

    private static WALSegment openLatestSegment(String directoryPath, boolean immediateFsync) {
        return openSegmentBySegmentNumber(
                directoryPath,
                findLatestSegmentNumber(directoryPath, DEFAULT_SEGMENT_NUMBER),
                immediateFsync
        );
    }

    private static WALSegment openSegmentBySegmentNumber(String directoryPath, long segmentNumber, boolean immediateFsync) {
        Path dir = Path.of(directoryPath);
        try {
            Files.createDirectories(dir); // ensures directory exists
        } catch (IOException e) {
            throw new RuntimeException("Failed to create WAL directory " + directoryPath, e);
        }

        String filePath = getSegmentFilePath(dir.toString(), segmentNumber);
        return new WALSegment(segmentNumber, filePath, immediateFsync, CHECKSUM);
    }

    public void write(byte[] data) {
        WALEntry entry = WALEntry.newBuilder()
                .setLogSequenceNumber(++lastLogSequenceNumber)
                .setData(ByteString.copyFrom(data))
                .build();

        lock.lock();
        try {
            rotateLogIfNeeded(entry);
            currentSegment.write(entry);
        } finally {
            lock.unlock();
        }
    }

    public List<WALRecord> read() {
        return currentSegment.readAll().stream()
                .map(entry -> new WALRecord(entry.getLogSequenceNumber(), entry.getData().toByteArray()))
                .collect(Collectors.toList());
    }

    public List<WALRecord> readAll() {
        List<WALRecord> allRecords = new ArrayList<>();

        List<Long> segmentNumbers = getAllSegmentNumbers(directoryPath);
        segmentNumbers.sort(Long::compareTo);

        segmentNumbers.forEach(num -> {
            WALSegmentReader segment = new WALSegmentReader(getSegmentFilePath(directoryPath, num), CHECKSUM);
            allRecords.addAll(
                    segment.readAll().stream()
                            .map(entry -> new WALRecord(entry.getLogSequenceNumber(), entry.getData().toByteArray()))
                            .toList()
            );
        });

        return allRecords;
    }

    public void flush() {
        currentSegment.flush();
    }

    public void commit() {
        currentSegment.commit();
    }

    public void close() {
        currentSegment.close();
    }

    private long getLastLogSequenceNumber(WALSegment segment) {
        try {
            WALEntry entry = segment.readLastEntry();
            return entry != null ? entry.getLogSequenceNumber() : DEFAULT_LOG_SEQUENCE_NUMBER - 1;
        } catch (IOException e) {
            logger.warn("Error reading last entry from WAL segment: {}", e.getMessage(), e);
            return DEFAULT_LOG_SEQUENCE_NUMBER - 1;
        }
    }

    private void rotateLogIfNeeded(WALEntry entry) {
        if (currentSegment.size() + entry.toByteArray().length >= maxSegmentSize) {
            rotateLog();
        }
    }

    private void rotateLog() {
        currentSegment.close();

        long nextSegmentNumber = currentSegment.getSegmentNumber() + 1;
        long oldestSegmentNumber = findOldestSegmentNumber(directoryPath, DEFAULT_SEGMENT_NUMBER);

        if ((nextSegmentNumber - oldestSegmentNumber) >= maxSegments) {
            removeSegmentFileBySegmentNumber(oldestSegmentNumber);
        }

        currentSegment = openSegmentBySegmentNumber(directoryPath, nextSegmentNumber, immediateFsync);
    }

    private void removeSegmentFileBySegmentNumber(long segmentNumber) {
        Path filePath = Path.of(getSegmentFilePath(directoryPath, segmentNumber));
        try {
            Files.deleteIfExists(filePath);
            logger.info("Deleted WAL segment file: {}", filePath);
        } catch (IOException e) {
            logger.warn("Failed to delete WAL segment file: {}", filePath, e);
        }
    }
}
