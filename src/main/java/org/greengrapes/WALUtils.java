package org.greengrapes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class WALUtils {
    public static String getSegmentFilePath(String directoryPath, long segmentNumber) {
        return Path.of(directoryPath).resolve("wal_segment_" + segmentNumber + ".log").toString();
    }

    public static long findLatestSegmentNumber(String directoryPath, long defaultSegmentNumber) {
        return getAllSegmentNumbers(directoryPath).stream()
                .max(Long::compareTo)
                .orElse(defaultSegmentNumber);
    }

    public static long findOldestSegmentNumber(String directoryPath, long defaultSegmentNumber) {
        return getAllSegmentNumbers(directoryPath).stream()
                .min(Long::compareTo)
                .orElse(defaultSegmentNumber);
    }

    public static List<Long> getAllSegmentNumbers(String directoryPath) {
        try {
            return Files.list(Path.of(directoryPath))
                    .filter(path -> path.getFileName().toString().startsWith("wal_segment_"))
                    .map(WALUtils::extractSegmentNumber)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            return Collections.emptyList();
        }
    }

    private static long extractSegmentNumber(Path path) {
        String name = path.getFileName().toString();
        String numberPart = name.replace("wal_segment_", "").replace(".log", "");
        return Long.parseLong(numberPart);
    }
}
