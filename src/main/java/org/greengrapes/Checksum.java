package org.greengrapes;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class Checksum {

    public int compute(long lsn, byte[] data) {
        java.util.zip.Checksum checksum = new CRC32();

        checksum.update(ByteBuffer.allocate(Long.BYTES).putLong(lsn).array());
        checksum.update(data);

        return (int) checksum.getValue();
    }

    public void verify(long lsn, byte[] data, int expectedChecksum) {
        int actualChecksum = compute(lsn, data);

        if (expectedChecksum != actualChecksum) {
            throw new IllegalStateException("Checksum mismatch for entry with LSN " + lsn);
        }
    }
}
