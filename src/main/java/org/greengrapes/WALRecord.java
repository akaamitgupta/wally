package org.greengrapes;

public class WALRecord {
    private final long logSequenceNumber;
    private final byte[] data;

    public WALRecord(long logSequenceNumber, byte[] data) {
        this.logSequenceNumber = logSequenceNumber;
        this.data = data;
    }

    public long getLogSequenceNumber() {
        return logSequenceNumber;
    }

    public byte[] getData() {
        return data;
    }
}
