package org.greengrapes;

import org.greengrapes.proto.WALEntry;

public class Fixtures {
    public static WALEntry buildWALEntry(long lsn, String payload) {
        return WALEntry.newBuilder()
                .setLogSequenceNumber(lsn)
                .setData(com.google.protobuf.ByteString.copyFromUtf8(payload))
                .setCRC(payload.hashCode())
                .build();
    }
}
