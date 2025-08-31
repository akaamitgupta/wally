package org.greengrapes;

import com.google.protobuf.ByteString;
import org.greengrapes.proto.WALEntry;

public class Fixtures {
    public static WALEntry buildWALEntry(long lsn, String payload) {
        return WALEntry.newBuilder()
                .setLogSequenceNumber(lsn)
                .setData(ByteString.copyFromUtf8(payload))
                .build();
    }
}
