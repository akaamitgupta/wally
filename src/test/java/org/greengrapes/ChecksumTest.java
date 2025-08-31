package org.greengrapes;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ChecksumTest {

    private final Checksum checksum = new Checksum();

    @Test
    void testComputeConsistency() {
        long lsn = 42L;
        byte[] data = "hello".getBytes();

        int c1 = checksum.compute(lsn, data);
        int c2 = checksum.compute(lsn, data);

        assertEquals(c1, c2, "Checksum should be deterministic and consistent");
    }

    @Test
    void testDifferentDataDifferentChecksum() {
        long lsn = 42L;

        int c1 = checksum.compute(lsn, "hello".getBytes());
        int c2 = checksum.compute(lsn, "world".getBytes());

        assertNotEquals(c1, c2, "Different data should produce different checksums");
    }

    @Test
    void testDifferentLsnDifferentChecksum() {
        byte[] data = "hello".getBytes();

        int c1 = checksum.compute(1L, data);
        int c2 = checksum.compute(2L, data);

        assertNotEquals(c1, c2, "Different LSN values should produce different checksums");
    }

    @Test
    void testVerifyPassesWhenChecksumMatches() {
        long lsn = 123L;
        byte[] data = "entry".getBytes();
        int c = checksum.compute(lsn, data);

        assertDoesNotThrow(() -> checksum.verify(lsn, data, c),
                "verify should not throw if checksum matches");
    }

    @Test
    void testVerifyThrowsOnMismatch() {
        long lsn = 123L;
        byte[] data = "entry".getBytes();
        int wrongChecksum = 999999; // something incorrect

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> checksum.verify(lsn, data, wrongChecksum));

        assertTrue(ex.getMessage().contains("Checksum mismatch"),
                "Exception message should indicate mismatch");
    }
}
