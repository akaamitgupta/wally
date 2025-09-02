## 🔧 Improvements & Pitfalls

3. **Crash recovery / partial entry handling**

    * If the process crashes mid-write, truncated or partially written entries may remain.
    * Current reader only validates checksums and fails on corruption.
    * No mechanism to automatically detect and repair incomplete entries (Go WAL can repair the last segment).

4. **Incomplete checkpointing**

    * `isCheckpoint` flag exists in the proto schema but is not fully integrated.
    * No API to create, persist, or restore from checkpoints.
    * Recovery always replays the entire WAL instead of resuming from the last valid checkpoint.

5. **Limited read API**

    * Only supports `read()` (single segment) and `readAll()` (all segments).
    * Missing important features:

        * Read from a specific offset or sequence number.
        * Resume reading after the last checkpoint.
        * Stream entries incrementally instead of loading all into memory.

6. **Rigid background flusher**

    * Flush interval fixed at **200 ms**, not configurable.
    * No adaptive flushing strategies (e.g., flush after N entries, or idle-based flushing).
    * This can cause either excessive fsyncs or delayed durability under load.

7. **Weak exception handling**

    * I/O issues are currently wrapped in generic `RuntimeException`.
    * Lack of error categorization makes it hard for callers to distinguish between recoverable vs fatal errors.
    * Should introduce a dedicated `WALException` hierarchy (e.g., `WALCorruptionException`, `WALWriteException`, `WALSyncException`).

8. **No benchmarking of sync/flush strategies**

* No performance benchmarks to compare durability guarantees across:

    * Immediate `fsync` (per write).
    * Batched `fsync` (group commit).
    * Background periodic flush (current 200 ms).
* Benchmarking would help evaluate trade-offs between throughput, latency, and durability guarantees.
