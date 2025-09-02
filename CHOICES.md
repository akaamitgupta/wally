## 🏗️ Design Choices

1. **Compact & structured serialization**

    * We used **Protobuf** instead of JSON/text because it’s binary, smaller in size, and enforces schema compatibility. This makes recovery safer and faster.

2. **Log entry format**

    * Chose a **\[length]\[data]\[checksum]** format.
    * Length prefix allows unambiguous parsing, and a CRC32 checksum ensures corruption detection.

3. **Endianness**

    * Adopted **little-endian** ordering for integers, matching modern CPU architectures and improving I/O performance.

4. **Buffering & flushing strategy**

    * Data is first written into a buffered stream, then periodically flushed to the OS page cache.
    * For durability, `fsync` forces the OS to persist to disk. This lets us trade off between **throughput (batched fsync)** and **durability (fsync on every write)**.

5. **Thread safety & concurrency**

    * Used **ReentrantLock** for safe concurrent writes and flushes.
    * Learned why reentrancy matters — e.g., `close()` internally calls `flush()`, both using the same lock.

6. **File rotation & retention**

    * WAL segments rotate after reaching a size limit.
    * Old files are deleted based on retention policy to prevent unbounded disk growth.

7. **Crash recovery**

    * On startup, WAL replays entries from existing segments.
    * Corruption is detected with CRC32; truncated entries are discarded.

8. **Background maintenance**

    * A **single-threaded scheduler** periodically flushes buffered data (every 200 ms in our case).
    * Simplifies durability guarantees without blocking writers.

---

## 📚 Learnings

* **Durability is subtle** → writes first land in the OS cache, not disk; explicit `fsync` is required.
* **Checksums are critical** → CRC32 is fast but weak; in real systems, stronger checksums or even cryptographic hashes may be used.
* **Thread safety isn’t trivial** → I had to learn reentrant locking patterns to avoid deadlocks in nested calls.
* **Performance trade-offs** → batching improves speed but risks data loss on crash; fine-tuning flush intervals is application-dependent.
* **Repairing logs is non-trivial** → you can only safely discard corrupted/truncated entries at the tail, not in the middle.

---

## 🚀 Future Improvements

* Configurable flush strategies (time-based, entry-count-based, or adaptive).
* Pluggable checksum algorithms (CRC64, SHA-256).
* Streaming read API (iterators instead of reading all entries).
* Recovery tooling for auto-repair of corrupted WAL segments.
* Benchmarking durability & throughput across different `fsync` strategies.
* Observability: metrics around flush latency, disk sync time, corruption events.
* Optional compression/encryption for production workloads.
