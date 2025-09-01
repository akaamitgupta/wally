# Write-Ahead Log (WAL) in Java

This project is a **Write-Ahead Log (WAL)** implementation in Java.  
It provides durable, append-only logging of data entries, which is useful for building databases, message queues, and fault-tolerant systems.

The WAL ensures that all writes are persisted to disk before being acknowledged, providing **crash safety** and **data integrity**.

---

## ✨ Features

- **Segmented log storage**  
  Log files are split into segments to limit file size and allow rotation.

- **CRC32 checksum validation**  
  Ensures data integrity on reads.

- **Configurable options**  
  - Max segment size (bytes)  
  - Max number of segments retained  
  - Immediate `fsync` or buffered writes  

- **Automatic log rotation**  
  Oldest segment files are deleted once the maximum segment limit is reached.

- **Concurrency safety**  
  Uses locks to ensure thread-safe writes and flushes.

- **Protobuf serialization**  
  Log entries are serialized using [Protocol Buffers](https://developers.google.com/protocol-buffers).

---

## 📦 Project Structure

- `WAL` → High-level interface for writing/reading log entries.  
- `WALSegment` → Manages a single segment file (append, flush, commit).  
- `WALSegmentReader` → Reads all or last entries from a segment file.  
- `WALRecord` → Simple wrapper for log entries (LSN + byte[]).  
- `Checksum` → Implements CRC32-based integrity check.  
- `WALUtils` → Utility methods for file/segment management.  
- `proto/WALEntry.proto` → Protobuf definition of a WAL entry.

---

## 🚀 Usage

### 1. Open a WAL
```java
WAL wal = WAL.openWAL(
    "/tmp/wal",   // directory for WAL segments
    false,        // immediate fsync (true for durability, false for performance)
    10 * 1024L,   // max segment size = 10 KB
    5             // max number of segments
);
````

### 2. Write data

```java
wal.write("hello".getBytes());
wal.write("world".getBytes());
```

### 3. Read from current segment

```java
List<WALRecord> records = wal.read();
for (WALRecord rec : records) {
    System.out.println(rec.getLogSequenceNumber() + " -> " + new String(rec.getData()));
}
```

### 4. Read from all segments

```java
List<WALRecord> all = wal.readAll();
```

### 5. Flush and Close

```java
wal.flush();   // flush to disk
wal.commit();  // force fsync
wal.close();   // close resources
```

---

## 🛠️ Build

This project uses **Gradle** and **Protobuf**.

### Prerequisites

* Java 17+
* Gradle 7+
* Protobuf Compiler (`protoc`)

### Compile & Run

```bash
./gradlew build
```

---

## 📂 Protobuf Schema

```proto
message WALEntry {
  uint64   logSequenceNumber = 1;
  bytes    data = 2;
  uint32   CRC = 3;
  optional bool isCheckpoint = 4;
}
```

## File Format

Each WAL segment file contains a sequence of entries with the following format:

```
[4-byte size (little-endian)] [WALEntry protobuf data]
```

The WALEntry protobuf contains:
- `logSequenceNumber`: Monotonically increasing sequence number
- `data`: The actual log data
- `CRC`: CRC32 checksum for integrity verification
- `isCheckpoint`: Optional flag for checkpointing (reserved for future use)

---

## 📌 Notes

* By default, data is buffered and flushed every **200 ms**.
* To guarantee durability on each write, set `immediateFsync = true`.
* Large entries are capped at **10 MB**.

---

## 📜 License

MIT License
