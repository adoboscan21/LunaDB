# 🚀 LunaDB 🐈

**LunaDB** is a high-performance, disk-first document and key-value database designed for extreme concurrency, unbreakable durability, and sub-millisecond query speeds. Built from scratch in Go, it seamlessly blends the flexibility of a NoSQL document store with the ACID guarantees and structured querying capabilities of a traditional relational database, all secured over a TLS-encrypted custom TCP protocol.

---

## 🎯 When to use LunaDB (Use Cases)

LunaDB is highly specialized. It is not a silver bullet for every problem, but in its "sweet spot," it outperforms decades-old giants. Here is a transparent guide on when to choose LunaDB over PostgreSQL or MongoDB:

| **Application / Use Case**                 | **Why LunaDB? 🐈**                                                                                                                                                                                | **When to use PostgreSQL 🐘 / MongoDB 🍃 instead?**                                                                                                 |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Edge Computing & Local POS**             | **Perfect Fit.** Compiles to a single lightweight binary, starts in 1 second, zero DBA maintenance required, and uses `mmap` to keep RAM usage minimal in constrained hardware.                   | Use Postgres if you require massive multi-node distributed clusters (though Mongo is better for this) or complex OLAP data warehousing at the edge. |
| **Telemetry, Logs & Audit (Append-Heavy)** | **Perfect Fit.** The sharded _Group Commit_ architecture absorbs massive write spikes (100k+ ops) by batching them into sequential disk writes, easily outperforming standard relational inserts. | Use MongoDB if you need native auto-tiering to cold storage, or Postgres if your logs require heavy, multi-table analytical aggregations.           |
| **E-commerce Catalogs (Read-Heavy)**       | **Perfect Fit.** With ~8µs concurrent lookups and Zero-Allocation BSON reading, it serves as an ultra-fast persistent data store for read-heavy web applications.                                 | Use PostgreSQL if your catalog requires complex relational constraints, strict foreign keys, and multi-table ACID transactions.                     |
| **Persistent L2 Cache**                    | **Perfect Fit.** Replaces Redis/Memcached when you have Terabytes of cache data that exceeds available RAM, persisting safely to disk with near-memory read speeds.                               | Use Redis if you strictly need 100% in-memory sub-microsecond latency and data loss on restart is acceptable.                                       |
| **High-Contention Financial Systems**      | **Avoid.** LunaDB uses collection/shard-level locking during extreme mixed read/write rushes.                                                                                                     | **Perfect Fit (PostgreSQL).** Postgres uses Row-Level Locking and MVCC, handling thousands of simultaneous balance updates without queuing.         |
| **High Availability (HA) Clusters**        | **Avoid.** LunaDB currently operates as a robust single-node engine.                                                                                                                              | **Perfect Fit (MongoDB).** Mongo's native Replica Sets provide automatic failover and zero-downtime redundancy.                                     |

---

## ⚡ Performance: LunaDB vs. PostgreSQL

LunaDB was built to solve specific bottlenecks in traditional relational databases, particularly around data ingestion, deep pagination, and read-heavy workloads.

In a simulated Enterprise ERP peak load benchmark (**1,000,000** historical records, **100,000** concurrent connections), LunaDB's architecture shines:

|**Business Process / Metric**|**LunaDB 🐈**|**PostgreSQL 🐘**|**Absolute Winner**|
|---|---|---|---|
|**Data Gen:** Historical Orders|**~13.7 s**|~32.5 s|**LunaDB** (2.3x faster)|
|**Action:** Concurrent Lookups|**~8.7 µs**|~35.0 µs|**LunaDB** (4x faster)|
|**Report:** Deep Pagination (USD)|**~119.6 ms**|~361.1 ms|**LunaDB** (3x faster)|
|**BI:** Distinct Quantities|**~5.4 ms**|~43.0 ms|**LunaDB** (7.8x faster)|
|**Action:** Price Adjustments (100k)|~138.8 µs|**~89.0 µs**|**PostgreSQL** (MVCC shines)|
|**Stress:** Quincena Rush (Mixed Ops)|~7.69 s|**~6.32 s**|**PostgreSQL** (Row-level locking)|

_How does it read so fast?_ By utilizing an in-memory B-Tree index mapped to a robust physical disk engine (`bbolt`), combined with Zero-Copy BSON parsing and aggressive automatic Group Commits (Write Batching).

---

## ✨ Key Architecture & Features

- 💾 **True ACID Durability:** Powered by `bbolt`, LunaDB ensures strict durability. The network layer _never_ acknowledges a write to the client until the physical disk `fsync` is successfully confirmed. No premature ACKs, no data loss on power failures.

- 🗄️ **Static Sharding:** LunaDB divides your data across multiple physical disk files (`shard_0.db`, `shard_1.db`) to bypass OS-level I/O bottlenecks. The topology is safely locked upon creation via an immutable `system_metadata.json`, preventing catastrophic human-error misconfigurations during reboots.

- 🌊 **Streaming Mutations (O(1) Memory):** Execute `UPDATE WHERE` and `DELETE WHERE` on millions of documents without Out-Of-Memory (OOM) crashes. LunaDB processes bulk operations in chunks, keeping RAM usage flat and predictable.

- 🛡️ **Null-Byte Safe B-Trees:** Implements advanced _Order-Preserving Escape_ encoding for B-Tree text indexes. Malicious inputs or UUIDs with null bytes cannot corrupt the index boundaries or break range scans.

- 🚦 **Massive Concurrency via Group Commit:** To solve the traditional disk I/O bottleneck, LunaDB implements an advanced **WriteBatcher**. It intelligently queues and merges thousands of concurrent micro-transactions into a single, atomic physical disk write.

- 🐈 **Zero-Copy BSON Engine:** Data is streamed and patched at the byte level. LunaDB reads binary BSON directly from the memory-mapped file and evaluates queries or sends data over the network with minimal reflection or struct unmarshalling, drastically reducing Go's Garbage Collection (GC) overhead.

- 📦 **Stateful Transactions:** Go beyond simple atomic operations. LunaDB supports `BEGIN`, `COMMIT`, and `ROLLBACK` commands. Complex, multi-document mutations are queued in memory and executed atomically, ensuring perfect data integrity across collections.

- 🔍 **Advanced Server-Side Query Optimizer:** Query your flexible JSON/BSON documents with a powerful execution engine supporting:

  - **Rich Filtering**: `AND`, `OR`, `NOT`, `LIKE` (Regex-backed), `IN`, `BETWEEN`, `IS NULL`.

  - **Streaming Aggregations**: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` with `GROUP BY` calculated on-the-fly using Lock Striping.

  - **Zero-Copy Joins**: A powerful `lookups` pipeline to instantly join documents from different collections.

- 🔐 **Enterprise-Grade Security:**

  - **TLS Encryption:** All client-server communication is encrypted with TLS 1.2+ out of the box.

  - **Granular RBAC:** Create users and assign specific `read`/`write` permissions down to the collection level.

---

## ⚙️ Quick Start with Docker Compose

To get the LunaDB server up and running quickly, follow these steps:

1. **Copy the environment file:**

```bash
   cp .example.env .env
```

1. **Start the services:**

```bash
   docker compose up -d --build
```

_This spins up the main database server securely on port `5876`_

---

## 🛠️ Manual Installation and Build

### Prerequisites

You need **Go version 1.25 or higher** to build and run this project.

### 1. Generate TLS Certificates 🔒

LunaDB requires TLS for all communications. Generate a self-signed certificate pair and place it in the `./certificates/` directory.

```bash
mkdir -p certificates
```

```bash
openssl req -x509 -newkey rsa:4096 -nodes -keyout certificates/server.key -out certificates/server.crt -days 3650 -subj "/CN=localhost" -addext "subjectAltName = DNS:localhost,IP:127.0.0.1"
```

### 2. Build and Run

Compile the Server and Client binaries:

```bash
go build -o ./bin/lunadb-server .
go build -o ./bin/lunadb-client ./cmd/client
```

Run the Server:

```bash
./bin/lunadb-server
```

---

## 🖥️ CLI Client

LunaDB comes with an interactive, autocompleting CLI client to easily manage your data, collections, and users.

**To connect inside Docker:**

```bash
docker exec -it <container-id> ./lunadb-client -u root -p rootpass
```

**For a direct, local authenticated connection:**

```bash
./bin/lunadb-client -u admin -p adminpass
```

> **⚠️ Security Notice:** The default password for `admin` is `adminpass`, and for `root` (localhost only) is `rootpass`. Change these immediately in a production environment using the `update password` command.

Type `help` once connected to explore the full suite of commands. For a detailed query guide, refer to the **[`docs/client.md`](https://www.google.com/search?q=%5Bhttps://github.com/adoboscan21/lunadb/blob/main/docs/client.md%5D\(https://github.com/adoboscan21/lunadb/blob/main/docs/client.md\))** file.

---

## ❤️ Support the Project

Hello! I'm the developer behind **LunaDB**. This is a passionate open-source effort to build a modern, high-performance database engine from scratch in Go.

If LunaDB has helped you learn, build, or scale your applications, consider supporting its continued development. Your contributions allow me to maintain the codebase, implement new features, and keep the project thriving.

### How You Can Help

Every contribution is enormously appreciated. You can make a direct donation via PayPal:

**[Click here to donate via PayPal](https://paypal.me/AdonayB?locale.x=es_XC&country.x=VE)**

### Other Ways to Contribute

Can't donate? You can still make a huge impact:

- ⭐ **Star this repository** to help others find it.

- 🗣️ **Share the project** on social media or with your engineering team.

- 🐛 **Report bugs** or request features by opening an issue.

- 💻 **Contribute code** by submitting a Pull Request.

Thank you for your support!
