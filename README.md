# 🚀 LunaDB 🐈

**LunaDB** is a high-performance, disk-first document and key-value database designed for extreme concurrency, unbreakable durability, and sub-millisecond query speeds. Built from scratch in Go, it seamlessly blends the flexibility of a NoSQL document store with the ACID guarantees and structured querying capabilities of a traditional relational database, all secured over a TLS-encrypted custom TCP protocol.

---

## ⚡ Performance: LunaDB vs. PostgreSQL

LunaDB was built to solve specific bottlenecks in traditional relational databases, particularly around high-concurrency write spikes and deep pagination.

In a simulated Enterprise ERP peak load benchmark (**1,000,000** historical records, **10,000** concurrent connections), LunaDB's architecture shines:

| Operation / Metric | LunaDB 🐈 | PostgreSQL 🐘 | Winner |
| :--- | :--- | :--- | :--- |
| **Deep Pagination** (`OFFSET 500k`) | **~4.4 ms** | ~490.6 ms | **LunaDB** (110x faster) |
| **Distinct Grouping** (Array sizing) | **~0.5 ms** | ~105.2 ms | **LunaDB** (200x faster) |
| **Concurrent Lookups** (10k CCU) | **~10 µs / req** | ~176 µs / req | **LunaDB** (17x faster) |
| **High-Stress Update Rush** (10k CCU) | **~1.01 s** | ~2.26 s | **LunaDB** (2.2x faster) |
| **Mass Purge** (Delete 10k items) | **~129 ms** | ~132 ms | **Tie** |

*How?* By utilizing an in-memory B-Tree index mapped to a robust physical disk engine (`bbolt`), combined with Zero-Copy BSON parsing and aggressive automatic Group Commits (Write Batching).

---

## ✨ Key Architecture & Features

- 💾 **Disk-First ACID Persistence:** Powered by `bbolt`, LunaDB ensures that acknowledged writes are instantly and safely synced to disk. No data loss, no volatile memory limits—your database is fully ACID compliant by default.
- 🚦 **Massive Concurrency via Group Commit:** To solve the traditional disk I/O bottleneck, LunaDB implements an advanced **WriteBatcher**. It intelligently queues and merges thousands of concurrent micro-transactions into a single, atomic physical disk write.
- 🧠 **Hybrid B-Tree Indexing:** LunaDB keeps the actual payload safely on disk while maintaining lightning-fast **B-Tree indexes in RAM**. This architecture allows for microsecond equality lookups, deep pagination (`OFFSET`/`LIMIT`), and range scans (`>`, `<`, `BETWEEN`) without touching the disk until the exact documents are identified.
- 🐈 **Zero-Copy BSON Engine:** Data is streamed and patched at the byte level. LunaDB reads binary BSON directly from the memory-mapped file and evaluates queries or sends data over the network with minimal reflection or struct unmarshalling, drastically reducing Go's Garbage Collection (GC) overhead.
- 📦 **Stateful Transactions:** Go beyond simple atomic operations. LunaDB supports `BEGIN`, `COMMIT`, and `ROLLBACK` commands. Complex, multi-document mutations are queued in memory and executed atomically, ensuring perfect data integrity across collections.
- 🛡️ **Hot Backups & Restores:** Perform full database snapshots (`backup`) or logical wipe-and-restores (`restore`) directly from the CLI without locking the database or stopping the world.
- 🔍 **Advanced Server-Side Query Optimizer:** Query your flexible JSON/BSON documents with a powerful execution engine supporting:
  - **Rich Filtering**: `AND`, `OR`, `NOT`, `LIKE` (Regex-backed), `IN`, `BETWEEN`, `IS NULL`.
  - **Massive Batch Mutations:** `UPDATE WHERE` and `DELETE WHERE` executed entirely server-side.
  - **Streaming Aggregations**: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` with `GROUP BY` calculated on-the-fly without blowing up RAM.
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

 _This spins up the main database server securely on port `5876`

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

Type `help` once connected to explore the full suite of commands. For a detailed query guide, refer to the **[`docs/client.md`](https://github.com/adoboscan21/lunadb/blob/main/docs/client.md)** file.

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
