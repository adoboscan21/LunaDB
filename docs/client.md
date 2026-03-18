# 🚀 LunaDB CLI Client Documentation 🚀

The `lunadb-client` is an interactive command-line interface (CLI) for direct, secure interaction with the `lunadb-server` via its custom TLS-encrypted TCP protocol. Designed for speed, security, and developer ergonomics, it seamlessly converts JSON to binary BSON under the hood.

## ▶️ How to Run

To start the client, you must provide the address of the `lunadb-server`. You can also include credentials for automatic login using flags.

**Locally:**

> `./bin/lunadb-client`
>
> `./bin/lunadb-client -u admin -p adminpass localhost:5876`

**Docker 🐳:**

> `docker exec -it <containerId> lunadb-client -u root -p rootpass localhost:5876`

*💡 Pro-Tip for Payloads: Instead of typing raw JSON strings, ensure you have a `json/` directory in your working path (e.g., `./json/payload.json`). The CLI will auto-resolve filenames passed as arguments, read the file, and convert it to BSON automatically.*

---

## 👥 User and Permission Management (Admins)

Authentication is required to execute most commands. User and permission management requires special privileges (Root or Write access to the `_system` collection).

- 🔐 **`login <username> <password>`**: Authenticates the connection with the server.

- ➕ **`user create <username> <password> <permissions_json|path>`**: Creates a new user. Example: `user create salesuser pass123 {"sales":"write", "products":"read"}`

- 🔄 **`user update <username> <permissions_json|path>`**: Completely replaces an existing user's permissions. Example: `user update salesuser {"*":"read"}`

- 🗑️ **`user delete <username>`**: Permanently deletes a user from the system.

- 🔑 **`update password <target_username> <new_password>`**: Updates a user's password. The `root` user can change anyone's password.

---

## 🛡️ Admin & Maintenance (Root Only)

Low-level administrative operations.

- 📦 **`backup`**: Triggers a **Hot Backup**. Safely takes a snapshot of the entire database to the `backups/` directory without locking concurrent reads or writes.

- 🔙 **`restore <backup_filename>`**: **Destructive Action!** Performs a Hot Restore. Wipes the current active buckets, restores the binary data from the specified backup file, and instantly rebuilds all B-Tree indexes in RAM.

---

## 📦 Stateful Transactions

LunaDB supports strict ACID transactions. Group multiple write operations and execute them atomically against the disk.

- **`begin`**: Starts a new transaction block. The prompt will change to `[TX]`. *(Note: Read collection commands like `get` or `query` are temporarily disabled in this mode).*

- **`commit`**: Atomically flushes all queued operations to the physical disk in a single write. Validates existence constraints (e.g., preventing updates on non-existent keys) before applying.

- **`rollback`**: Discards all queued commands in memory and closes the transaction safely.

---

## 🗂️ Collection Commands

### Collection Management

- ✨ **`collection create <collection_name>`**: Initializes a new physical bucket on disk.

- 🔥 **`collection delete <collection_name>`**: Drops the collection, wiping its data from disk and clearing its indexes from RAM.

- 📜 **`collection list`**: Lists all collections you have read access to.

### 📄 Collection Item Operations

*The `<value_json>` or `<patch_json>` can be a raw string or a file name inside the `json/` directory (e.g., `item.json`).*

- ✅ **`collection item set <collection> [<key>] <value_json|path>`**: Saves a document. Omitting the key auto-generates a UUID. Example: `collection item set products laptop-01 {"name": "Pro"}`

- 📤 **`collection item get <collection> <key>`**: Gets an item by its key.

- ✍️ **`collection item update <collection> <key> <patch_json|path>`**: Partially updates a document using a Zero-Copy BSON patch technique.

- 🗑️ **`collection item delete <collection> <key>`**: Permanently deletes an item from the disk and removes it from RAM indexes.

- 📋 **`collection item list <collection>`**: **(Root only)** Streams all items in the specified collection.

### ⚡ Batch Operations (High-Throughput)

Powered by LunaDB's `WriteBatcher` engine and Server-Side Query execution, these commands are designed to handle thousands of records in a single physical disk commit without moving data over the network.

- **`collection item set many <collection> <json_array|path>`**: Inserts multiple items at once. Skips existing keys. Example: `collection item set many products batch_insert.json`

- **`collection item update many <collection> <patch_json_array|path>`**: Applies patches to multiple items instantly. Payload must be a JSON array formatted as `[{"_id": "...", "patch": {...}}]`.

- **`collection item delete many <collection> <keys_json_array|path>`**: Mass deletes multiple items using a JSON array of string keys.

- 🎯 **`collection update where <collection> <query_json|path> <patch_json|path>`**: Mass updates all documents that match a specific query condition using a BSON patch. Highly optimized server-side operation. Example: `collection update where orders {"filter":{"field":"status","op":"=","value":"Pending"}} {"status":"Shipped"}`

- 🧨 **`collection delete where <collection> <query_json|path>`**: Mass deletes all documents that match a specific query condition. Fast and efficient index-level deletion. Example: `collection delete where orders {"filter":{"field":"status","op":"=","value":"Cancelled"}}`

---

## 🔍 Index Commands

Test performance differences by running queries before and after creating B-Tree indexes.

- 📈 **`collection index create <collection> <field_name>`**: Builds a B-Tree index in RAM for a specific field, drastically accelerating queries.

- 📜 **`collection index list <collection>`**: Lists all active indexes on the collection.

- 🔥 **`collection index delete <collection> <field_name>`**: Removes an existing index from RAM.

---

## ❓ Collection Query Command

Execute complex queries with filtering, sorting, joins, and on-the-fly aggregations using LunaDB's Zero-Copy BSON streaming engine.

- **`collection query <collection> <query_json|path>`**: Executes a query. Example: `collection query products query1.json`

### Query JSON Structure

| **Key** | **Type** | **Description** |
| :--- | :--- | :--- |
| `filter` | object | Conditions to select items (`WHERE` clause). Leverages B-Tree index merging. |
| `order_by` | array | Sorts the results (`[{"field": "price", "direction": "desc"}]`). |
| `limit` | number | Restricts the number of results (Optimized via Heap Sort or B-Tree fast paths). |
| `offset` | number | Skips results. Highly optimized for Deep Pagination if an index exists. |
| `count` | boolean | Returns a raw count of matching items. |
| `distinct` | string | Returns unique values for a field (Instantaneous if indexed). |
| `group_by` | array | Groups results for aggregation operations. |
| `aggregations` | object | Defines mathematical functions: `sum`, `avg`, `min`, `max`, `count`. |
| `having` | object | Filters results *after* mathematical aggregations are calculated. |
| `projection` | array | Selects strictly which fields to return. |
| `lookups` | array | Performs in-memory Hash Joins with documents from other collections. |

### Supported Filter Operators

| **Operator** | **Syntax** | **Description** |
| :--- | :--- | :--- |
| **Comparisons** | `=`, `!=`, `>`, `>=`, `<`, `<=` | Standard numeric and string comparisons. |
| **Pattern** | `like` | SQL-like pattern matching (`%` for wildcards). Backed by Regex caching. |
| **Inclusion** | `in`, `between` | Check array inclusion or numeric/string ranges. |
| **Nullity** | `is null`, `is not null` | Checks for the existence or absence of a field. |
| **Logical** | `and`, `or`, `not` | Combine multiple filter objects. |

### 🧠 Deep Query Examples

- **Complex Nested Filtering**: Find sales in the 'North' region that are either 'pending' OR have an amount > 1000.

> `collection query sales {"filter":{"and":[{"field":"region","op":"=","value":"North"},{"or":[{"field":"status","op":"=","value":"pending"},{"field":"amount","op":">","value":1000}]}]}}`

- **Pattern Matching & Range Testing**: Find users whose email ends in '@gmail.com' and age is between 25 and 40.

> `collection query users {"filter":{"and":[{"field":"email","op":"like","value":"%@gmail.com"},{"field":"age","op":"between","value":[25, 40]}]}}`

- **Multi-Aggregation Query**: For each salesperson, calculate their total sales (`SUM`), average sale amount (`AVG`), and number of sales (`COUNT`).

> `collection query sales {"aggregations":{"total_sold":{"func":"sum","field":"amount"},"average_sale":{"func":"avg","field":"amount"},"deal_count":{"func":"count","field":"_id"}},"group_by":["salesperson"]}`

- **Joining Collections (Lookups)**: Create a report from `inventory_status`, joining data from `products` and `suppliers` to get a complete view, showing only the product name, stock, and supplier name.

> `collection query inventory_status {"lookups":[{"from":"products","localField":"productId","foreignField":"_id","as":"product"},{"from":"suppliers","localField":"product.supplierId","foreignField":"_id","as":"supplier"}],"projection":["product.name","stock","supplier.name"]}`

---

## 💻 Client-Side Commands

- ℹ️ **`help`**: Displays the list of all commands dynamically fetched from the server.

- 💨 **`clear`**: Clears the terminal screen.

- 🚪 **`exit`**: Closes the TLS connection and exits the client safely (automatically rolling back active transactions)
