# 🚀 LunaDB CLI Client Documentation 🚀

The `lunadb-client` is a command-line interface (CLI) for direct, secure interaction with the `lunadb-server` via its custom TLS-encrypted TCP protocol. Designed for speed, security, and scalability.

## ▶️ How to Run

To start the client, you must provide the address of the `lunadb-server`. You can also include credentials for automatic login using flags.

**Locally:**

> `./bin/lunadb-client`
>
> `./bin/lunadb-client -u admin -p adminpass localhost:5876`

**Docker 🐳:**

> `sudo docker exec -it <containerId> lunadb-client -u root -p rootpass localhost:5876`

_Important for Testing: To pass JSON files instead of raw strings, ensure you have a `json/` directory in your working path (e.g., `./json/payload.json`). The CLI will auto-resolve filenames passed as arguments._

---

## 👥 User and Permission Management (Admins)

Authentication is required to execute most commands. User and permission management requires special privileges (Root or Write access to the `_system` collection).

- 🔐 **`login <username> <password>`**: Authenticates the connection with the server.

- ➕ **`user create <username> <password> <permissions_json|path>`**: Creates a new user. Example: `user create salesuser pass123 {"sales":"write", "products":"read"}`

- 🔄 **`user update <username> <permissions_json|path>`**: Completely replaces an existing user's permissions. Example: `user update salesuser {"*":"read"}`

- 🗑️ **`user delete <username>`**: Permanently deletes a user from the system.

- 🔑 **`update password <target_username> <new_password>`**: Updates a user's password. The `root` user can change anyone's password.

---

## 👑 Main Store Commands (Root Only)

These commands operate on the primary key-value store bypassing collections. Available **only to the `root` user**.

- 💾 **`set <key> <value_json> [ttl_seconds]`**: Sets a raw key-value pair. Test TTL expiration by appending seconds at the end.

- 📥 **`get <key>`**: Retrieves the value associated with a key from the main store.

---

## 🛡️ Admin & Maintenance (Root Only)

Low-level administrative operations.

- 📦 **`backup`**: Triggers a full, manual backup of all server data immediately to the `backups/` directory.

- 🔙 **`restore <backup_directory_name>`**: **Destructive Action!** Restores the entire server state from a specific backup directory.

---

## 📦 Transactions

LunaDB supports ACID-like transactions. Group multiple write operations (`set`, `update`, `delete`) and execute them atomically.

- **`begin`**: Starts a new transaction block. The prompt will show `[TX]`. Read commands are disabled in this mode.

- **`commit`**: Atomically applies all queued commands. Validates existence locks before applying.

- **`rollback`**: Discards all queued commands and releases key locks safely.

---

## 🗂️ Collection Commands

### Collection Management

- ✨ **`collection create <collection_name>`**: Initializes a new collection.

- 🔥 **`collection delete <collection_name>`**: Drops the collection and enqueues async deletion of its disk files.

- 📜 **`collection list`**: Lists all collections you have read access to.

### 📄 Collection Item Operations

_The `<value_json>` or `<patch_json>` can be a raw string or a file name inside the `json/` directory (e.g., `item.json`)._

- ✅ **`collection item set <collection> [<key>] <value_json|path> [ttl]`**: Saves an item. Omitting the key auto-generates a UUID. Example: `collection item set products laptop-01 {"name": "Pro"} 3600`

- 📤 **`collection item get <collection> <key>`**: Gets an item by its key.

- ✍️ **`collection item update <collection> <key> <patch_json|path>`**: Partially updates an item with the fields from the patch.

- 🗑️ **`collection item delete <collection> <key>`**: Deletes an item by its key (or tombstone it if it's in cold storage).

- 📋 **`collection item list <collection>`**: **(Root only)** Lists all items in the specified collection.

### ⚡ Batch Operations (Stress Testing)

- **`collection item set many <collection> <json_array|path>`**: Inserts multiple items at once. Skips existing keys. Example: `collection item set many products batch_insert.json`

- **`collection item update many <collection> <patch_json_array|path>`**: Applies patches to multiple items. Payload must be an array of `{"_id": "...", "patch": {...}}`.

- **`collection item delete many <collection> <keys_json_array|path>`**: Deletes multiple items provided in a JSON array of string keys.

---

## 🔍 Index Commands

Test performance differences by running queries before and after creating indexes.

- 📈 **`collection index create <collection> <field_name>`**: Builds a B-Tree index for a specific field.

- 📜 **`collection index list <collection>`**: Lists all active indexes on the collection.

- 🔥 **`collection index delete <collection> <field_name>`**: Removes an existing index.

---

## ❓ Collection Query Command

Execute complex queries with filtering, sorting, joins, and aggregations using zero-copy inspection and mmap for cold data.

- **`collection query <collection> <query_json|path>`**: Executes a query. Example: `collection query products query1.json`

### Query JSON Structure

|**Key**|**Type**|**Description**|
|---|---|---|
|`filter`|object|Conditions to select items (`WHERE` clause).|
|`order_by`|array|Sorts the results (`[{"field": "price", "direction": "desc"}]`).|
|`limit`|number|Restricts the number of results (Optimized via Heap Sort).|
|`offset`|number|Skips results, used for pagination.|
|`count`|boolean|Returns a count of matching items.|
|`distinct`|string|Returns unique values for a field.|
|`group_by`|array|Groups results for aggregation.|
|`aggregations`|object|Defines functions like `sum`, `avg`, `min`, `max`, `count`.|
|`having`|object|Filters results after aggregation.|
|`projection`|array|Selects which fields to return.|
|`lookups`|array|Joins data from other collections (In-Memory Hash Join).|

### Supported Filter Operators

|**Operator**|**Syntax**|**Description**|
|---|---|---|
|**Comparisons**|`=`, `!=`, `>`, `>=`, `<`, `<=`|Standard numeric and string comparisons.|
|**Pattern**|`like`|SQL-like pattern matching (`%` for wildcards).|
|**Inclusion**|`in`, `between`|Check array inclusion or numeric/string ranges.|
|**Nullity**|`is null`, `is not null`|Checks for the existence or absence of a field.|
|**Logical**|`and`, `or`, `not`|Combine multiple filter objects.|

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

- ℹ️ **`help`**: Displays the list of available commands.

- 💨 **`clear`**: Clears the terminal screen.

- 🚪 **`exit`**: Closes the connection and exits the client safely.
