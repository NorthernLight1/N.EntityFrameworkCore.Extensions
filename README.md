## N.EntityFrameworkCore.Extensions

[![NuGet](https://img.shields.io/nuget/v/N.EntityFrameworkCore.Extensions)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions)
[![Downloads](https://img.shields.io/nuget/dt/N.EntityFrameworkCore.Extensions)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions)

Bulk data extensions for Entity Framework Core. Add high-throughput insert, update, delete, merge, and sync operations to your `DbContext`, plus query-based DML and CSV/JSON export helpers.

| | |
| --- | --- |
| **Operations** | BulkDelete · BulkFetch · BulkInsert · BulkMerge · BulkSaveChanges · BulkSync · BulkUpdate · Fetch · DeleteFromQuery · InsertFromQuery · UpdateFromQuery · QueryToCsvFile · QueryToJsonFile · QueryToJsonLinesFile · SqlQueryToCsvFile · SqlQueryToJsonFile · SqlQueryToJsonLinesFile |
| **Databases** | SQL Server · PostgreSQL · MySQL · SQLite · Oracle |
| **EF Core / .NET** | Entity Framework Core 10 on .NET 10 (`net10.0`) |
| **Also supports** | Multiple schemas · Complex properties · Value converters · Transactions · Sync & async APIs · TPC / TPH / TPT inheritance |
| **License** | [MIT](LICENSE) |

Questions, bugs, and feature requests: [open an issue](https://github.com/NorthernLight1/N.EntityFrameworkCore.Extensions/issues).

---

## Table of Contents

- [Installation](#installation)
- [Setup](#setup)
- [Usage](#usage)
  - [BulkInsert](#bulkinsert)
  - [BulkDelete](#bulkdelete)
  - [BulkFetch](#bulkfetch)
  - [BulkUpdate](#bulkupdate)
  - [BulkMerge](#bulkmerge)
  - [BulkSync](#bulksync)
  - [BulkSaveChanges](#bulksavechanges)
  - [Fetch](#fetch)
  - [DeleteFromQuery](#deletefromquery)
  - [InsertFromQuery](#insertfromquery)
  - [UpdateFromQuery](#updatefromquery)
  - [QueryToCsvFile](#querytocsvfile)
  - [QueryToJsonFile](#querytojsonfile)
  - [QueryToJsonLinesFile](#querytojsonlinesfile)
  - [SqlQueryToCsvFile](#sqlquerytocsvfile)
  - [SqlQueryToJsonFile](#sqlquerytojsonfile)
  - [SqlQueryToJsonLinesFile](#sqlquerytojsonlinesfile)
  - [DbSet Utilities](#dbset-utilities)
  - [FromSqlQuery](#fromsqlquery)
- [Options](#options)
  - [Common Options (BulkOptions)](#common-options-bulkoptions)
  - [BulkInsertOptions](#bulkinsertoptions)
  - [BulkDeleteOptions](#bulkdeleteoptions)
  - [BulkUpdateOptions](#bulkupdateoptions)
  - [BulkMergeOptions](#bulkmergeoptions)
  - [BulkSyncOptions](#bulksyncoptions)
  - [BulkFetchOptions](#bulkfetchoptions)
  - [FetchOptions](#fetchoptions)
  - [QueryToFileOptions](#querytofileoptions)
  - [QueryToJsonFileOptions](#querytojsonfileoptions)
- [Result Objects](#result-objects)
  - [BulkMergeResult](#bulkmergeresult)
  - [BulkMergeOutputRow](#bulkmergeoutputrow)
  - [BulkSyncResult](#bulksyncresult)
  - [FetchResult](#fetchresult)
  - [QueryToFileResult](#querytofileresult)
  - [SqlQuery](#sqlquery)
- [Transactions](#transactions)
- [Provider Notes](#provider-notes)
  - [MySQL](#mysql)
  - [Oracle](#oracle)
- [API Reference](#api-reference)

---

## Installation

### Provider packages (recommended)

Install the package that matches your database:

| Provider | Package |
| --- | --- |
| SQL Server | [![](https://img.shields.io/nuget/v/N.EntityFrameworkCore.Extensions.SqlServer?label=NuGet)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions.SqlServer) [![](https://img.shields.io/nuget/dt/N.EntityFrameworkCore.Extensions.SqlServer?label=Downloads)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions.SqlServer) `dotnet add package N.EntityFrameworkCore.Extensions.SqlServer` |
| PostgreSQL | [![](https://img.shields.io/nuget/v/N.EntityFrameworkCore.Extensions.PostgreSql?label=NuGet)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions.PostgreSql) [![](https://img.shields.io/nuget/dt/N.EntityFrameworkCore.Extensions.PostgreSql?label=Downloads)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions.PostgreSql) `dotnet add package N.EntityFrameworkCore.Extensions.PostgreSql` |
| MySQL | [![](https://img.shields.io/nuget/v/N.EntityFrameworkCore.Extensions.MySql?label=NuGet)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions.MySql) [![](https://img.shields.io/nuget/dt/N.EntityFrameworkCore.Extensions.MySql?label=Downloads)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions.MySql) `dotnet add package N.EntityFrameworkCore.Extensions.MySql` |
| SQLite | [![](https://img.shields.io/nuget/v/N.EntityFrameworkCore.Extensions.Sqlite?label=NuGet)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions.Sqlite) [![](https://img.shields.io/nuget/dt/N.EntityFrameworkCore.Extensions.Sqlite?label=Downloads)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions.Sqlite) `dotnet add package N.EntityFrameworkCore.Extensions.Sqlite` |
| Oracle | [![](https://img.shields.io/nuget/v/N.EntityFrameworkCore.Extensions.Oracle?label=NuGet)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions.Oracle) [![](https://img.shields.io/nuget/dt/N.EntityFrameworkCore.Extensions.Oracle?label=Downloads)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions.Oracle) `dotnet add package N.EntityFrameworkCore.Extensions.Oracle` |

### Meta-package

[`N.EntityFrameworkCore.Extensions`](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions) depends on all provider packages above. Prefer a single provider package when your application targets one database.

```sh
dotnet add package N.EntityFrameworkCore.Extensions
```

---

## Setup

Call `SetupEfCoreExtensions()` in `DbContext.OnConfiguring`. This registers an EF Core `DbCommandInterceptor` used internally by bulk operations. It is required for operations that rewrite table names at execution time (for example `InsertFromQuery` targeting a new table). Other operations work without it, but calling setup is recommended for all providers.

### SQL Server

```csharp
protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
{
    optionsBuilder
        .UseSqlServer("your-connection-string")
        .SetupEfCoreExtensions();
}
```

### PostgreSQL

```csharp
protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
{
    optionsBuilder
        .UseNpgsql("your-connection-string")
        .SetupEfCoreExtensions();
}
```

### MySQL

```csharp
protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
{
    optionsBuilder
        .UseMySql("your-connection-string", ServerVersion.AutoDetect("your-connection-string"))
        .SetupEfCoreExtensions();
}
```

### SQLite

```csharp
protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
{
    optionsBuilder
        .UseSqlite("your-connection-string")
        .SetupEfCoreExtensions();
}
```

### Oracle

```csharp
protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
{
    optionsBuilder
        .UseOracle("your-connection-string")
        .SetupEfCoreExtensions();
}
```

### Test projects

| Provider | Configuration |
| --- | --- |
| SQL Server | `N.EntityFrameworkCore.Extensions.Test/N.EntityFrameworkCore.Extensions.SqlServer.Test/appsettings.json` or `ConnectionStrings__SqlServerTestDatabase` |
| PostgreSQL | `N.EntityFrameworkCore.Extensions.Test/N.EntityFrameworkCore.Extensions.PostgreSql.Test/appsettings.json` or `ConnectionStrings__PostgreSqlTestDatabase` |
| MySQL | `N.EntityFrameworkCore.Extensions.Test/N.EntityFrameworkCore.Extensions.MySql.Test/appsettings.json` or `ConnectionStrings__MySqlTestDatabase` |
| SQLite | `N.EntityFrameworkCore.Extensions.Test/N.EntityFrameworkCore.Extensions.Sqlite.Test/appsettings.json` or `ConnectionStrings__SqliteTestDatabase` (in-memory shared cache by default) |
| Oracle | `N.EntityFrameworkCore.Extensions.Test/N.EntityFrameworkCore.Extensions.Oracle.Test/appsettings.json` or `ConnectionStrings__OracleTestDatabase` |

---

## Usage

### BulkInsert

Inserts a large number of entities in a single bulk operation.

```csharp
var orders = new List<Order>();
for (int i = 0; i < 10000; i++)
{
    orders.Add(new Order { OrderDate = DateTime.UtcNow, TotalPrice = 2.99 });
}
dbContext.BulkInsert(orders);
```

Async:

```csharp
await dbContext.BulkInsertAsync(orders);
```

With options:

```csharp
dbContext.BulkInsert(orders, options =>
{
    options.BatchSize = 5000;
    options.KeepIdentity = true;
    options.InsertIfNotExists = true;
});
```

Progress reporting (all providers):

```csharp
dbContext.BulkInsert(orders, options =>
{
    options.NotifyAfter = 1000;
    options.RowsCopied = e => Console.WriteLine($"Copied {e.RowsCopied} rows");
});
```

**SQL Server — tables with triggers:**

Set `FireTriggers = true` when the target table has `INSERT` triggers so `MERGE … OUTPUT` is skipped while `AutoMapOutput` can still map identity values afterward.

```csharp
dbContext.BulkInsert(products, options =>
{
    options.FireTriggers = true;
});
```

From an async stream (batches internally; default `BatchSize` is `10000` when unset):

```csharp
await dbContext.BulkInsertAsync(ReadOrdersAsync(), options =>
{
    options.BatchSize = 5000;
    options.AutoMapOutput = false;
});

async IAsyncEnumerable<Order> ReadOrdersAsync()
{
    // yield entities from a stream, channel, or remote source
    yield break;
}
```

### BulkDelete

Deletes a large number of entities in a single bulk operation.

```csharp
var orders = dbContext.Orders.Where(o => o.TotalPrice < 5.35M).ToList();
dbContext.BulkDelete(orders);
```

Async:

```csharp
await dbContext.BulkDeleteAsync(orders);
```

With options (custom match condition):

```csharp
dbContext.BulkDelete(orders, options =>
{
    options.DeleteOnCondition = (s, t) => s.Id == t.Id;
});
```

Soft delete (updates columns from entity values instead of deleting rows):

```csharp
foreach (var order in orders)
    order.IsDeleted = true;

dbContext.BulkDelete(orders, options =>
{
    options.SoftDeleteColumns = x => new { x.IsDeleted };
});
```

### BulkFetch

Retrieves entities from the database that match a local list of entity stubs (useful for key-based lookups). Pass objects with the key properties populated — not bare primitive IDs.

```csharp
var stubs = new List<Product>
{
    new() { Id = 10001 },
    new() { Id = 10002 },
    new() { Id = 10003 },
};
var products = dbContext.Products.BulkFetch(stubs).ToList();
```

Async, with a custom join condition:

```csharp
var stubs = orders.Select(o => new Order { ExternalId = o.ExternalId }).ToList();
var fetched = await dbContext.Orders.BulkFetchAsync(stubs, options =>
{
    options.JoinOnCondition = (s, t) => s.ExternalId == t.ExternalId;
});
```

Attach fetched entities to the change tracker:

```csharp
var products = dbContext.Products
    .BulkFetch(stubs, options => { options.AttachToContext = true; })
    .ToList();
```

### BulkUpdate

Updates a large number of entities in a single bulk operation.

```csharp
var products = dbContext.Products.Where(o => o.Price < 5.35M).ToList();
foreach (var product in products)
{
    product.Price = 6M;
}
dbContext.BulkUpdate(products);
```

Async:

```csharp
await dbContext.BulkUpdateAsync(products);
```

With options (update only specific columns):

```csharp
dbContext.BulkUpdate(products, options =>
{
    options.InputColumns = o => new { o.Price };
});
```

### BulkMerge

Inserts new entities and updates existing ones in a single bulk operation (upsert).

```csharp
var products = new List<Product>();

var existingProducts = dbContext.Products.Where(o => o.Price < 5.35M).ToList();
foreach (var product in existingProducts)
{
    product.Price = 6M;
}
products.AddRange(existingProducts);
products.Add(new Product { Name = "Hat", Price = 10.25M });
products.Add(new Product { Name = "Shirt", Price = 20.95M });

BulkMergeResult<Product> result = dbContext.BulkMerge(products);
Console.WriteLine($"Inserted: {result.RowsInserted}, Updated: {result.RowsUpdated}");
```

Async:

```csharp
var result = await dbContext.BulkMergeAsync(products);
```

With options (custom match condition and ignore columns):

```csharp
var result = dbContext.BulkMerge(products, options =>
{
    options.MergeOnCondition = (s, t) => s.Id == t.Id;
    options.IgnoreColumnsOnInsert = o => new { o.CreatedDate };
    options.IgnoreColumnsOnUpdate = o => new { o.CreatedDate };
});
```

**PostgreSQL, MySQL, and SQLite — native upsert:**

Uses a single native upsert statement matched on the primary key when one is present. Custom `MergeOnCondition` is not applied in this mode. `AutoMapOutput` remains supported; set it to `false` if you do not need generated values mapped back.

```csharp
var result = dbContext.BulkMerge(products, options =>
{
    options.UseNativeUpsert = true;
});
```

### BulkSync

Synchronizes the target table with the provided list. Rows in the database that are not present in the source list are removed (hard delete by default).

```csharp
var products = new List<Product>();

var existingProducts = dbContext.Products.Where(o => o.Id <= 1000).ToList();
foreach (var product in existingProducts)
{
    product.Price = 6M;
}
products.AddRange(existingProducts);
products.Add(new Product { Name = "Hat", Price = 10.25M });
products.Add(new Product { Name = "Shirt", Price = 20.95M });

// Existing products with Id > 1000 are deleted
BulkSyncResult<Product> result = dbContext.BulkSync(products);
Console.WriteLine($"Inserted: {result.RowsInserted}, Updated: {result.RowsUpdated}, Deleted: {result.RowsDeleted}");
```

Async:

```csharp
var result = await dbContext.BulkSyncAsync(products);
```

With options (custom match condition):

```csharp
var result = dbContext.BulkSync(products, options =>
{
    options.MergeOnCondition = (s, t) => s.Id == t.Id;
    options.IgnoreColumnsOnUpdate = o => new { o.CreatedDate };
});
```

Soft-delete unmatched rows instead of removing them (constant values in the expression):

```csharp
dbContext.BulkSync(orders, options =>
{
    options.SoftDeleteColumns = x => new { IsDeleted = true };
});
```

### BulkSaveChanges

A high-performance replacement for `SaveChanges()`. Processes all pending changes using bulk operations.

```csharp
var orders = new List<Order>();
for (int i = 0; i < 10000; i++)
{
    orders.Add(new Order { Id = -i, OrderDate = DateTime.UtcNow, TotalPrice = 2.99 });
}
dbContext.Orders.AddRange(orders);
dbContext.BulkSaveChanges();
```

Async:

```csharp
await dbContext.BulkSaveChangesAsync();
```

### Fetch

Retrieves query results in batches, processing each batch with a callback. Useful for large result sets that should not be loaded into memory all at once.

```csharp
var query = dbContext.Products.Where(o => o.Price < 5.35M);
int batchCount = 0;
int totalCount = 0;

query.Fetch(result =>
{
    batchCount++;
    totalCount += result.Results.Count;
},
new FetchOptions<Product> { BatchSize = 1000 });

Console.WriteLine($"Fetched {totalCount} products in {batchCount} batches.");
```

Async:

```csharp
await query.FetchAsync(async result =>
{
    await ProcessBatchAsync(result.Results);
},
new FetchOptions<Product> { BatchSize = 1000 });
```

### DeleteFromQuery

Deletes rows directly in the database using a LINQ query, without loading entities into the `DbContext`.

```csharp
// Delete all products
dbContext.Products.DeleteFromQuery();

// Delete all products priced under $5.35
dbContext.Products.Where(x => x.Price < 5.35M).DeleteFromQuery();

// With a custom command timeout (seconds)
dbContext.Products.Where(x => x.Price < 5.35M).DeleteFromQuery(commandTimeout: 120);
```

Async:

```csharp
await dbContext.Products.Where(x => x.Price < 5.35M).DeleteFromQueryAsync();
```

### InsertFromQuery

Inserts rows into a target table by selecting from a LINQ query. The query results are loaded client-side (`AsNoTracking`) and then bulk-inserted into the destination table (unlike `DeleteFromQuery` / `UpdateFromQuery`, which run entirely on the server).

```csharp
// Copy all products priced under $10 into a separate table
dbContext.Products
    .Where(x => x.Price < 10M)
    .InsertFromQuery("ProductsUnderTen", o => new { o.Id, o.Price });

// With a custom command timeout (seconds)
dbContext.Products
    .Where(x => x.Price < 10M)
    .InsertFromQuery("ProductsUnderTen", o => new { o.Id, o.Price }, commandTimeout: 120);
```

Async:

```csharp
await dbContext.Products
    .Where(x => x.Price < 10M)
    .InsertFromQueryAsync("ProductsUnderTen", o => new { o.Id, o.Price });
```

### UpdateFromQuery

Updates rows directly in the database using a LINQ query, without loading entities into the `DbContext`.

```csharp
// Change all products priced at $5.35 to $5.75
dbContext.Products
    .Where(x => x.Price == 5.35M)
    .UpdateFromQuery(o => new Product { Price = 5.75M });

// With a custom command timeout (seconds)
dbContext.Products
    .Where(x => x.Price == 5.35M)
    .UpdateFromQuery(o => new Product { Price = 5.75M }, commandTimeout: 120);
```

Async:

```csharp
await dbContext.Products
    .Where(x => x.Price == 5.35M)
    .UpdateFromQueryAsync(o => new Product { Price = 5.75M });
```

### QueryToCsvFile

Exports LINQ query results to a CSV file or stream.

```csharp
// Export to file
QueryToFileResult result = dbContext.Products
    .Where(x => x.Price > 5M)
    .QueryToCsvFile("products.csv");

Console.WriteLine($"Rows written: {result.DataRowCount}");
```

```csharp
// Export to stream with options
using var stream = File.OpenWrite("products.csv");
await dbContext.Products.QueryToCsvFileAsync(stream, options =>
{
    options.IncludeHeaderRow = true;
    options.ColumnDelimiter = ";";
});
```

### SqlQueryToCsvFile

Exports the results of a raw SQL query to a CSV file or stream.

```csharp
QueryToFileResult result = dbContext.Database
    .SqlQueryToCsvFile("output.csv", "SELECT Id, Name, Price FROM Products WHERE Price > @p0", 5M);
```

Async:

```csharp
QueryToFileResult result = await dbContext.Database
    .SqlQueryToCsvFileAsync("output.csv", "SELECT Id, Name FROM Products", Array.Empty<object>());
```

### QueryToJsonFile

Exports LINQ query results to a JSON file or stream as a JSON array.

```csharp
QueryToFileResult result = dbContext.Products
    .Where(x => x.Price > 5M)
    .QueryToJsonFile("products.json");

Console.WriteLine($"Rows written: {result.DataRowCount}");
```

```csharp
using var stream = File.OpenWrite("products.json");
await dbContext.Products.QueryToJsonFileAsync(stream, options =>
{
    options.Indented = true;
});
```

### QueryToJsonLinesFile

Exports query results as newline-delimited JSON (NDJSON) — one JSON object per line. Better suited than `QueryToJsonFile` for very large exports because the file is not wrapped in a single JSON array.

```csharp
QueryToFileResult result = dbContext.Products
    .Where(x => x.Price > 5M)
    .QueryToJsonLinesFile("products.jsonl");

await dbContext.Products.QueryToJsonLinesFileAsync("products.jsonl");
```

### SqlQueryToJsonFile

Exports the results of a raw SQL query to a JSON file or stream.

```csharp
QueryToFileResult result = dbContext.Database
    .SqlQueryToJsonFile("output.json", "SELECT Id, Name, Price FROM Products WHERE Price > @p0", 5M);
```

Async:

```csharp
QueryToFileResult result = await dbContext.Database
    .SqlQueryToJsonFileAsync("output.json", "SELECT Id, Name FROM Products", Array.Empty<object>());
```

### SqlQueryToJsonLinesFile

Same as `SqlQueryToJsonFile`, but writes NDJSON (one object per line).

```csharp
QueryToFileResult result = dbContext.Database
    .SqlQueryToJsonLinesFile("output.jsonl", "SELECT Id, Name, Price FROM Products WHERE Price > @p0", 5M);
```

### DbSet Utilities

**Clear** — deletes all rows in the table (equivalent to `DELETE FROM`):

```csharp
dbContext.Orders.Clear();
await dbContext.Orders.ClearAsync();
```

**Truncate** — truncates the table (faster than `Clear` on most providers). Identity/sequence reset depends on the database (for example PostgreSQL uses `TRUNCATE … RESTART IDENTITY`; SQLite falls back to `DELETE FROM` and does not reset `AUTOINCREMENT`).

```csharp
dbContext.Orders.Truncate();
await dbContext.Orders.TruncateAsync();
```

### FromSqlQuery

Creates a `SqlQuery` for counting or executing raw SQL without materializing entities:

```csharp
var sqlQuery = dbContext.Database.FromSqlQuery("SELECT * FROM Products WHERE Price > @p0", 5M);
int count = sqlQuery.Count();
```

---

## Options

### Common Options (BulkOptions)

All bulk operations accept options that derive from `BulkOptions`:

| Property | Type | Description |
| --- | --- | --- |
| `BatchSize` | `int` | Number of rows per batch. Defaults to `0` (provider/driver default). For `BulkInsertAsync(IAsyncEnumerable<T>)`, defaults to `10000` when unset. |
| `CommandTimeout` | `int?` | SQL command timeout in seconds. |
| `UsePermanentTable` | `bool` | Use a permanent staging table instead of a temporary one. |
| `NotifyAfter` | `int` | Number of rows processed before raising a progress callback. Use with `RowsCopied`. Available on all providers; on SQL Server these also drive `SqlBulkCopy` progress. |
| `RowsCopied` | `Action<BulkRowsCopiedEventArgs>` | Progress callback invoked when `NotifyAfter` rows have been processed. |
| `BulkCopyOptions` | `SqlBulkCopyOptions` | **SQL Server only.** Flags passed to `SqlBulkCopy` (for example `TableLock`, `CheckConstraints`). Prefer `FireTriggers` for trigger-enabled tables. |
| `FireTriggers` | `bool` | **SQL Server only.** When `true`, skips `MERGE … OUTPUT` so inserts/merges work with tables that have `INSERT` triggers while still supporting `AutoMapOutput` (identity values are mapped afterward when needed). Target triggers still fire from the `MERGE` itself. |
| `EnableStreaming` | `bool` | **SQL Server only.** Enables `SqlBulkCopy.EnableStreaming` for streaming from the data reader during large loads. |
| `ColumnOrderHints` | `SqlBulkCopyColumnOrderHintCollection` | **SQL Server only.** Column sort-order hints for bulk load (useful when loading into a clustered index in key order). |
| `SqlRowsCopied` | `SqlRowsCopiedEventHandler` | **SQL Server only.** Native `SqlBulkCopy.SqlRowsCopied` event handler. |

### BulkInsertOptions

| Property | Type | Description |
| --- | --- | --- |
| `AutoMapOutput` | `bool` | Map database-generated values (e.g. identity keys) back to entities. Default: `true`. |
| `IgnoreColumns` | `Expression<Func<T, object>>` | Columns to exclude from the insert. |
| `IncludeGraph` | `bool` | When `true`, also inserts related entities from navigation properties and assigns foreign keys from mapped primary keys. Default: `false`. |
| `InputColumns` | `Expression<Func<T, object>>` | Columns to include in the insert (all others are excluded). |
| `InsertIfNotExists` | `bool` | Skip rows that already exist in the target table. Default: `false`. |
| `InsertOnCondition` | `Expression<Func<T, T, bool>>` | Custom condition used to determine whether a row already exists. |
| `KeepIdentity` | `bool` | Preserve source identity values instead of letting the database generate them. Default: `false`. |

`BulkInsertAsync` also accepts `IAsyncEnumerable<T>` and batches internally (default batch size `10000`, overridable via `BatchSize`).

Columns configured with `HasDefaultValue` / `HasDefaultValueSql` are included in bulk insert and merge payloads so explicit entity values are written. Computed columns remain excluded. Identity primary keys are still omitted unless `KeepIdentity` is enabled.

### BulkDeleteOptions

| Property | Type | Description |
| --- | --- | --- |
| `DeleteOnCondition` | `Expression<Func<T, T, bool>>` | Custom condition used to match rows for deletion. |
| `SoftDeleteColumns` | `Expression<Func<T, object>>` | When set, performs an `UPDATE` using values from each entity instead of a physical `DELETE`. |

### BulkUpdateOptions

| Property | Type | Description |
| --- | --- | --- |
| `InputColumns` | `Expression<Func<T, object>>` | Columns to update (all others are excluded). |
| `IgnoreColumns` | `Expression<Func<T, object>>` | Columns to exclude from the update. |
| `UpdateOnCondition` | `Expression<Func<T, T, bool>>` | Custom condition used to match rows for updating. |
| `IncludeGraph` | `bool` | When `true`, also updates related entities from navigation properties. Default: `false`. |

### BulkMergeOptions

| Property | Type | Description |
| --- | --- | --- |
| `MergeOnCondition` | `Expression<Func<T, T, bool>>` | Custom condition used to match source and target rows. |
| `IgnoreColumnsOnInsert` | `Expression<Func<T, object>>` | Columns to exclude when inserting new rows. |
| `IgnoreColumnsOnUpdate` | `Expression<Func<T, object>>` | Columns to exclude when updating existing rows. |
| `InputColumnsOnInsert` | `Expression<Func<T, object>>` | Columns to include when inserting new rows. |
| `InputColumnsOnUpdate` | `Expression<Func<T, object>>` | Columns to include when updating existing rows. |
| `AutoMapOutput` | `bool` | Map database-generated values back to entities after the merge. Default: `true`. |
| `IncludeGraph` | `bool` | When `true`, also merges related entities from navigation properties. Default: `false`. |
| `UseNativeUpsert` | `bool` | **PostgreSQL, MySQL, and SQLite only.** Uses a single native upsert (`ON CONFLICT` / `ON DUPLICATE KEY UPDATE`) instead of separate insert/update statements. **BulkMerge only** (ignored by `BulkSync`). Requires a primary key. Conflict matching uses the primary key — custom `MergeOnCondition` is not applied when native upsert is enabled. |
| `SoftDeleteColumns` | `Expression<Func<T, object>>` | Used by **BulkSync**: updates unmatched rows with constant values from the expression instead of deleting them. Ignored by `BulkMerge`. |

### BulkSyncOptions

Inherits `BulkMergeOptions`. `BulkSync` always removes unmatched target rows (hard delete), or soft-deletes them when `SoftDeleteColumns` is set. Use `BulkMerge` when unmatched rows should remain unchanged. `UseNativeUpsert` is ignored for `BulkSync` (native upsert only applies to `BulkMerge`).

### BulkFetchOptions

| Property | Type | Description |
| --- | --- | --- |
| `JoinOnCondition` | `Expression<Func<T, T, bool>>` | Condition used to join the local stubs to the database table. Defaults to primary-key equality when unset. |
| `InputColumns` | `Expression<Func<T, object>>` | Columns to select from the **target table** into the returned entities. |
| `IgnoreColumns` | `Expression<Func<T, object>>` | Columns to exclude from the returned entities. |
| `AttachToContext` | `bool` | When `true`, attaches fetched entities to the `DbContext` with `EntityState.Unchanged`. Default: `false`. |

### FetchOptions

| Property | Type | Description |
| --- | --- | --- |
| `BatchSize` | `int` | Number of rows to retrieve per batch. When `0` or unset, the entire result set is returned in a single batch. |
| `InputColumns` | `Expression<Func<T, object>>` | Columns to select. |
| `IgnoreColumns` | `Expression<Func<T, object>>` | Columns to exclude from results. |

### QueryToFileOptions

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `ColumnDelimiter` | `string` | `","` | Column separator character. |
| `RowDelimiter` | `string` | `"\r\n"` | Row separator sequence. |
| `IncludeHeaderRow` | `bool` | `true` | Write a header row with column names. |
| `TextQualifer` | `string` | `""` | Character used to wrap field values (e.g. `"`). Property name matches the public API spelling (`TextQualifer`). |
| `CommandTimeout` | `int?` | `null` | SQL command timeout in seconds. |

### QueryToJsonFileOptions

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `Indented` | `bool` | `true` | Write formatted JSON with indentation for array export. NDJSON (`QueryToJsonLinesFile`) always writes compact objects per line. |
| `CommandTimeout` | `int?` | `null` | SQL command timeout in seconds (for SQL-based JSON export methods). |

---

## Result Objects

### BulkMergeResult

Returned by `BulkMerge` and `BulkMergeAsync` (`BulkMergeResult<T>`).

| Property | Type | Description |
| --- | --- | --- |
| `RowsAffected` | `int` | Total number of rows affected. |
| `RowsInserted` | `int` | Number of rows inserted. |
| `RowsUpdated` | `int` | Number of rows updated. |
| `RowsDeleted` | `int` | Number of rows deleted (typically populated by `BulkSync`). |
| `Output` | `IEnumerable<BulkMergeOutputRow<T>>` | Per-row output with merge action details. |

### BulkMergeOutputRow

Each element in `BulkMergeResult<T>.Output` (`BulkMergeOutputRow<T>`).

| Property | Type | Description |
| --- | --- | --- |
| `Action` | `string` | The merge action performed: `"INSERT"`, `"UPDATE"`, or `"DELETE"`. |

```csharp
var result = dbContext.BulkMerge(products);
foreach (var row in result.Output)
{
    Console.WriteLine(row.Action);
}
```

### BulkSyncResult

Inherits `BulkMergeResult<T>`. Returned by `BulkSync` and `BulkSyncAsync`. `RowsDeleted` reflects unmatched rows that were removed or soft-deleted.

### FetchResult

Passed to the callback in `Fetch` / `FetchAsync` (`FetchResult<T>`).

| Property | Type | Description |
| --- | --- | --- |
| `Results` | `List<T>` | Entities in the current batch. |
| `Batch` | `int` | Current batch number (1-based). |

### QueryToFileResult

Returned by CSV and JSON export methods (LINQ and SQL, sync and async).

| Property | Type | Description |
| --- | --- | --- |
| `DataRowCount` | `int` | Number of data rows written. |
| `TotalRowCount` | `int` | Total rows written. For CSV, includes the header row when present. For JSON/NDJSON, equals `DataRowCount`. |
| `BytesWritten` | `long` | Bytes written to the file or stream. |

### SqlQuery

Returned by `DatabaseFacade.FromSqlQuery(...)`. Supports counting or executing raw SQL without materializing entities.

| Member | Description |
| --- | --- |
| `Count()` | Returns the number of rows matched by the query. |
| `CountAsync(cancellationToken)` | Async version of `Count()`. |
| `ExecuteNonQuery()` | Executes the SQL statement and returns the number of rows affected. |
| `SqlText` | The SQL text of the query. |
| `Parameters` | The parameters passed to the query. |

```csharp
var sqlQuery = dbContext.Database.FromSqlQuery("SELECT * FROM Products WHERE Price > @p0", 5M);
int count = sqlQuery.Count();
```

---

## Transactions

Bulk operations enlist in the ambient EF Core transaction when one is active:

```csharp
using var transaction = dbContext.Database.BeginTransaction();
try
{
    dbContext.BulkInsert(orders);
    dbContext.BulkUpdate(products);
    transaction.Commit();
}
catch
{
    transaction.Rollback();
    throw;
}
```

---

## Provider Notes

### MySQL

#### Bulk insert and local infile

For best bulk-insert throughput, enable MySQL local infile on both the client and server. The provider uses `MySqlBulkCopy` when possible and falls back to batched inserts when local infile is unavailable.

```csharp
optionsBuilder
    .UseMySql("your-connection-string;AllowLoadLocalInfile=true", ServerVersion.AutoDetect("your-connection-string"))
    .SetupEfCoreExtensions();
```

The MySQL server must also have `local_infile=1` enabled. See the [MySQL provider README](N.EntityFrameworkCore.Extensions.MySql/README.md) for details.

#### InsertFromQuery and transactions

On MySQL, `InsertFromQuery` / `InsertFromQueryAsync` create the destination table with DDL when it does not already exist. MySQL auto-commits around DDL, so the library **throws** `InvalidOperationException` if a user transaction is open and the destination table must be created.

```csharp
// Throws when ProductsUnderTen does not exist yet
using var transaction = dbContext.Database.BeginTransaction();
dbContext.Products
    .Where(x => x.Price < 10M)
    .InsertFromQuery("ProductsUnderTen", o => new { o.Id, o.Price });
```

If the destination table already exists, `InsertFromQuery` can run inside a transaction (data is bulk-inserted into the existing table). Create the table outside a transaction first when you need both DDL and transactional inserts, or use `BulkInsert` when transactional safety for new tables is required.

### Oracle

Oracle support is provided by [`N.EntityFrameworkCore.Extensions.Oracle`](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions.Oracle). Notable differences:

- **`FireTriggers`** — Not supported (no SQL Server `SqlBulkCopy` equivalent).
- **`MergeOnCondition`** — Rendered into Oracle `MERGE ... ON (...)`. Columns used in the ON predicate are excluded from `UPDATE SET` (ORA-38104).
- **Staging tables** — Global temporary staging tables may not be dropped while a caller transaction is open; permanent staging (`UsePermanentTable = true`) is dropped after the operation.

See the [Oracle provider README](N.EntityFrameworkCore.Extensions.Oracle/README.md#provider-notes) for additional details.

---

## API Reference

### DbContext Extensions

| Method | Description |
| --- | --- |
| **BulkDelete** | |
| `BulkDelete<T>(items)` | Bulk delete entities. |
| `BulkDelete<T>(items, options)` | Bulk delete entities with options. |
| `BulkDeleteAsync<T>(items, cancellationToken)` | Async bulk delete. |
| `BulkDeleteAsync<T>(items, options, cancellationToken)` | Async bulk delete with options. |
| **BulkInsert** | |
| `BulkInsert<T>(items)` | Bulk insert entities. |
| `BulkInsert<T>(items, options)` | Bulk insert entities with options. |
| `BulkInsertAsync<T>(items, cancellationToken)` | Async bulk insert. |
| `BulkInsertAsync<T>(items, options, cancellationToken)` | Async bulk insert with options. |
| `BulkInsertAsync<T>(IAsyncEnumerable<T>, cancellationToken)` | Async bulk insert from a streaming source (batched). |
| `BulkInsertAsync<T>(IAsyncEnumerable<T>, options, cancellationToken)` | Async streaming bulk insert with options. |
| **BulkMerge** | |
| `BulkMerge<T>(items)` | Bulk merge (upsert). Returns `BulkMergeResult<T>`. |
| `BulkMerge<T>(items, options)` | Bulk merge with options. |
| `BulkMergeAsync<T>(items, cancellationToken)` | Async bulk merge. |
| `BulkMergeAsync<T>(items, options, cancellationToken)` | Async bulk merge with options. |
| **BulkSaveChanges** | |
| `BulkSaveChanges()` | Save pending changes using bulk operations. Returns `int`. |
| `BulkSaveChanges(acceptAllChangesOnSuccess)` | Save changes, controlling whether `AcceptAllChanges` is called. Returns `int`. |
| `BulkSaveChangesAsync()` | Async bulk save changes. Returns `Task<int>`. Does not accept `CancellationToken`. |
| `BulkSaveChangesAsync(acceptAllChangesOnSuccess)` | Async bulk save changes with option. Returns `Task<int>`. |
| **BulkSync** | |
| `BulkSync<T>(items)` | Sync entities — insert/update/delete to match source list. Returns `BulkSyncResult<T>`. |
| `BulkSync<T>(items, options)` | Bulk sync with options. |
| `BulkSyncAsync<T>(items, cancellationToken)` | Async bulk sync. |
| `BulkSyncAsync<T>(items, options, cancellationToken)` | Async bulk sync with options. |
| **BulkUpdate** | |
| `BulkUpdate<T>(items)` | Bulk update entities. |
| `BulkUpdate<T>(items, options)` | Bulk update entities with options. |
| `BulkUpdateAsync<T>(items, cancellationToken)` | Async bulk update. |
| `BulkUpdateAsync<T>(items, options, cancellationToken)` | Async bulk update with options. |

### DbSet Extensions

| Method | Description |
| --- | --- |
| **BulkFetch** | |
| `BulkFetch<T, U>(items)` | Retrieve entities matching a local list. |
| `BulkFetch<T, U>(items, options)` | Retrieve entities matching a local list with options. |
| `BulkFetchAsync<T, U>(items, cancellationToken)` | Async retrieve entities matching a local list. |
| `BulkFetchAsync<T, U>(items, options, cancellationToken)` | Async retrieve with options. |
| **Utilities** | |
| `Clear<T>()` | Delete all rows from the table. |
| `ClearAsync<T>(cancellationToken)` | Async delete all rows. |
| `Truncate<T>()` | Truncate the table. |
| `TruncateAsync<T>(cancellationToken)` | Async truncate the table. |

### IQueryable Extensions

| Method | Description |
| --- | --- |
| **Fetch** | |
| `Fetch<T>(Action<FetchResult<T>>, options)` | Fetch rows in batches and process each batch via a callback. |
| `FetchAsync<T>(Func<FetchResult<T>, Task>, options, cancellationToken)` | Async batch fetch. |
| **DeleteFromQuery** | |
| `DeleteFromQuery<T>(commandTimeout?)` | Delete matching rows without loading them. |
| `DeleteFromQueryAsync<T>(commandTimeout?, cancellationToken)` | Async delete from query. |
| **InsertFromQuery** | |
| `InsertFromQuery<T>(tableName, selectExpression, commandTimeout?)` | Insert query results into another table. |
| `InsertFromQueryAsync<T>(tableName, selectExpression, commandTimeout?, cancellationToken)` | Async insert from query. |
| **UpdateFromQuery** | |
| `UpdateFromQuery<T>(updateExpression, commandTimeout?)` | Update matching rows without loading them. |
| `UpdateFromQueryAsync<T>(updateExpression, commandTimeout?, cancellationToken)` | Async update from query. |
| **QueryToCsvFile** | |
| `QueryToCsvFile<T>(filePath \| stream, options?)` | Export query results to CSV. |
| `QueryToCsvFileAsync<T>(...)` | Async CSV export. |
| **QueryToJsonFile** | |
| `QueryToJsonFile<T>(filePath \| stream, options?)` | Export query results as a JSON array. |
| `QueryToJsonFileAsync<T>(...)` | Async JSON array export. |
| **QueryToJsonLinesFile** | |
| `QueryToJsonLinesFile<T>(filePath \| stream, options?)` | Export query results as NDJSON. |
| `QueryToJsonLinesFileAsync<T>(...)` | Async NDJSON export. |

### DatabaseFacade Extensions

| Method | Description |
| --- | --- |
| `FromSqlQuery(sqlText, parameters)` | Create a `SqlQuery` for counting or executing raw SQL. |
| `SqlQueryToCsvFile(...)` / `SqlQueryToCsvFileAsync(...)` | Export raw SQL results to CSV. |
| `SqlQueryToJsonFile(...)` / `SqlQueryToJsonFileAsync(...)` | Export raw SQL results as a JSON array. |
| `SqlQueryToJsonLinesFile(...)` / `SqlQueryToJsonLinesFileAsync(...)` | Export raw SQL results as NDJSON. |
| `ClearTable(tableName)` / `ClearTableAsync(...)` | Delete all rows from a table by name. |
| `TruncateTable(tableName, ifExists)` / `TruncateTableAsync(...)` | Truncate a table by name. |
| `DropTable(tableName, ifExists)` | Drop a table by name. |
| `DropTable(tableName, ifExists, isTemporary)` | **MySQL only.** Drop a table; set `isTemporary` when dropping a temporary table. |
| `TableExists(tableName)` | Returns `true` if the table exists. |
| `TableHasIdentity(tableName)` | Returns `true` if the table has an identity column. |
