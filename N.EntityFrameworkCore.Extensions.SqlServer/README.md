## N.EntityFrameworkCore.Extensions.SqlServer

[![NuGet](https://img.shields.io/nuget/v/N.EntityFrameworkCore.Extensions.SqlServer)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions.SqlServer)
[![Downloads](https://img.shields.io/nuget/dt/N.EntityFrameworkCore.Extensions.SqlServer)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions.SqlServer)

Bulk data extensions for Entity Framework Core — **SQL Server** provider.

| | |
| --- | --- |
| **Operations** | BulkDelete · BulkFetch · BulkInsert · BulkMerge · BulkSaveChanges · BulkSync · BulkUpdate · Fetch · DeleteFromQuery · InsertFromQuery · UpdateFromQuery · QueryToCsvFile · QueryToJsonFile · QueryToJsonLinesFile · SqlQueryToCsvFile · SqlQueryToJsonFile · SqlQueryToJsonLinesFile |
| **Also supports** | Multiple schemas · Complex properties · Value converters · Transactions · Sync & async APIs · TPC / TPH / TPT inheritance |

Questions, bugs, and feature requests: [open an issue](https://github.com/NorthernLight1/N.EntityFrameworkCore.Extensions/issues).

---

## Installation

```sh
dotnet add package N.EntityFrameworkCore.Extensions.SqlServer
```

The meta-package [`N.EntityFrameworkCore.Extensions`](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions) also installs this provider along with PostgreSql, MySql, Sqlite, and Oracle.

---

## Setup

Call `SetupEfCoreExtensions()` in your `DbContext.OnConfiguring` override:

```csharp
protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
{
    optionsBuilder
        .UseSqlServer("your-connection-string")
        .SetupEfCoreExtensions();
}
```

---

## Usage

Common examples below. For options tables, result objects, and the full API reference, see the [main README](https://github.com/NorthernLight1/N.EntityFrameworkCore.Extensions#readme).

### BulkInsert

```csharp
var orders = new List<Order>();
for (int i = 0; i < 10000; i++)
    orders.Add(new Order { OrderDate = DateTime.UtcNow, TotalPrice = 2.99 });

dbContext.BulkInsert(orders);
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

Progress reporting:

```csharp
dbContext.BulkInsert(orders, options =>
{
    options.NotifyAfter = 1000;
    options.RowsCopied = e => Console.WriteLine($"Copied {e.RowsCopied} rows");
});
```

From an async stream (batches internally; default `BatchSize` is `10000` when unset):

```csharp
await dbContext.BulkInsertAsync(ReadOrdersAsync());

async IAsyncEnumerable<Order> ReadOrdersAsync()
{
    // yield entities from a stream, channel, or remote source
    yield break;
}
```


**SQL Server — tables with triggers:**

When inserting into a table that has `INSERT` triggers, set `FireTriggers = true`. This skips `MERGE … OUTPUT` (which is incompatible with triggers when mapping identities) while still supporting `AutoMapOutput` — identity values are mapped afterward when needed. You can set `AutoMapOutput = false` if you do not need generated keys on the entities.

```csharp
dbContext.BulkInsert(products, options =>
{
    options.FireTriggers = true;
});
```

**SQL Server — `SqlBulkCopy` tuning:**

Additional options for the underlying `SqlBulkCopy` used for staging and direct bulk inserts (`NotifyAfter` / `RowsCopied` also work with the cross-provider progress API above):

```csharp
dbContext.BulkInsert(orders, options =>
{
    options.EnableStreaming = true;
    options.NotifyAfter = 1000;
    options.RowsCopied = e => Console.WriteLine($"Copied {e.RowsCopied} rows");
    options.ColumnOrderHints.Add(new SqlBulkCopyColumnOrderHint("Id", SortOrder.Ascending));
    options.BulkCopyOptions = SqlBulkCopyOptions.TableLock;
});
```

| Option | Purpose |
| --- | --- |
| `EnableStreaming` | Stream rows from the reader instead of buffering the full batch in memory. |
| `ColumnOrderHints` | Hint column sort order to align with a clustered index during bulk load. |
| `NotifyAfter` / `RowsCopied` | Also drive `SqlBulkCopy` progress on the native bulk-copy path (same options as the cross-provider progress sample above). |
| `BulkCopyOptions` | Low-level flags such as `TableLock` and `CheckConstraints`. Prefer `FireTriggers` for trigger-enabled tables. |

### BulkDelete

```csharp
var orders = dbContext.Orders.Where(o => o.TotalPrice < 5.35M).ToList();

dbContext.BulkDelete(orders);
await dbContext.BulkDeleteAsync(orders);
```

Soft delete (updates columns from entity values instead of a physical delete):

```csharp
foreach (var order in orders)
    order.IsDeleted = true;

dbContext.BulkDelete(orders, options =>
{
    options.SoftDeleteColumns = x => new { x.IsDeleted };
});
```

### BulkFetch

```csharp
var stubs = new List<Product>
{
    new() { Id = 10001 },
    new() { Id = 10002 },
    new() { Id = 10003 },
};
var products = dbContext.Products.BulkFetch(stubs).ToList();
await dbContext.Products.BulkFetchAsync(stubs);
```

Attach fetched entities to the change tracker:

```csharp
var products = dbContext.Products
    .BulkFetch(stubs, options => { options.AttachToContext = true; })
    .ToList();
```

### BulkUpdate

```csharp
var products = dbContext.Products.Where(o => o.Price < 5.35M).ToList();
foreach (var product in products) product.Price = 6M;

dbContext.BulkUpdate(products);
await dbContext.BulkUpdateAsync(products);
```

Update only specific columns:

```csharp
dbContext.BulkUpdate(products, options =>
{
    options.InputColumns = o => new { o.Price };
});
```

### BulkMerge (Upsert)

```csharp
BulkMergeResult<Product> result = dbContext.BulkMerge(products);
Console.WriteLine($"Inserted: {result.RowsInserted}, Updated: {result.RowsUpdated}");
```

With a custom match condition:

```csharp
dbContext.BulkMerge(products, options =>
{
    options.MergeOnCondition = (s, t) => s.Id == t.Id;
    options.IgnoreColumnsOnInsert = o => new { o.CreatedDate };
    options.IgnoreColumnsOnUpdate = o => new { o.CreatedDate };
});
```

### BulkSync

```csharp
BulkSyncResult<Product> result = dbContext.BulkSync(products);
Console.WriteLine($"Inserted: {result.RowsInserted}, Updated: {result.RowsUpdated}, Deleted: {result.RowsDeleted}");
```

Soft-delete unmatched rows instead of removing them:

```csharp
dbContext.BulkSync(orders, options =>
{
    options.SoftDeleteColumns = x => new { IsDeleted = true };
});
```

### BulkSaveChanges

```csharp
dbContext.Orders.AddRange(orders);
dbContext.BulkSaveChanges();
await dbContext.BulkSaveChangesAsync();
```

### Fetch

```csharp
dbContext.Products.Where(o => o.Price < 5.35M).Fetch(result =>
{
    Console.WriteLine($"Batch {result.Batch}: {result.Results.Count} rows");
},
new FetchOptions<Product> { BatchSize = 1000 });

await dbContext.Products.Where(o => o.Price < 5.35M).FetchAsync(async result =>
{
    await ProcessBatchAsync(result.Results);
},
new FetchOptions<Product> { BatchSize = 1000 });
```

### DeleteFromQuery

```csharp
dbContext.Products.Where(x => x.Price < 5.35M).DeleteFromQuery();
await dbContext.Products.Where(x => x.Price < 5.35M).DeleteFromQueryAsync();
```

### InsertFromQuery

```csharp
dbContext.Products
    .Where(x => x.Price < 10M)
    .InsertFromQuery("ProductsUnderTen", o => new { o.Id, o.Price });
```

### UpdateFromQuery

```csharp
dbContext.Products
    .Where(x => x.Price == 5.35M)
    .UpdateFromQuery(o => new Product { Price = 5.75M });
```

### QueryToCsvFile

```csharp
QueryToFileResult result = dbContext.Products
    .Where(x => x.Price > 5M)
    .QueryToCsvFile("products.csv");
```

### SqlQueryToCsvFile

```csharp
dbContext.Database.SqlQueryToCsvFile(
    "output.csv",
    "SELECT Id, Name, Price FROM Products WHERE Price > @p0",
    5M);
```

### QueryToJsonFile

```csharp
dbContext.Products.Where(x => x.Price > 5M).QueryToJsonFile("products.json");
await dbContext.Products.QueryToJsonFileAsync("products.json");
```

### SqlQueryToJsonFile

```csharp
dbContext.Database.SqlQueryToJsonFile("output.json", "SELECT Id, Name FROM Products WHERE Price > @p0", 5M);
```

### QueryToJsonLinesFile

```csharp
dbContext.Products.Where(x => x.Price > 5M).QueryToJsonLinesFile("products.jsonl");
```

### SqlQueryToJsonLinesFile

```csharp
dbContext.Database.SqlQueryToJsonLinesFile("output.jsonl", "SELECT Id, Name FROM Products WHERE Price > @p0", 5M);
```

### Clear / Truncate

```csharp
dbContext.Orders.Clear();
await dbContext.Orders.ClearAsync();

dbContext.Orders.Truncate();
await dbContext.Orders.TruncateAsync();
```

### FromSqlQuery

```csharp
var sqlQuery = dbContext.Database.FromSqlQuery(
    "SELECT * FROM Products WHERE Price > @p0", 5M);
int count = sqlQuery.Count();
```

### Transactions

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

For full documentation including all options, result objects, and the complete API reference, see the [main README](https://github.com/NorthernLight1/N.EntityFrameworkCore.Extensions#readme).
