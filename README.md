## N.EntityFrameworkCore.Extensions

[![latest version](https://img.shields.io/nuget/v/N.EntityFrameworkCore.Extensions)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions) [![downloads](https://img.shields.io/nuget/dt/N.EntityFrameworkCore.Extensions)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions)

High-performance bulk data extensions for Entity Framework Core. Extends your `DbContext` with bulk operations, query-based DML, CSV export, and utility helpers — all without loading entities into memory.

**Supported operations:** BulkDelete · BulkFetch · BulkInsert · BulkMerge · BulkSaveChanges · BulkSync · BulkUpdate · Fetch · DeleteFromQuery · InsertFromQuery · UpdateFromQuery · QueryToCsvFile · SqlQueryToCsvFile

**Supports:** Multiple Schemas · Complex Properties · Value Converters · Transactions · Synchronous & Asynchronous Execution

**Inheritance Models:** Table-Per-Concrete · Table-Per-Hierarchy · Table-Per-Type

**Database:** SQL Server · PostgreSql · MySQL

---

> 💬 **Feedback & Feature Requests**
> Found a bug? Have an idea for a new feature or improvement? We'd love to hear from you!
> Please [open an issue](https://github.com/NorthernLight1/N.EntityFrameworkCore.Extensions/issues) on GitHub — whether it's a bug report, a feature request, a question, or general feedback, all contributions are welcome.

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
  - [SqlQueryToCsvFile](#sqlquerytocsvfile)
  - [DbSet Utilities](#dbset-utilities)
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
- [Result Objects](#result-objects)
  - [BulkMergeResult\<T\>](#bulkmergeresultt)
  - [BulkMergeOutputRow\<T\>](#bulkmergeoutputrowt)
  - [BulkSyncResult\<T\>](#bulksyncresultt)
  - [FetchResult\<T\>](#fetchresultt)
  - [QueryToFileResult](#querytofileresult)
  - [SqlQuery](#sqlquery)
- [Transactions](#transactions)
- [MySQL Limitations](#mysql-limitations)
- [API Reference](#api-reference)
- [Donations](#donations)

---

## Installation

Install the **all-in-one meta-package** (includes SQL Server and PostgreSql — MySQL must be installed separately):

```sh
dotnet add package N.EntityFrameworkCore.Extensions
```

Or install **only the provider you need**:

| Provider | Package |
| --- | --- |
| SQL Server | [![](https://img.shields.io/nuget/v/N.EntityFrameworkCore.Extensions.SqlServer?label=NuGet)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions.SqlServer) `dotnet add package N.EntityFrameworkCore.Extensions.SqlServer` |
| PostgreSql | [![](https://img.shields.io/nuget/v/N.EntityFrameworkCore.Extensions.PostgreSql?label=NuGet)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions.PostgreSql) `dotnet add package N.EntityFrameworkCore.Extensions.PostgreSql` |
| MySQL | [![](https://img.shields.io/nuget/v/N.EntityFrameworkCore.Extensions.MySql?label=NuGet)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions.MySql) `dotnet add package N.EntityFrameworkCore.Extensions.MySql` |

---

## Setup

Call `SetupEfCoreExtensions()` in your `DbContext.OnConfiguring` override.

### SQL Server

```csharp
protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
{
    optionsBuilder
        .UseSqlServer("your-connection-string")
        .SetupEfCoreExtensions();
}
```

### PostgreSql

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

This registers an EF Core `DbCommandInterceptor` used internally by bulk operations. It is required for operations that rewrite table names at execution time (e.g. `InsertFromQuery` targeting a new table); all other operations work without it.

### Test configuration

The SQL Server test project uses `N.EntityFrameworkCore.Extensions.Test\N.EntityFrameworkCore.Extensions.SqlServer.Test\appsettings.json` (or `ConnectionStrings__SqlServerTestDatabase` in the environment). The PostgreSql test project uses `N.EntityFrameworkCore.Extensions.Test\N.EntityFrameworkCore.Extensions.PostgreSql.Test\appsettings.json` (or `ConnectionStrings__PostgreSqlTestDatabase` in the environment). The MySQL test project uses `N.EntityFrameworkCore.Extensions.Test\N.EntityFrameworkCore.Extensions.MySql.Test\appsettings.json` (or `ConnectionStrings__MySqlTestDatabase` in the environment).

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

### BulkFetch

Retrieves entities from the database that match objects in a local list (useful for key-based lookups).

```csharp
var ids = new List<int> { 10001, 10002, 10003, 10004, 10005 };
var products = dbContext.Products
    .BulkFetch(ids, options => { options.JoinOnCondition = (s, t) => s.Id == t.Id; })
    .ToList();
```

Async:

```csharp
var products = await dbContext.Products
    .BulkFetchAsync(ids, options => { options.JoinOnCondition = (s, t) => s.Id == t.Id; });
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

### BulkSync

Synchronizes the database table with the provided list. Entities not in the source list are deleted by default.

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

// All existing products with Id > 1000 will be deleted
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

Inserts rows into a target table by selecting from a LINQ query, without loading data into the `DbContext`.

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

### DbSet Utilities

**Clear** — deletes all rows in the table (equivalent to `DELETE FROM`):

```csharp
dbContext.Orders.Clear();
await dbContext.Orders.ClearAsync();
```

**Truncate** — truncates the table (faster than Clear, resets identity):

```csharp
dbContext.Orders.Truncate();
await dbContext.Orders.TruncateAsync();
```

---

## Options

### Common Options (BulkOptions)

All bulk operations accept options that derive from `BulkOptions`:

| Property | Type | Description |
| --- | --- | --- |
| `BatchSize` | `int` | Number of rows per batch. Defaults to `0` (driver default). |
| `CommandTimeout` | `int?` | SQL command timeout in seconds. |
| `UsePermanentTable` | `bool` | Use a permanent staging table instead of a temporary one. |

### BulkInsertOptions

| Property | Type | Description |
| --- | --- | --- |
| `AutoMapOutput` | `bool` | Map database-generated values (e.g. identity keys) back to entities. Default: `true`. |
| `IgnoreColumns` | `Expression<Func<T, object>>` | Columns to exclude from the insert. |
| `InputColumns` | `Expression<Func<T, object>>` | Columns to include in the insert (all others are excluded). |
| `InsertIfNotExists` | `bool` | Skip rows that already exist in the target table. Default: `false`. |
| `InsertOnCondition` | `Expression<Func<T, T, bool>>` | Custom condition used to determine whether a row already exists. |
| `KeepIdentity` | `bool` | Preserve source identity values instead of letting the database generate them. Default: `false`. |

### BulkDeleteOptions

| Property | Type | Description |
| --- | --- | --- |
| `DeleteOnCondition` | `Expression<Func<T, T, bool>>` | Custom condition used to match rows for deletion. |

### BulkUpdateOptions

| Property | Type | Description |
| --- | --- | --- |
| `InputColumns` | `Expression<Func<T, object>>` | Columns to update (all others are excluded). |
| `IgnoreColumns` | `Expression<Func<T, object>>` | Columns to exclude from the update. |
| `UpdateOnCondition` | `Expression<Func<T, T, bool>>` | Custom condition used to match rows for updating. |

### BulkMergeOptions

| Property | Type | Description |
| --- | --- | --- |
| `MergeOnCondition` | `Expression<Func<T, T, bool>>` | Custom condition used to match source and target rows. |
| `IgnoreColumnsOnInsert` | `Expression<Func<T, object>>` | Columns to exclude when inserting new rows. |
| `IgnoreColumnsOnUpdate` | `Expression<Func<T, object>>` | Columns to exclude when updating existing rows. |
| `AutoMapOutput` | `bool` | Map database-generated values back to entities after the merge. Default: `true`. |

### BulkSyncOptions

Inherits all `BulkMergeOptions` properties. `DeleteIfNotMatched` is always `true` for `BulkSync`, meaning rows not present in the source list are always removed from the target table. Use `BulkMerge` if you do not want rows deleted.

### BulkFetchOptions

| Property | Type | Description |
| --- | --- | --- |
| `JoinOnCondition` | `Expression<Func<T, T, bool>>` | Condition used to join the local list to the database table. |
| `InputColumns` | `Expression<Func<T, object>>` | Columns to include from the local list in the join. |
| `IgnoreColumns` | `Expression<Func<T, object>>` | Columns to exclude from the result. |

### FetchOptions

| Property | Type | Description |
| --- | --- | --- |
| `BatchSize` | `int` | Number of rows to retrieve per batch. |
| `InputColumns` | `Expression<Func<T, object>>` | Columns to select. |
| `IgnoreColumns` | `Expression<Func<T, object>>` | Columns to exclude from results. |

### QueryToFileOptions

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `ColumnDelimiter` | `string` | `","` | Column separator character. |
| `RowDelimiter` | `string` | `"\r\n"` | Row separator sequence. |
| `IncludeHeaderRow` | `bool` | `true` | Write a header row with column names. |
| `TextQualifer` | `string` | `""` | Character used to wrap field values (e.g. `"`). |
| `CommandTimeout` | `int?` | `null` | SQL command timeout in seconds. |

---

## Result Objects

### BulkMergeResult\<T\>

Returned by `BulkMerge` and `BulkMergeAsync`.

| Property | Type | Description |
| --- | --- | --- |
| `RowsAffected` | `int` | Total number of rows affected. |
| `RowsInserted` | `int` | Number of rows inserted. |
| `RowsUpdated` | `int` | Number of rows updated. |
| `RowsDeleted` | `int` | Number of rows deleted (populated by `BulkSync`). |
| `Output` | `IEnumerable<BulkMergeOutputRow<T>>` | Per-row output with merge action details. |

### BulkMergeOutputRow\<T\>

Each element in `BulkMergeResult<T>.Output`.

| Property | Type | Description |
| --- | --- | --- |
| `Action` | `string` | The merge action performed. One of `"INSERT"`, `"UPDATE"`, or `"DELETE"`. |

Example — inspecting per-row results after a merge:

```csharp
var result = dbContext.BulkMerge(products);
foreach (var row in result.Output)
{
    Console.WriteLine(row.Action); // "INSERT", "UPDATE", or "DELETE"
}
```

### BulkSyncResult\<T\>

Inherits `BulkMergeResult<T>`. Returned by `BulkSync` and `BulkSyncAsync`. `RowsDeleted` is always populated.

### FetchResult\<T\>

Passed to the callback in `Fetch` / `FetchAsync`.

| Property | Type | Description |
| --- | --- | --- |
| `Results` | `List<T>` | Entities in the current batch. |
| `Batch` | `int` | Current batch number (1-based). |

### QueryToFileResult

Returned by `QueryToCsvFile`, `SqlQueryToCsvFile`, and their async variants.

| Property | Type | Description |
| --- | --- | --- |
| `DataRowCount` | `int` | Number of data rows written (excludes header). |
| `TotalRowCount` | `int` | Total rows written including header. |
| `BytesWritten` | `long` | Bytes written to the file or stream. |

### SqlQuery

Returned by `DatabaseFacade.FromSqlQuery(...)`. Allows counting or executing raw SQL without loading entities.

| Member | Description |
| --- | --- |
| `Count()` | Returns the number of rows matched by the query. |
| `CountAsync(cancellationToken)` | Async version of `Count()`. |
| `ExecuteNonQuery()` | Executes the SQL statement and returns the number of rows affected. |
| `SqlText` | The SQL text of the query. |
| `Parameters` | The parameters passed to the query. |

Example:

```csharp
var sqlQuery = dbContext.Database.FromSqlQuery("SELECT * FROM Products WHERE Price > @p0", 5M);
int count = sqlQuery.Count();
Console.WriteLine($"Matching rows: {count}");
```

---

## Transactions

All bulk operations participate in an ambient transaction when one exists. Pass the transaction to `BeginTransaction()` or use the `Database` property on your context:

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
}
```

---

## MySQL Limitations

MySQL has specific constraints that affect certain operations due to how it handles DDL statements and transactions.

### InsertFromQuery and Transactions

`InsertFromQuery` / `InsertFromQueryAsync` are **not supported inside user-managed transactions** on MySQL. Internally these operations execute `CREATE TABLE ... SELECT`, which is a DDL statement. MySQL automatically issues an implicit commit before and after any DDL statement, which would silently commit your active transaction.

```csharp
// ⚠️ Do NOT use InsertFromQuery inside a transaction on MySQL
using var transaction = dbContext.Database.BeginTransaction();
dbContext.Products
    .Where(x => x.Price < 10M)
    .InsertFromQuery("ProductsUnderTen", o => new { o.Id, o.Price }); // implicit commit!
transaction.Rollback(); // has no effect — already committed
```

Use `InsertFromQuery` outside of a transaction on MySQL, or use `BulkInsert` as an alternative when transactional safety is required.

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
| **BulkMerge** | |
| `BulkMerge<T>(items)` | Bulk merge (upsert) entities. Returns `BulkMergeResult<T>`. |
| `BulkMerge<T>(items, options)` | Bulk merge with options. |
| `BulkMergeAsync<T>(items, cancellationToken)` | Async bulk merge. |
| `BulkMergeAsync<T>(items, options, cancellationToken)` | Async bulk merge with options. |
| **BulkSaveChanges** | |
| `BulkSaveChanges()` | Save all pending changes using bulk operations. |
| `BulkSaveChanges(acceptAllChangesOnSuccess)` | Save changes, controlling whether `AcceptAllChanges` is called. |
| `BulkSaveChangesAsync(cancellationToken)` | Async bulk save changes. |
| `BulkSaveChangesAsync(acceptAllChangesOnSuccess, cancellationToken)` | Async bulk save changes with option. |
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
| `Fetch<T>(action, options)` | Fetch rows in batches and process each batch via a callback. |
| `FetchAsync<T>(action, options, cancellationToken)` | Async batch fetch. |
| **DeleteFromQuery** | |
| `DeleteFromQuery<T>()` | Delete all matching rows without loading them. |
| `DeleteFromQueryAsync<T>(cancellationToken)` | Async delete from query. |
| **InsertFromQuery** | |
| `InsertFromQuery<T>(tableName, selectExpression)` | Insert query results into another table. |
| `InsertFromQueryAsync<T>(tableName, selectExpression, cancellationToken)` | Async insert from query. |
| **UpdateFromQuery** | |
| `UpdateFromQuery<T>(updateExpression)` | Update all matching rows without loading them. |
| `UpdateFromQueryAsync<T>(updateExpression, cancellationToken)` | Async update from query. |
| **QueryToCsvFile** | |
| `QueryToCsvFile<T>(filePath)` | Export query results to a CSV file. |
| `QueryToCsvFile<T>(stream)` | Export query results to a stream. |
| `QueryToCsvFile<T>(filePath, options)` | Export to file with options. |
| `QueryToCsvFile<T>(stream, options)` | Export to stream with options. |
| `QueryToCsvFileAsync<T>(filePath, cancellationToken)` | Async export to file. |
| `QueryToCsvFileAsync<T>(stream, cancellationToken)` | Async export to stream. |
| `QueryToCsvFileAsync<T>(filePath, options, cancellationToken)` | Async export to file with options. |
| `QueryToCsvFileAsync<T>(stream, options, cancellationToken)` | Async export to stream with options. |

### DatabaseFacade Extensions

| Method | Description |
| --- | --- |
| `FromSqlQuery(sqlText, parameters)` | Create a `SqlQuery` object for counting or executing raw SQL. |
| `SqlQueryToCsvFile(filePath, sqlText, parameters)` | Export raw SQL results to a CSV file. |
| `SqlQueryToCsvFile(stream, sqlText, parameters)` | Export raw SQL results to a stream. |
| `SqlQueryToCsvFile(filePath, options, sqlText, parameters)` | Export with options. |
| `SqlQueryToCsvFileAsync(filePath, sqlText, parameters, cancellationToken)` | Async export to file. |
| `SqlQueryToCsvFileAsync(stream, sqlText, parameters, cancellationToken)` | Async export to stream. |
| `SqlQueryToCsvFileAsync(filePath, options, sqlText, parameters, cancellationToken)` | Async export to file with options. |
| `ClearTable(tableName)` | Delete all rows from a table by name. |
| `ClearTableAsync(tableName, cancellationToken)` | Async delete all rows from a table by name. |
| `TruncateTable(tableName, ifExists)` | Truncate a table by name. |
| `TruncateTableAsync(tableName, ifExists, cancellationToken)` | Async truncate a table by name. |
| `DropTable(tableName, ifExists)` | Drop a table by name. |
| `TableExists(tableName)` | Returns `true` if the table exists. |
| `TableHasIdentity(tableName)` | Returns `true` if the table has an identity column. |

---

## Donations

If you found this project helpful and you would like to support it, feel free to donate through PayPal or Bitcoin.

- 💳 **PayPal:** [Donate via PayPal](https://www.paypal.com/donate/?hosted_button_id=HR6JSVYKAMLSQ)
- ₿ **Bitcoin:** `bc1qxqpymnf4gj22nt4wj3wy56ks48fw59v8y9sg9z`
