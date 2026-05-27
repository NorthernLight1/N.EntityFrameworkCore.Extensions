## N.EntityFrameworkCore.Extensions.SqlServer

[![latest version](https://img.shields.io/nuget/v/N.EntityFrameworkCore.Extensions.SqlServer)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions.SqlServer) [![downloads](https://img.shields.io/nuget/dt/N.EntityFrameworkCore.Extensions.SqlServer)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions.SqlServer)

High-performance bulk data extensions for Entity Framework Core — **SQL Server** provider. Extends your `DbContext` with bulk operations, query-based DML, CSV export, and utility helpers — all without loading entities into memory.

**Supported operations:** BulkDelete · BulkFetch · BulkInsert · BulkMerge · BulkSaveChanges · BulkSync · BulkUpdate · Fetch · DeleteFromQuery · InsertFromQuery · UpdateFromQuery · QueryToCsvFile · SqlQueryToCsvFile

**Supports:** Multiple Schemas · Complex Properties · Value Converters · Transactions · Synchronous & Asynchronous Execution

**Inheritance Models:** Table-Per-Concrete · Table-Per-Hierarchy · Table-Per-Type

---

> 💬 **Feedback & Feature Requests**
> Found a bug or have an idea? Please [open an issue](https://github.com/NorthernLight1/N.EntityFrameworkCore.Extensions/issues) on GitHub.

---

## Installation

```sh
dotnet add package N.EntityFrameworkCore.Extensions.SqlServer
```

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

### BulkInsert

```csharp
var orders = new List<Order>();
for (int i = 0; i < 10000; i++)
    orders.Add(new Order { OrderDate = DateTime.UtcNow, TotalPrice = 2.99 });

dbContext.BulkInsert(orders);
await dbContext.BulkInsertAsync(orders);
```

### BulkUpdate

```csharp
var products = dbContext.Products.Where(o => o.Price < 5.35M).ToList();
foreach (var product in products) product.Price = 6M;

dbContext.BulkUpdate(products);
await dbContext.BulkUpdateAsync(products);
```

### BulkDelete

```csharp
var orders = dbContext.Orders.Where(o => o.TotalPrice < 5.35M).ToList();

dbContext.BulkDelete(orders);
await dbContext.BulkDeleteAsync(orders);
```

### BulkMerge (Upsert)

```csharp
BulkMergeResult<Product> result = dbContext.BulkMerge(products);
Console.WriteLine($"Inserted: {result.RowsInserted}, Updated: {result.RowsUpdated}");
```

### BulkSync

```csharp
BulkSyncResult<Product> result = dbContext.BulkSync(products);
Console.WriteLine($"Inserted: {result.RowsInserted}, Updated: {result.RowsUpdated}, Deleted: {result.RowsDeleted}");
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
}
```

---

For full documentation including all options, result objects, and the complete API reference, see the [main README](https://github.com/NorthernLight1/N.EntityFrameworkCore.Extensions#readme).

---

## Donations

- 💳 **PayPal:** [Donate via PayPal](https://www.paypal.com/donate/?hosted_button_id=HR6JSVYKAMLSQ)
- ₿ **Bitcoin:** `bc1qxqpymnf4gj22nt4wj3wy56ks48fw59v8y9sg9z`
