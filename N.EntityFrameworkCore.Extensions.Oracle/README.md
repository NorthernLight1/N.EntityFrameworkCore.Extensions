## N.EntityFrameworkCore.Extensions.Oracle

[![latest version](https://img.shields.io/nuget/v/N.EntityFrameworkCore.Extensions.Oracle)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions.Oracle) [![downloads](https://img.shields.io/nuget/dt/N.EntityFrameworkCore.Extensions.Oracle)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions.Oracle)

High-performance bulk data extensions for Entity Framework Core — **Oracle** provider. Extends your `DbContext` with bulk operations, query-based DML, CSV export, and utility helpers — all without loading entities into memory.

**Supported operations:** BulkDelete · BulkFetch · BulkInsert · BulkMerge · BulkSaveChanges · BulkSync · BulkUpdate · Fetch · DeleteFromQuery · InsertFromQuery · UpdateFromQuery · QueryToCsvFile · SqlQueryToCsvFile

**Supports:** Multiple Schemas · Complex Properties · Value Converters · Transactions · Synchronous & Asynchronous Execution

**Inheritance Models:** Table-Per-Concrete · Table-Per-Hierarchy · Table-Per-Type

---

## Installation

```sh
dotnet add package N.EntityFrameworkCore.Extensions.Oracle
```

---

## Setup

Call `SetupEfCoreExtensions()` in your `DbContext.OnConfiguring` override:

```csharp
protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
{
    optionsBuilder
        .UseOracle("your-connection-string")
        .SetupEfCoreExtensions();
}
```

---

For full documentation including all options, result objects, and the complete API reference, see the [main README](https://github.com/NorthernLight1/N.EntityFrameworkCore.Extensions#readme).
