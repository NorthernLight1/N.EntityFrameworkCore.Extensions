# N.EntityFrameworkCore.Extensions.Sqlite

SQLite provider for `N.EntityFrameworkCore.Extensions`.

Provides high-performance bulk operations for EF Core SQLite contexts:
- BulkDelete
- BulkInsert
- BulkMerge
- BulkSync
- BulkUpdate
- Fetch
- DeleteFromQuery
- InsertFromQuery
- UpdateFromQuery

## Installation

```sh
dotnet add package N.EntityFrameworkCore.Extensions.Sqlite
```

## Setup

```csharp
protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
{
    optionsBuilder
        .UseSqlite("your-connection-string")
        .SetupEfCoreExtensions();
}
```
