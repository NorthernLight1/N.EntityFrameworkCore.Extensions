## N.EntityFrameworkCore.Extensions

[![NuGet](https://img.shields.io/nuget/v/N.EntityFrameworkCore.Extensions)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions)
[![Downloads](https://img.shields.io/nuget/dt/N.EntityFrameworkCore.Extensions)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions)

**Meta-package** that depends on all provider packages:

| Provider | Package |
| --- | --- |
| SQL Server | [N.EntityFrameworkCore.Extensions.SqlServer](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions.SqlServer) |
| PostgreSQL | [N.EntityFrameworkCore.Extensions.PostgreSql](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions.PostgreSql) |
| MySQL | [N.EntityFrameworkCore.Extensions.MySql](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions.MySql) |
| SQLite | [N.EntityFrameworkCore.Extensions.Sqlite](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions.Sqlite) |
| Oracle | [N.EntityFrameworkCore.Extensions.Oracle](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions.Oracle) |

### Install

```sh
# All providers
dotnet add package N.EntityFrameworkCore.Extensions
```

If your app only uses one database, install that provider package directly instead:

```sh
dotnet add package N.EntityFrameworkCore.Extensions.SqlServer
# or .PostgreSql / .MySql / .Sqlite / .Oracle
```

Setup, usage examples, options, and API reference are in the [repository README](https://github.com/NorthernLight1/N.EntityFrameworkCore.Extensions#readme).
