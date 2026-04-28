using System;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Npgsql;

namespace N.EntityFrameworkCore.Extensions.Test.Common;

public class Config
{
    private static readonly IConfigurationRoot configuration = new ConfigurationBuilder()
        .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
        .AddEnvironmentVariables()
        .Build();

    public static string GetConnectionString(string name)
    {
        return configuration.GetConnectionString(name);
    }
    public static string DatabaseProvider => configuration["DatabaseProvider"] ?? "SqlServer";
    public static bool IsSqlServer => string.Equals(DatabaseProvider, "SqlServer", StringComparison.OrdinalIgnoreCase);
    public static bool IsPostgreSql => string.Equals(DatabaseProvider, "PostgreSql", StringComparison.OrdinalIgnoreCase);
    public static bool UsePostgreSqlContainer =>
        IsPostgreSql && !string.Equals(configuration["UsePostgreSqlContainer"], "false", StringComparison.OrdinalIgnoreCase);
    public static string GetTestDatabaseConnectionString() => IsPostgreSql
        ? (UsePostgreSqlContainer ? PostgreSqlContainerManager.GetConnectionString() : GetConnectionString("PostgreSqlTestDatabase"))
        : GetConnectionString("SqlServerTestDatabase");
    public static DbParameter CreateParameter(string name, object value) => IsPostgreSql
        ? new NpgsqlParameter(name, value ?? DBNull.Value)
        : new SqlParameter(name, value ?? DBNull.Value);
    public static string DelimitIdentifier(string identifier) => IsPostgreSql ? $"\"{identifier}\"" : $"[{identifier}]";
    public static string DelimitTableName(string tableName) => IsPostgreSql ? $"\"{tableName}\"" : tableName;
    public static bool IsPrimaryKeyViolation(Exception exception) =>
        IsSqlServer
            ? exception.Message.StartsWith("Violation of PRIMARY KEY constraint 'PK_Orders'.", StringComparison.Ordinal)
            : exception.Message.Contains("duplicate key value violates unique constraint", StringComparison.OrdinalIgnoreCase);
}
