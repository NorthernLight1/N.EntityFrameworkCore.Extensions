using System;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;

namespace N.EntityFrameworkCore.Extensions.Test.Common;

public class Config
{
    private static readonly IConfigurationRoot configuration = new ConfigurationBuilder()
        .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
        .AddEnvironmentVariables()
        .Build();

    // Keeps the named in-memory database alive for the entire test session.
    private static readonly SqliteConnection _keepAliveConnection = OpenKeepAlive();

    private static SqliteConnection OpenKeepAlive()
    {
        var conn = new SqliteConnection(GetConnectionString("SqliteTestDatabase")
            ?? "Data Source=N.EntityFrameworkCore.Extensions.Test;Mode=Memory;Cache=Shared;Foreign Keys=True");
        conn.Open();
        return conn;
    }

    public static string GetConnectionString(string name)
    {
        return configuration.GetConnectionString(name);
    }
    public static string DatabaseProvider => configuration["DatabaseProvider"] ?? "Sqlite";
    public static bool IsSqlite => string.Equals(DatabaseProvider, "Sqlite", StringComparison.OrdinalIgnoreCase);
    public static string GetTestDatabaseConnectionString() => GetConnectionString("SqliteTestDatabase")
        ?? "Data Source=N.EntityFrameworkCore.Extensions.Test;Mode=Memory;Cache=Shared;Foreign Keys=True";
    public static DbParameter CreateParameter(string name, object value)
    {
        if (value is decimal d)
            return new SqliteDecimalParameter(name, d);
        return new SqliteParameter(name, value ?? DBNull.Value);
    }
    public static string DelimitIdentifier(string identifier) => $"\"{identifier}\"";
    public static string DelimitTableName(string tableName) => DelimitIdentifier(tableName);
    public static bool IsPrimaryKeyViolation(Exception exception) =>
        exception.Message.Contains("UNIQUE constraint failed", StringComparison.OrdinalIgnoreCase)
        || exception.Message.Contains("PRIMARY KEY constraint failed", StringComparison.OrdinalIgnoreCase)
        || exception.Message.Contains("SQLite Error 19", StringComparison.OrdinalIgnoreCase);
}

