using System;
using System.Data.Common;
using Microsoft.Extensions.Configuration;
using MySqlConnector;

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
    public static bool IsMySql => true;
    public static bool IsPostgreSql => false;
    public static bool IsSqlServer => false;
    public static bool UseMySqlContainer =>
        !string.Equals(configuration["UseMySqlContainer"], "false", StringComparison.OrdinalIgnoreCase);
    public static string GetTestDatabaseConnectionString() =>
        UseMySqlContainer ? MySqlContainerManager.GetConnectionString() : GetConnectionString("MySqlTestDatabase");
    public static DbParameter CreateParameter(string name, object value) =>
        new MySqlParameter(name, value ?? DBNull.Value);
    public static string DelimitIdentifier(string identifier) => $"`{identifier}`";
    public static string DelimitTableName(string tableName) => $"`{tableName}`";
    public static bool IsPrimaryKeyViolation(Exception exception) =>
        exception.Message.Contains("Duplicate entry", StringComparison.OrdinalIgnoreCase);
}
