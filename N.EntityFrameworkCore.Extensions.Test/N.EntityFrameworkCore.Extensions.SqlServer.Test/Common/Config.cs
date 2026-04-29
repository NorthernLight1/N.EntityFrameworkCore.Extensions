using System;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

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
    public static bool IsSqlServer => true;
    public static bool UseSqlServerContainer =>
        !string.Equals(configuration["UseSqlServerContainer"], "false", StringComparison.OrdinalIgnoreCase);
    public static string GetTestDatabaseConnectionString() =>
        UseSqlServerContainer ? SqlServerContainerManager.GetConnectionString() : GetConnectionString("SqlServerTestDatabase");
    public static DbParameter CreateParameter(string name, object value) => new SqlParameter(name, value ?? DBNull.Value);
    public static string DelimitIdentifier(string identifier) => $"[{identifier}]";
    public static string DelimitTableName(string tableName) => tableName;
    public static bool IsPrimaryKeyViolation(Exception exception) =>
        exception.Message.StartsWith("Violation of PRIMARY KEY constraint 'PK_Orders'.", StringComparison.Ordinal);
}
