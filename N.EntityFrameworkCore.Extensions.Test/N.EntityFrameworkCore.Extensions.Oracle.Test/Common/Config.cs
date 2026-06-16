using System;
using System.Data.Common;
using Oracle.ManagedDataAccess.Client;
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
    public static bool IsOracle => true;
    public static bool UseOracleContainer =>
        !string.Equals(configuration["UseOracleContainer"], "false", StringComparison.OrdinalIgnoreCase);
    public static string GetTestDatabaseConnectionString() =>
        UseOracleContainer ? OracleContainerManager.GetConnectionString() : GetConnectionString("OracleTestDatabase");
    public static string GetRestrictedTestDatabaseConnectionString() =>
        UseOracleContainer ? OracleContainerManager.GetRestrictedConnectionString() : GetConnectionString("OracleRestrictedTestDatabase");
    public static DbParameter CreateParameter(string name, object value) => new OracleParameter(name, value ?? DBNull.Value);
    public static string DelimitIdentifier(string identifier) => $"\"{identifier.Trim('"')}\"";
    public static string DelimitTableName(string tableName)
    {
        var parts = tableName.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return parts.Length == 2
            ? $"{DelimitIdentifier(parts[0].ToUpperInvariant())}.{DelimitIdentifier(parts[1].ToUpperInvariant())}"
            : DelimitIdentifier(tableName.ToUpperInvariant());
    }
    public static bool IsPrimaryKeyViolation(Exception exception) =>
        exception.Message.StartsWith("ORA-00001: unique constraint", StringComparison.Ordinal);
}
