using System;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace N.EntityFrameworkCore.Extensions.Util;

internal enum DatabaseProvider
{
    SqlServer,
    PostgreSql,
    Sqlite
}

internal readonly record struct DatabaseObjectName(string Schema, string Name)
{
    internal bool HasSchema => !string.IsNullOrWhiteSpace(Schema);
}

internal static class RelationalProviderUtil
{
    internal static DatabaseProvider GetDatabaseProvider(this DatabaseFacade database)
    {
        return database.ProviderName switch
        {
            string providerName when providerName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase) => DatabaseProvider.SqlServer,
            string providerName when providerName.Contains("Npgsql", StringComparison.OrdinalIgnoreCase) => DatabaseProvider.PostgreSql,
            string providerName when providerName.Contains("Sqlite", StringComparison.OrdinalIgnoreCase) => DatabaseProvider.Sqlite,
            _ => throw new NotSupportedException($"The database provider '{database.ProviderName}' is not supported.")
        };
    }

    internal static bool IsSqlServer(this DatabaseFacade database) => database.GetDatabaseProvider() == DatabaseProvider.SqlServer;

    internal static bool IsPostgreSql(this DatabaseFacade database) => database.GetDatabaseProvider() == DatabaseProvider.PostgreSql;

    internal static bool IsSqliteProvider(this DatabaseFacade database) => database.GetDatabaseProvider() == DatabaseProvider.Sqlite;

    internal static string GetDefaultSchema(this DatabaseFacade database) =>
        database.IsSqliteProvider() ? null : database.IsPostgreSql() ? "public" : "dbo";

    internal static string DelimitIdentifier(this DatabaseFacade database, string identifier) =>
        database.GetSqlGenerationHelper().DelimitIdentifier(UnwrapIdentifier(identifier));

    internal static string DelimitIdentifier(this DatabaseFacade database, string identifier, string schema) =>
        database.IsSqliteProvider() || schema == null
            ? database.DelimitIdentifier(identifier)
            : database.GetSqlGenerationHelper().DelimitIdentifier(UnwrapIdentifier(identifier), UnwrapIdentifier(schema));

    internal static string DelimitIdentifier(this DbContext dbContext, string identifier) =>
        dbContext.Database.DelimitIdentifier(identifier);

    internal static string DelimitIdentifier(this DbContext dbContext, string identifier, string schema) =>
        dbContext.Database.DelimitIdentifier(identifier, schema);

    internal static string DelimitTableName(this DatabaseFacade database, string tableName)
    {
        var objectName = database.ParseObjectName(tableName);
        return objectName.HasSchema
            ? database.DelimitIdentifier(objectName.Name, objectName.Schema)
            : database.DelimitIdentifier(objectName.Name);
    }

    internal static string DelimitTableName(this DbContext dbContext, string tableName) =>
        dbContext.Database.DelimitTableName(tableName);

    internal static string DelimitMemberAccess(this DbContext dbContext, string alias, string columnName) =>
        $"{dbContext.DelimitIdentifier(alias)}.{dbContext.DelimitIdentifier(columnName)}";

    internal static string DelimitMemberAccess(this DatabaseFacade database, string alias, string columnName) =>
        $"{database.DelimitIdentifier(alias)}.{database.DelimitIdentifier(columnName)}";

    internal static DatabaseObjectName ParseObjectName(this DatabaseFacade database, string objectName)
    {
        string normalized = objectName.Trim();
        if (string.IsNullOrWhiteSpace(normalized))
            throw new ArgumentException("Object name cannot be empty.", nameof(objectName));

        var parts = normalized.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (database.IsSqliteProvider())
        {
            string name = parts.Length >= 1 ? UnwrapIdentifier(parts[parts.Length - 1]) : normalized;
            return new DatabaseObjectName(null, name);
        }

        return parts.Length switch
        {
            1 => new DatabaseObjectName(IsTemporaryName(parts[0]) ? null : database.GetDefaultSchema(), UnwrapIdentifier(parts[0])),
            2 => new DatabaseObjectName(UnwrapIdentifier(parts[0]), UnwrapIdentifier(parts[1])),
            _ => throw new InvalidOperationException($"Unsupported object name format '{objectName}'.")
        };
    }

    internal static string UnwrapIdentifier(string value) =>
        value.Trim().Trim('[', ']', '"', '`');

    internal static string GetTemporaryTableName(this DatabaseFacade database, string baseName)
    {
        string temporaryName = $"tmp_be_xx_{UnwrapIdentifier(baseName)}_{Guid.NewGuid():N}";
        return database.DelimitIdentifier(temporaryName);
    }

    internal static string GetPermanentStagingTableName(this DatabaseFacade database, string schema, string tableName, string uniqueSuffix)
    {
        string stagingName = $"tmp_be_xx_{UnwrapIdentifier(tableName)}_{uniqueSuffix}";
        return database.IsSqliteProvider()
            ? $"main.{database.DelimitIdentifier(stagingName)}"
            : database.DelimitIdentifier(stagingName, schema);
    }

    internal static DbConnection CloneConnection(this DbConnection dbConnection)
    {
        if (dbConnection is SqliteConnection sqliteConnection)
        {
            var newConnection = new SqliteConnection(sqliteConnection.ConnectionString);
            newConnection.Open();
            return newConnection;
        }

        return dbConnection switch
        {
            ICloneable cloneable => (DbConnection)cloneable.Clone(),
            _ => throw new NotSupportedException($"Connection type '{dbConnection.GetType().FullName}' does not support cloning.")
        };
    }

    private static ISqlGenerationHelper GetSqlGenerationHelper(this DatabaseFacade database) =>
        ((IInfrastructure<IServiceProvider>)database).Instance.GetService(typeof(ISqlGenerationHelper)) as ISqlGenerationHelper
        ?? throw new InvalidOperationException("Unable to resolve ISqlGenerationHelper.");

    private static bool IsTemporaryName(string objectName) =>
        !string.IsNullOrWhiteSpace(objectName)
        && UnwrapIdentifier(objectName).StartsWith("#", StringComparison.Ordinal);
}
