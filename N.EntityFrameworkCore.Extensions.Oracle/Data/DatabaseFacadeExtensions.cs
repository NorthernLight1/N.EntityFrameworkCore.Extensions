using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using N.EntityFrameworkCore.Extensions.Enums;
using N.EntityFrameworkCore.Extensions.Util;
using Oracle.ManagedDataAccess.Client;

namespace N.EntityFrameworkCore.Extensions;

public static class DatabaseFacadeExtensions
{
    private static readonly ConcurrentDictionary<string, byte> TransactionalCloneTables = new(StringComparer.OrdinalIgnoreCase);

    public static SqlQuery FromSqlQuery(this DatabaseFacade database, string sqlText, params object[] parameters)
    {
        return new SqlQuery(database, sqlText, parameters);
    }
    public static int ClearTable(this DatabaseFacade database, string tableName)
    {
        string sql = $"DELETE FROM {database.DelimitTableName(tableName)}";
        try
        {
            return database.ExecuteSqlRaw(sql);
        }
        catch (OracleException ex) when (ex.Number == 1502)
        {
            try
            {
                database.RebuildUnusableIndexes([tableName]);
                return database.ExecuteSqlRaw(sql);
            }
            catch (OracleException rebuildException) when (rebuildException.Number == 1452)
            {
                database.TruncateTableForRecovery(tableName);
                database.RebuildUnusableIndexes([tableName]);
                return 0;
            }
        }
    }
    public static int DropTable(this DatabaseFacade database, string tableName, bool ifExists = false)
    {
        string formattedTableName = database.DelimitTableName(tableName);
        string sql = ifExists ? $"DROP TABLE IF EXISTS {formattedTableName}" : $"DROP TABLE {formattedTableName}";
        return database.ExecuteSqlInternal(sql, null, ConnectionBehavior.Default);
    }
    public static void TruncateTable(this DatabaseFacade database, string tableName, bool ifExists = false)
    {
        bool truncateTable = !ifExists || database.TableExists(tableName);
        if (!truncateTable)
            return;

        string formattedTableName = database.DelimitTableName(tableName);
        string sql = $"TRUNCATE TABLE {formattedTableName}";
        database.ExecuteSqlRaw(sql);
    }
    public static bool TableExists(this DatabaseFacade database, string tableName)
    {
        if (database.CurrentTransaction == null && TransactionalCloneTables.TryRemove(GetTransactionalCloneTableKey(database, tableName), out _))
        {
            try
            {
                database.ExecuteSqlInternal($"DROP TABLE {database.DelimitTableName(tableName)}");
            }
            catch
            {
                // If rollback or external cleanup already removed the table, treat it as absent.
            }

            return false;
        }

        var objectName = database.ParseObjectName(tableName);
        string normalizedName = objectName.Name.ToUpperInvariant();
        if (objectName.HasSchema)
        {
            return Convert.ToBoolean(database.ExecuteScalar(
                "SELECT CASE WHEN EXISTS (SELECT 1 FROM ALL_TABLES WHERE OWNER = :schema AND TABLE_NAME = :name) THEN 1 ELSE 0 END",
                [CreateParameter(database, ":schema", objectName.Schema.ToUpperInvariant()), CreateParameter(database, ":name", normalizedName)]));
        }

        return Convert.ToBoolean(database.ExecuteScalar(
            "SELECT CASE WHEN EXISTS (SELECT 1 FROM USER_TABLES WHERE TABLE_NAME = :name) THEN 1 ELSE 0 END",
            [CreateParameter(database, ":name", normalizedName)]));
    }
    public static bool TableHasIdentity(this DatabaseFacade database, string tableName)
    {
        var objectName = database.ParseObjectName(tableName);
        string normalizedName = objectName.Name.ToUpperInvariant();
        if (objectName.HasSchema)
        {
            return Convert.ToBoolean(database.ExecuteScalar(
                """
                SELECT CASE WHEN EXISTS (
                    SELECT 1 FROM ALL_TAB_COLUMNS
                    WHERE OWNER = :schema AND TABLE_NAME = :name AND IDENTITY_COLUMN = 'YES'
                ) THEN 1 ELSE 0 END
                """,
                [CreateParameter(database, ":schema", objectName.Schema.ToUpperInvariant()), CreateParameter(database, ":name", normalizedName)]));
        }

        return Convert.ToBoolean(database.ExecuteScalar(
            """
            SELECT CASE WHEN EXISTS (
                SELECT 1 FROM USER_TAB_COLUMNS
                WHERE TABLE_NAME = :name AND IDENTITY_COLUMN = 'YES'
            ) THEN 1 ELSE 0 END
            """,
            [CreateParameter(database, ":name", normalizedName)]));
    }
    internal static int CloneTable(this DatabaseFacade database, string sourceTable, string destinationTable, IEnumerable<string> columnNames, string internalIdColumnName = null)
    {
        return database.CloneTable([sourceTable], destinationTable, columnNames, internalIdColumnName);
    }

    internal static void MarkTransactionalCloneTable(this DatabaseFacade database, string tableName)
    {
        TransactionalCloneTables[GetTransactionalCloneTableKey(database, tableName)] = 0;
    }

    private static string GetTransactionalCloneTableKey(DatabaseFacade database, string tableName)
    {
        var objectName = database.ParseObjectName(tableName);
        var schema = objectName.HasSchema ? objectName.Schema : database.GetDefaultSchema();
        return $"{schema?.ToUpperInvariant()}.{objectName.Name.ToUpperInvariant()}";
    }

    internal static int CloneTable(this DatabaseFacade database, IEnumerable<string> sourceTables, string destinationTable, IEnumerable<string> columnNames, string internalIdColumnName = null)
    {
        string columns = columnNames != null && columnNames.Any() ? string.Join(",", columnNames.Select(database.FormatSelectColumn)) : "*";
        if (!string.IsNullOrEmpty(internalIdColumnName))
            columns = $"{columns},CAST(NULL AS NUMBER(10)) AS {database.DelimitIdentifier(internalIdColumnName)}";

        string sql = $"CREATE TABLE {destinationTable} AS SELECT {columns} FROM {string.Join(",", sourceTables)} WHERE 1=0";
        return database.ExecuteSqlRaw(sql);
    }
    internal static DbCommand CreateCommand(this DatabaseFacade database, ConnectionBehavior connectionBehavior = ConnectionBehavior.Default)
    {
        var dbConnection = database.GetDbConnection(connectionBehavior);
        if (dbConnection.State != ConnectionState.Open)
            dbConnection.Open();
        var command = dbConnection.CreateCommand();
        if (command is OracleCommand oracleCommand)
            oracleCommand.BindByName = true;
        if (database.CurrentTransaction != null && connectionBehavior == ConnectionBehavior.Default)
            command.Transaction = database.CurrentTransaction.GetDbTransaction();
        return command;
    }
    internal static int ExecuteSqlInternal(this DatabaseFacade database, string sql, int? commandTimeout = null, ConnectionBehavior connectionBehavior = default)
    {
        return database.ExecuteSql(sql, null, commandTimeout, connectionBehavior);
    }

    private static void RebuildUnusableIndexes(this DatabaseFacade database, IEnumerable<string> tableNames = null)
    {
        var indexNames = new List<string>();
        var normalizedTableNames = tableNames?
            .Select(tableName => database.ParseObjectName(tableName).Name.ToUpperInvariant())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        using (var command = database.CreateCommand())
        {
            command.CommandText = "SELECT INDEX_NAME FROM USER_INDEXES WHERE STATUS = 'UNUSABLE'";
            if (normalizedTableNames?.Length > 0)
            {
                var parameterNames = normalizedTableNames
                    .Select((_, index) => $":tableName{index}")
                    .ToArray();
                command.CommandText += $" AND TABLE_NAME IN ({string.Join(", ", parameterNames)})";

                for (int index = 0; index < normalizedTableNames.Length; index++)
                    command.Parameters.Add(new OracleParameter(parameterNames[index], normalizedTableNames[index]));
            }

            using var reader = command.ExecuteReader();
            while (reader.Read())
                indexNames.Add(reader.GetString(0));
        }

        foreach (var indexName in indexNames)
            database.ExecuteSqlInternal($"ALTER INDEX {database.DelimitIdentifier(indexName)} REBUILD");
    }

    private static void EnablePrimaryAndUniqueConstraints(this DatabaseFacade database, IEnumerable<string> tableNames)
    {
        var normalizedTableNames = tableNames?
            .Select(tableName => database.ParseObjectName(tableName).Name.ToUpperInvariant())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
        if (normalizedTableNames == null || normalizedTableNames.Length == 0)
            return;

        var constraints = new List<(string TableName, string ConstraintName)>();
        using (var command = database.CreateCommand())
        {
            var parameterNames = normalizedTableNames
                .Select((_, index) => $":tableName{index}")
                .ToArray();
            command.CommandText = $"""
                SELECT TABLE_NAME, CONSTRAINT_NAME
                FROM USER_CONSTRAINTS
                WHERE CONSTRAINT_TYPE IN ('P', 'U')
                  AND STATUS = 'DISABLED'
                  AND TABLE_NAME IN ({string.Join(", ", parameterNames)})
                """;

            for (int index = 0; index < normalizedTableNames.Length; index++)
                command.Parameters.Add(new OracleParameter(parameterNames[index], normalizedTableNames[index]));

            using var reader = command.ExecuteReader();
            while (reader.Read())
                constraints.Add((reader.GetString(0), reader.GetString(1)));
        }

        foreach (var (tableName, constraintName) in constraints)
            database.ExecuteSqlInternal($"ALTER TABLE {database.DelimitIdentifier(tableName)} ENABLE CONSTRAINT {database.DelimitIdentifier(constraintName)}");
    }

    private static void TruncateTableForRecovery(this DatabaseFacade database, string tableName)
    {
        var objectName = database.ParseObjectName(tableName);
        var normalizedTableName = objectName.Name.ToUpperInvariant();
        var childConstraints = new List<(string TableName, string ConstraintName)>();

        using (var command = database.CreateCommand())
        {
            command.CommandText = """
                SELECT child.TABLE_NAME, child.CONSTRAINT_NAME
                FROM USER_CONSTRAINTS child
                JOIN USER_CONSTRAINTS parent ON child.R_CONSTRAINT_NAME = parent.CONSTRAINT_NAME
                WHERE child.CONSTRAINT_TYPE = 'R'
                  AND parent.TABLE_NAME = :tableName
                """;
            command.Parameters.Add(new OracleParameter(":tableName", normalizedTableName));
            using var reader = command.ExecuteReader();
            while (reader.Read())
                childConstraints.Add((reader.GetString(0), reader.GetString(1)));
        }

        var affectedTableNames = childConstraints
            .Select(constraint => constraint.TableName)
            .Append(normalizedTableName)
            .ToArray();

        try
        {
            foreach (var (childTableName, constraintName) in childConstraints)
                database.ExecuteSqlInternal($"ALTER TABLE {database.DelimitIdentifier(childTableName)} DISABLE CONSTRAINT {database.DelimitIdentifier(constraintName)}");

            foreach (var (childTableName, _) in childConstraints)
                database.ExecuteSqlInternal($"TRUNCATE TABLE {database.DelimitIdentifier(childTableName)}");

            database.ExecuteSqlInternal($"TRUNCATE TABLE {database.DelimitTableName(tableName)}");
        }
        finally
        {
            database.RebuildUnusableIndexes(affectedTableNames);
            database.EnablePrimaryAndUniqueConstraints(affectedTableNames);
            foreach (var (childTableName, constraintName) in childConstraints)
                database.ExecuteSqlInternal($"ALTER TABLE {database.DelimitIdentifier(childTableName)} ENABLE CONSTRAINT {database.DelimitIdentifier(constraintName)}");
        }
    }

    internal static int ExecuteSql(this DatabaseFacade database, string sql, object[] parameters = null, int? commandTimeout = null, ConnectionBehavior connectionBehavior = default)
    {
        using var command = database.CreateCommand(connectionBehavior);
        command.CommandText = NormalizeOracleSqlParameters(sql);
        if (commandTimeout != null)
            command.CommandTimeout = commandTimeout.Value;
        if (parameters != null)
            AddParameters(command, parameters);
        return command.ExecuteNonQuery();
    }
    internal static object ExecuteScalar(this DatabaseFacade database, string query, object[] parameters = null, int? commandTimeout = null)
    {
        using var command = database.CreateCommand();
        command.CommandText = NormalizeOracleSqlParameters(query);
        if (commandTimeout.HasValue)
            command.CommandTimeout = commandTimeout.Value;
        if (parameters != null)
            AddParameters(command, parameters);
        return command.ExecuteScalar();
    }
    internal static void ToggleIdentityInsert(this DatabaseFacade database, string tableName, bool enable)
    {
        // Oracle identity columns do not require an IDENTITY_INSERT toggle.
    }
    internal static DbConnection GetDbConnection(this DatabaseFacade database, ConnectionBehavior connectionBehavior)
    {
        return connectionBehavior == ConnectionBehavior.New ? database.GetDbConnection().CloneConnection() : database.GetDbConnection();
    }

    private static DbParameter CreateParameter(DatabaseFacade database, string name, object value)
    {
        using var command = database.GetDbConnection().CreateCommand();
        var parameter = command.CreateParameter();
        parameter.ParameterName = NormalizeOracleParameterName(name);
        parameter.Value = value ?? DBNull.Value;
        return parameter;
    }
    internal static string NormalizeOracleSqlParameters(string sql) =>
        string.IsNullOrEmpty(sql)
            ? sql
            : Regex.Replace(sql, @"(?<!:)\@([A-Za-z_][A-Za-z0-9_]*)", ":$1");

    internal static void AddParameters(DbCommand command, object[] parameters)
    {
        foreach (var parameter in parameters.OfType<DbParameter>())
        {
            parameter.ParameterName = NormalizeOracleParameterName(parameter.ParameterName);
            command.Parameters.Add(parameter);
        }
    }

    private static string NormalizeOracleParameterName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            return name;

        return name.StartsWith('@') ? $":{name[1..]}" : name;
    }
    internal static string FormatSelectColumn(this DatabaseFacade database, string columnName)
    {
        if (columnName.Contains('[') || columnName.Contains('"') || columnName.Contains('(') || columnName.Contains(' '))
            return columnName;

        if (columnName.Contains('.'))
            return string.Join(".", columnName.Split('.').Select(database.DelimitIdentifier));

        return database.DelimitIdentifier(columnName);
    }
}
