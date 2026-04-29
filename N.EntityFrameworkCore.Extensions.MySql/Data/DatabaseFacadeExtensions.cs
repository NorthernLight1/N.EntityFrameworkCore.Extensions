using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using N.EntityFrameworkCore.Extensions.Enums;
using N.EntityFrameworkCore.Extensions.Util;

namespace N.EntityFrameworkCore.Extensions;

public static class DatabaseFacadeExtensions
{
    public static SqlQuery FromSqlQuery(this DatabaseFacade database, string sqlText, params object[] parameters)
    {
        return new SqlQuery(database, sqlText, parameters);
    }
    public static int ClearTable(this DatabaseFacade database, string tableName)
    {
        string sql = $"DELETE FROM {database.DelimitTableName(tableName)}";
        return database.ExecuteSqlRaw(sql);
    }
    public static int DropTable(this DatabaseFacade database, string tableName, bool ifExists = false, bool isTemporary = false)
    {
        string formattedTableName = database.DelimitTableName(tableName);
        // Use DROP TEMPORARY TABLE for MySQL temporary staging tables to avoid implicit transaction commit
        string temporaryKeyword = isTemporary ? "TEMPORARY " : "";
        string sql = ifExists ? $"DROP {temporaryKeyword}TABLE IF EXISTS {formattedTableName}" : $"DROP {temporaryKeyword}TABLE {formattedTableName}";
        return database.ExecuteSqlInternal(sql, null, ConnectionBehavior.Default);
    }
    public static void TruncateTable(this DatabaseFacade database, string tableName, bool ifExists = false)
    {
        bool truncateTable = !ifExists || database.TableExists(tableName);
        if (!truncateTable)
            return;

        string formattedTableName = database.DelimitTableName(tableName);
        // MySQL TRUNCATE automatically resets AUTO_INCREMENT; PostgreSQL needs RESTART IDENTITY
        string sql = database.IsPostgreSql()
            ? $"TRUNCATE TABLE {formattedTableName} RESTART IDENTITY"
            : $"TRUNCATE TABLE {formattedTableName}";
        database.ExecuteSqlRaw(sql);
    }
    public static bool TableExists(this DatabaseFacade database, string tableName)
    {
        var objectName = database.ParseObjectName(tableName);
        if (database.IsMySql())
        {
            return Convert.ToBoolean(database.ExecuteScalar(
                "SELECT EXISTS (SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = @name)",
                [CreateParameter(database, "@name", objectName.Name)]));
        }
        return Convert.ToBoolean(database.ExecuteScalar(
            database.IsPostgreSql()
                ? "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = @schema AND table_name = @name)"
                : "SELECT CASE WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @name) THEN 1 ELSE 0 END",
            [CreateParameter(database, "@schema", objectName.Schema), CreateParameter(database, "@name", objectName.Name)]));
    }
    public static bool TableHasIdentity(this DatabaseFacade database, string tableName)
    {
        var objectName = database.ParseObjectName(tableName);
        if (database.IsMySql())
        {
            return Convert.ToBoolean(database.ExecuteScalar(
                "SELECT EXISTS (SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = @name AND EXTRA LIKE '%auto_increment%')",
                [CreateParameter(database, "@name", objectName.Name)]));
        }
        string sql = database.IsPostgreSql()
            ? """
              SELECT EXISTS (
                  SELECT 1
                  FROM information_schema.columns
                  WHERE table_schema = @schema
                    AND table_name = @name
                    AND (is_identity = 'YES' OR column_default LIKE 'nextval(%')
              )
              """
            : "SELECT ISNULL(OBJECTPROPERTY(OBJECT_ID(@fullName), 'TableHasIdentity'), 0)";

        object[] parameters = database.IsPostgreSql()
            ? [CreateParameter(database, "@schema", objectName.Schema), CreateParameter(database, "@name", objectName.Name)]
            : [CreateParameter(database, "@fullName", $"{objectName.Schema}.{objectName.Name}")];

        return Convert.ToBoolean(database.ExecuteScalar(sql, parameters));
    }
    internal static int CloneTable(this DatabaseFacade database, string sourceTable, string destinationTable, IEnumerable<string> columnNames, string internalIdColumnName = null, bool isTemporary = false)
    {
        return database.CloneTable([sourceTable], destinationTable, columnNames, internalIdColumnName, isTemporary);
    }
    internal static int CloneTable(this DatabaseFacade database, IEnumerable<string> sourceTables, string destinationTable, IEnumerable<string> columnNames, string internalIdColumnName = null, bool isTemporary = false)
    {
        string columns = columnNames != null && columnNames.Any() ? string.Join(",", columnNames.Select(database.FormatSelectColumn)) : "*";
        if (!string.IsNullOrEmpty(internalIdColumnName))
            columns = $"{columns},CAST(NULL AS SIGNED) AS {database.DelimitIdentifier(internalIdColumnName)}";

        // MySQL TEMPORARY tables do not cause implicit transaction commits (unlike regular DDL tables)
        string createKeyword = database.IsMySql() && isTemporary ? "CREATE TEMPORARY TABLE" : "CREATE TABLE";
        string sql = database.IsMySql()
            ? $"{createKeyword} {destinationTable} AS SELECT {columns} FROM {string.Join(",", sourceTables)} WHERE 1=0"
            : database.IsPostgreSql()
                ? $"CREATE TABLE {destinationTable} AS SELECT {columns} FROM {string.Join(",", sourceTables)} LIMIT 0"
                : $"SELECT TOP 0 {columns} INTO {destinationTable} FROM {string.Join(",", sourceTables)}";
        return database.ExecuteSqlInternal(sql);
    }
    internal static DbCommand CreateCommand(this DatabaseFacade database, ConnectionBehavior connectionBehavior = ConnectionBehavior.Default)
    {
        var dbConnection = database.GetDbConnection(connectionBehavior);
        if (dbConnection.State != ConnectionState.Open)
            dbConnection.Open();
        var command = dbConnection.CreateCommand();
        if (database.CurrentTransaction != null && connectionBehavior == ConnectionBehavior.Default)
            command.Transaction = database.CurrentTransaction.GetDbTransaction();
        return command;
    }
    internal static int ExecuteSqlInternal(this DatabaseFacade database, string sql, int? commandTimeout = null, ConnectionBehavior connectionBehavior = default)
    {
        return database.ExecuteSql(sql, null, commandTimeout, connectionBehavior);
    }
    internal static int ExecuteSql(this DatabaseFacade database, string sql, object[] parameters = null, int? commandTimeout = null, ConnectionBehavior connectionBehavior = default)
    {
        using var command = database.CreateCommand(connectionBehavior);
        command.CommandText = sql;
        if (commandTimeout != null)
            command.CommandTimeout = commandTimeout.Value;
        if (parameters != null)
            command.Parameters.AddRange(parameters);
        return command.ExecuteNonQuery();
    }
    internal static object ExecuteScalar(this DatabaseFacade database, string query, object[] parameters = null, int? commandTimeout = null)
    {
        using var command = database.CreateCommand();
        command.CommandText = query;
        if (commandTimeout.HasValue)
            command.CommandTimeout = commandTimeout.Value;
        if (parameters != null)
            command.Parameters.AddRange(parameters);
        return command.ExecuteScalar();
    }
    internal static void ToggleIdentityInsert(this DatabaseFacade database, string tableName, bool enable)
    {
        if (database.IsPostgreSql() || database.IsMySql())
            return;

        bool hasIdentity = database.TableHasIdentity(tableName);
        if (hasIdentity)
        {
            string boolString = enable ? "ON" : "OFF";
            database.ExecuteSql($"SET IDENTITY_INSERT {tableName} {boolString}");
        }
    }
    internal static DbConnection GetDbConnection(this DatabaseFacade database, ConnectionBehavior connectionBehavior)
    {
        return connectionBehavior == ConnectionBehavior.New ? database.GetDbConnection().CloneConnection() : database.GetDbConnection();
    }

    private static DbParameter CreateParameter(DatabaseFacade database, string name, object value)
    {
        using var command = database.GetDbConnection().CreateCommand();
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        return parameter;
    }
    internal static string FormatSelectColumn(this DatabaseFacade database, string columnName)
    {
        if (columnName.Contains('[') || columnName.Contains('"') || columnName.Contains('`') || columnName.Contains('(') || columnName.Contains(' '))
            return columnName;

        if (columnName.Contains('.'))
            return string.Join(".", columnName.Split('.').Select(database.DelimitIdentifier));

        return database.DelimitIdentifier(columnName);
    }
}
