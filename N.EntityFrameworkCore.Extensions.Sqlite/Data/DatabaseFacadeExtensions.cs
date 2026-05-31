using System;
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
        string sql = database.IsPostgreSql()
            ? $"TRUNCATE TABLE {formattedTableName} RESTART IDENTITY"
            : $"DELETE FROM {formattedTableName}";
        database.ExecuteSqlRaw(sql);
    }
    public static bool TableExists(this DatabaseFacade database, string tableName)
    {
        var objectName = database.ParseObjectName(tableName);
        if (database.IsSqliteProvider())
        {
            const string sqliteQuery = "SELECT CASE WHEN EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=@name) OR EXISTS(SELECT 1 FROM temp.sqlite_master WHERE type='table' AND name=@name) THEN 1 ELSE 0 END";
            return Convert.ToBoolean(database.ExecuteScalar(sqliteQuery, [CreateParameter(database, "@name", objectName.Name)]));
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
        if (database.IsSqliteProvider())
        {
            using var command = database.CreateCommand();
            command.CommandText = $"PRAGMA table_info({database.DelimitIdentifier(objectName.Name)})";
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                string type = reader.IsDBNull(2) ? string.Empty : reader.GetString(2);
                int pk = reader.IsDBNull(5) ? 0 : reader.GetInt32(5);
                if (pk > 0 && type.Equals("INTEGER", StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
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
    internal static int CloneTable(this DatabaseFacade database, string sourceTable, string destinationTable, IEnumerable<string> columnNames, string internalIdColumnName = null)
    {
        return database.CloneTable([sourceTable], destinationTable, columnNames, internalIdColumnName);
    }
    internal static int CloneTable(this DatabaseFacade database, IEnumerable<string> sourceTables, string destinationTable, IEnumerable<string> columnNames, string internalIdColumnName = null)
    {
        string columns = columnNames != null && columnNames.Any() ? string.Join(",", columnNames.Select(database.FormatSelectColumn)) : "*";
        if (!string.IsNullOrEmpty(internalIdColumnName))
            columns = $"{columns},CAST(NULL AS INTEGER) AS {database.DelimitIdentifier(internalIdColumnName)}";

        string sql;
        if (database.IsSqliteProvider())
        {
            string createKeyword = destinationTable.StartsWith("main.", StringComparison.OrdinalIgnoreCase) ? "CREATE TABLE" : "CREATE TEMP TABLE";
            sql = $"{createKeyword} {destinationTable} AS SELECT {columns} FROM {string.Join(",", sourceTables)} LIMIT 0";
        }
        else
        {
            sql = database.IsPostgreSql()
                ? $"CREATE TABLE {destinationTable} AS SELECT {columns} FROM {string.Join(",", sourceTables)} LIMIT 0"
                : $"SELECT TOP 0 {columns} INTO {destinationTable} FROM {string.Join(",", sourceTables)}";
        }
        return database.ExecuteSqlRaw(sql);
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
        query = RewriteDecimalComparisons(query, parameters);
        command.CommandText = query;
        if (commandTimeout.HasValue)
            command.CommandTimeout = commandTimeout.Value;
        if (parameters != null)
            command.Parameters.AddRange(parameters);
        return command.ExecuteScalar();
    }
    internal static void ToggleIdentityInsert(this DatabaseFacade database, string tableName, bool enable)
    {
        if (database.IsPostgreSql() || database.IsSqliteProvider())
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
    /// <summary>
    /// Rewrites SQL comparisons involving SqliteDecimalParameter so that TEXT-stored decimal columns
    /// are cast to REAL before comparison, enabling correct numeric ordering.
    /// Example: <c>"Price" &gt; @Price</c> → <c>CAST("Price" AS REAL) &gt; @Price</c>
    /// </summary>
    internal static string RewriteDecimalComparisons(string sql, object[] parameters)
    {
        if (parameters == null) return sql;
        foreach (var param in parameters)
        {
            if (param is SqliteDecimalParameter dp)
            {
                string escapedName = Regex.Escape(dp.ParameterName);
                sql = Regex.Replace(sql,
                    @"""(\w+)""\s*(>=|<=|<>|>|<|=)\s*" + escapedName,
                    @"CAST(""$1"" AS REAL) $2 " + dp.ParameterName,
                    RegexOptions.IgnoreCase);
            }
        }
        return sql;
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
