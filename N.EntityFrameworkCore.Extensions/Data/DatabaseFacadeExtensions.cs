using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Scaffolding.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using N.EntityFrameworkCore.Extensions.Enums;
using N.EntityFrameworkCore.Extensions.Util;

namespace N.EntityFrameworkCore.Extensions;

public static class DabaseFacadeExtensions
{
    public static SqlQuery FromSqlQuery(this DatabaseFacade database, string sqlText, params object[] parameters)
    {
            return new SqlQuery(database, sqlText, parameters);
        }
    public static int ClearTable(this DatabaseFacade database, string tableName)
    {
            return database.ExecuteSqlRaw(string.Format("DELETE FROM {0}", tableName));
        }
    internal static int CloneTable(this DatabaseFacade database, string sourceTable, string destinationTable, IEnumerable<string> columnNames, string internalIdColumnName = null)
    {
            return database.CloneTable(new string[] { sourceTable }, destinationTable, columnNames, internalIdColumnName);
        }
    internal static int CloneTable(this DatabaseFacade database, IEnumerable<string> sourceTables, string destinationTable, IEnumerable<string> columnNames, string internalIdColumnName = null)
    {
            string columns = columnNames != null && columnNames.Count() > 0 ? string.Join(",", CommonUtil.FormatColumns(columnNames)) : "*";
            columns = !string.IsNullOrEmpty(internalIdColumnName) ? string.Format("{0},CAST( NULL AS INT) AS {1}", columns, internalIdColumnName) : columns;
            return database.ExecuteSqlRaw(string.Format("SELECT TOP 0 {0} INTO {1} FROM {2}", columns, destinationTable, string.Join(",", sourceTables)));
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
    public static int DropTable(this DatabaseFacade database, string tableName, bool ifExists = false)
    {
            bool deleteTable = !ifExists || (ifExists && database.TableExists(tableName)) ? true : false;
            return deleteTable ? database.ExecuteSqlInternal(string.Format("DROP TABLE {0}", tableName), null, ConnectionBehavior.Default) : -1;
        }
    public static void TruncateTable(this DatabaseFacade database, string tableName, bool ifExists = false)
    {
            bool truncateTable = !ifExists || (ifExists && database.TableExists(tableName)) ? true : false;
            if (truncateTable)
            {
                database.ExecuteSqlRaw(string.Format("TRUNCATE TABLE {0}", tableName));
            }
        }
    public static bool TableExists(this DatabaseFacade database, string tableName)
    {
            return Convert.ToBoolean(database.ExecuteScalar(string.Format("SELECT CASE WHEN OBJECT_ID(N'{0}', N'U') IS NOT NULL THEN 1 ELSE 0 END", tableName)));
        }
    public static bool TableHasIdentity(this DatabaseFacade database, string tableName)
    {
            return Convert.ToBoolean(database.ExecuteScalar($"SELECT ISNULL(OBJECTPROPERTY(OBJECT_ID('{tableName}'), 'TableHasIdentity'), 0)"));
        }
    internal static int ExecuteSqlInternal(this DatabaseFacade database, string sql, int? commandTimeout = null, ConnectionBehavior connectionBehavior = default)
    {
            return database.ExecuteSql(sql, null, commandTimeout, connectionBehavior);
        }
    internal static int ExecuteSql(this DatabaseFacade database, string sql, object[] parameters = null, int? commandTimeout = null, ConnectionBehavior connectionBehavior = default)
    {
            var command = database.CreateCommand(connectionBehavior);
            command.CommandText = sql;
            if (commandTimeout != null)
            {
                command.CommandTimeout = commandTimeout.Value;
            }
            if (parameters != null)
            {
                command.Parameters.AddRange(parameters);
            }
            return command.ExecuteNonQuery();
        }
    internal static object ExecuteScalar(this DatabaseFacade database, string query, object[] parameters = null, int? commandTimeout = null)
    {
            object value;
            var dbConnection = database.GetDbConnection() as SqlConnection;
            using (var sqlCommand = dbConnection.CreateCommand())
            {
                sqlCommand.CommandText = query;
                if (database.CurrentTransaction != null)
                    sqlCommand.Transaction = database.CurrentTransaction.GetDbTransaction() as SqlTransaction;
                if (dbConnection.State == ConnectionState.Closed)
                    dbConnection.Open();
                if (commandTimeout.HasValue)
                    sqlCommand.CommandTimeout = commandTimeout.Value;
                if (parameters != null)
                    sqlCommand.Parameters.AddRange(parameters);
                value = sqlCommand.ExecuteScalar();
            }
            return value;
        }
    internal static void ToggleIdentityInsert(this DatabaseFacade database, string tableName, bool enable)
    {
            bool hasIdentity = database.TableHasIdentity(tableName);
            if (hasIdentity)
            {
                string boolString = enable ? "ON" : "OFF";
                database.ExecuteSql($"SET IDENTITY_INSERT {tableName} {boolString}");
            }
        }
    internal static DbConnection GetDbConnection(this DatabaseFacade database, ConnectionBehavior connectionBehavior)
    {
            return connectionBehavior == ConnectionBehavior.New ? ((ICloneable)database.GetDbConnection()).Clone() as DbConnection : database.GetDbConnection();
        }
}