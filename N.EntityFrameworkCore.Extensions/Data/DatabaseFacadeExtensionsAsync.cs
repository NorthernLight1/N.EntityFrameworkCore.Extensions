using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using N.EntityFrameworkCore.Extensions.Util;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.Data;
using System;

namespace N.EntityFrameworkCore.Extensions
{
    public static class DabaseFacadeExtensionsAsync
    {
        public async static Task<int> ClearTableAsync(this DatabaseFacade database, string tableName, CancellationToken cancellationToken = default)
        {
            return await database.ExecuteSqlRawAsync(string.Format("DELETE FROM {0}", tableName), cancellationToken);
        }
        internal async static Task<int> CloneTableAsync(this DatabaseFacade database, string sourceTable, string destinationTable, IEnumerable<string> columnNames, string internalIdColumnName = null, CancellationToken cancellationToken = default)
        {
            return await database.CloneTableAsync(new string[] { sourceTable }, destinationTable, columnNames, internalIdColumnName, cancellationToken);
        }
        internal async static Task<int> CloneTableAsync(this DatabaseFacade database, IEnumerable<string> sourceTables, string destinationTable, IEnumerable<string> columnNames, string internalIdColumnName = null, CancellationToken cancellationToken = default)
        {
            string columns = columnNames != null && columnNames.Count() > 0 ? string.Join(",", CommonUtil.FormatColumns(columnNames)) : "*";
            columns = !string.IsNullOrEmpty(internalIdColumnName) ? string.Format("{0},CAST( NULL AS INT) AS {1}", columns, internalIdColumnName) : columns;
            return await database.ExecuteSqlRawAsync(string.Format("SELECT TOP 0 {0} INTO {1} FROM {2}", columns, destinationTable, string.Join(",", sourceTables)), cancellationToken);
        }
        public async static Task TruncateTableAsync(this DatabaseFacade database, string tableName, bool ifExists = false, CancellationToken cancellationToken = default)
        {
            bool truncateTable = !ifExists || (ifExists && database.TableExists(tableName)) ? true : false;
            if (truncateTable)
            {
                await database.ExecuteSqlRawAsync(string.Format("TRUNCATE TABLE {0}", tableName), cancellationToken);
            }
        }
        internal async static Task<int> ExecuteSqlAsync(this DatabaseFacade database, string sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            return await database.ExecuteSqlAsync(sql, null, commandTimeout, cancellationToken);
        }
        internal async static Task<int> ExecuteSqlAsync(this DatabaseFacade database, string sql, object[] parameters = null, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            int value = -1;
            int? origCommandTimeout = database.GetCommandTimeout();
            database.SetCommandTimeout(commandTimeout);
            if (parameters != null)
                value = await database.ExecuteSqlRawAsync(sql, parameters, cancellationToken);
            else
                value = await database.ExecuteSqlRawAsync(sql, cancellationToken);
            database.SetCommandTimeout(origCommandTimeout);
            return value;
        }
        internal async static Task<object> ExecuteScalarAsync(this DatabaseFacade database, string query, object[] parameters = null, int? commandTimeout = null, CancellationToken cancellationToken = default)
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
                value = await sqlCommand.ExecuteScalarAsync(cancellationToken);
            }
            return value;
        }
        internal async static Task ToggleIdentityInsertAsync(this DatabaseFacade database, string tableName, bool enable)
        {
            bool hasIdentity = database.TableHasIdentity(tableName);
            if (hasIdentity)
            {
                string boolString = enable ? "ON" : "OFF";
                await database.ExecuteSqlAsync($"SET IDENTITY_INSERT {tableName} {boolString}", database.GetCommandTimeout());
            }
        }
    }
}

