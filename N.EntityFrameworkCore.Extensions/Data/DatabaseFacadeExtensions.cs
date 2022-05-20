using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using N.EntityFrameworkCore.Extensions.Util;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace N.EntityFrameworkCore.Extensions
{
    public static class DabaseFacadeExtensions
    {
        public static SqlQuery FromSqlQuery(this DatabaseFacade database, string sqlText, params object[] parameters)
        {
            var dbConnection = database.GetDbConnection() as SqlConnection;
            return new SqlQuery(dbConnection, sqlText, parameters);
        }
        public static int ClearTable(this DatabaseFacade database, string tableName)
        {
            return database.ExecuteSqlRaw(string.Format("DELETE FROM {0}", tableName));
        }
        internal static int CloneTable(this DatabaseFacade database, string sourceTable, string destinationTable, IEnumerable<string> columnNames, string internalIdColumnName = null)
        {
            string columns = columnNames != null && columnNames.Count() > 0 ? string.Join(",", CommonUtil.FormatColumns(columnNames)) : "*";
            columns = !string.IsNullOrEmpty(internalIdColumnName) ? string.Format("{0},CAST( NULL AS INT) AS {1}", columns, internalIdColumnName) : columns;
            return database.ExecuteSqlRaw(string.Format("SELECT TOP 0 {0} INTO {1} FROM {2}", columns, destinationTable, sourceTable));
        }
        public  static int DropTable(this DatabaseFacade database, string tableName, bool ifExists = false)
        {
            bool deleteTable = !ifExists || (ifExists && database.TableExists(tableName)) ? true : false;
            return deleteTable ? database.ExecuteSqlRaw(string.Format("DROP TABLE {0}", tableName)) : -1;
        }
        public static void TruncateTable(this DatabaseFacade database, string tableName, bool ifExists = false)
        {
            var dbConnection = database.GetDbConnection() as SqlConnection;
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
    }
}

