using Microsoft.Data.SqlClient;
using N.EntityFrameworkCore.Extensions.Util;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace N.EntityFrameworkCore.Extensions
{
    internal static class SqlUtil
    {
        internal static int ExecuteSql(string query, SqlConnection connection, SqlTransaction transaction, int? commandTimeout = null)
        {
            return SqlUtil.ExecuteSql(query, connection, transaction, null, commandTimeout);
        }
        internal static int ExecuteSql(string query, SqlConnection connection, SqlTransaction transaction, object[] parameters = null, int? commandTimeout = null)
        {
            var sqlCommand = new SqlCommand(query, connection, transaction);
            if (connection.State == ConnectionState.Closed)
                connection.Open();
            if (commandTimeout.HasValue)
                sqlCommand.CommandTimeout = commandTimeout.Value;
            if (parameters != null)
                sqlCommand.Parameters.AddRange(parameters);
            return sqlCommand.ExecuteNonQuery();
        }
        internal static object ExecuteScalar(string query, SqlConnection connection, SqlTransaction transaction, object[] parameters = null, int? commandTimeout = null)
        {
            var sqlCommand = new SqlCommand(query, connection, transaction);
            if (connection.State == ConnectionState.Closed)
                connection.Open();
            if (commandTimeout.HasValue)
                sqlCommand.CommandTimeout = commandTimeout.Value;
            if (parameters != null)
                sqlCommand.Parameters.AddRange(parameters);
            return sqlCommand.ExecuteScalar();
        }
        internal static string ConvertToColumnString(IEnumerable<string> columnNames)
        {
            return string.Join(",", columnNames);
        }
        internal static int ToggleIdentityInsert(bool enable, string tableName, SqlConnection dbConnection, SqlTransaction dbTransaction)
        {
            string boolString = enable ? "ON" : "OFF";
            return ExecuteSql(string.Format("SET IDENTITY_INSERT {0} {1}", tableName, boolString), dbConnection, dbTransaction, null);
        }
    }
}