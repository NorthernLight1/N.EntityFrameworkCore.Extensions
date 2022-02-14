using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

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
            var dbConnection = database.GetDbConnection() as SqlConnection;
            return SqlUtil.ClearTable(tableName, dbConnection, null);
        }
        internal static int CloneTable(this DatabaseFacade database, string sourceTable, string destinationTable, string[] columnNames, string internalIdColumnName = null)
        {
            string columns = columnNames != null && columnNames.Length > 0 ? string.Join(",", columnNames) : "*";
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
            bool truncateTable = !ifExists || (ifExists && SqlUtil.TableExists(tableName, dbConnection, null)) ? true : false;
            if (truncateTable)
            {
                SqlUtil.TruncateTable(tableName, dbConnection, null);
            }
        }
        public static bool TableExists(this DatabaseFacade database, string tableName)
        {
            var dbTransaction = database.CurrentTransaction != null ? database.CurrentTransaction.GetDbTransaction() as SqlTransaction : null;
            var dbConnection = database.GetDbConnection() as SqlConnection;
            return SqlUtil.TableExists(tableName, dbConnection, dbTransaction);
        }
    }
}

