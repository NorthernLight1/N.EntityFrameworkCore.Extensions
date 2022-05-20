using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using N.EntityFrameworkCore.Extensions.Util;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;

namespace N.EntityFrameworkCore.Extensions
{
    public static class DabaseFacadeExtensionsAsync
    {
        public async static Task<int> ClearTableAsync(this DatabaseFacade database, string tableName, CancellationToken cancellationToken = default)
        {
            return await database.ExecuteSqlRawAsync(string.Format("DELETE FROM {0}", tableName), cancellationToken);
        }
        internal async static Task<int> CloneTableAsync(this DatabaseFacade database, string sourceTable, string destinationTable, IEnumerable<string> columnNames, string internalIdColumnName = null)
        {
            string columns = columnNames != null && columnNames.Count() > 0 ? string.Join(",", CommonUtil.FormatColumns(columnNames)) : "*";
            columns = !string.IsNullOrEmpty(internalIdColumnName) ? string.Format("{0},CAST( NULL AS INT) AS {1}", columns, internalIdColumnName) : columns;
            return await database.ExecuteSqlRawAsync(string.Format("SELECT TOP 0 {0} INTO {1} FROM {2}", columns, destinationTable, sourceTable));
        }
        public async static Task TruncateTableAsync(this DatabaseFacade database, string tableName, bool ifExists = false, CancellationToken cancellationToken = default)
        {
            bool truncateTable = !ifExists || (ifExists && database.TableExists(tableName)) ? true : false;
            if (truncateTable)
            {
                await database.ExecuteSqlRawAsync(string.Format("TRUNCATE TABLE {0}", tableName), cancellationToken);
            }
        }
    }
}

