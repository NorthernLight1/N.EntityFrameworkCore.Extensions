using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using System.Threading;
using System.Threading.Tasks;

namespace N.EntityFrameworkCore.Extensions
{
    public static class DabaseFacadeExtensionsAsync
    {
        public async static Task<int> ClearTableAsync(this DatabaseFacade database, string tableName, CancellationToken cancellationToken = default)
        {
            return await database.ExecuteSqlRawAsync(string.Format("DELETE FROM {0}", tableName), cancellationToken);
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

