using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using N.EntityFrameworkCore.Extensions.Util;

namespace N.EntityFrameworkCore.Extensions;

public static class DatabaseFacadeExtensionsAsync
{
    public static async Task<int> ClearTableAsync(this DatabaseFacade database, string tableName, CancellationToken cancellationToken = default)
    {
        string sql = $"DELETE FROM {database.DelimitTableName(tableName)}";
        return await database.ExecuteSqlRawAsync(sql, cancellationToken);
    }
    public static async Task TruncateTableAsync(this DatabaseFacade database, string tableName, bool ifExists = false, CancellationToken cancellationToken = default)
    {
        bool truncateTable = !ifExists || database.TableExists(tableName);
        if (!truncateTable)
            return;

        string formattedTableName = database.DelimitTableName(tableName);
        string sql = $"TRUNCATE TABLE {formattedTableName}";
        await database.ExecuteSqlRawAsync(sql, cancellationToken);
    }
    internal static async Task<int> CloneTableAsync(this DatabaseFacade database, string sourceTable, string destinationTable, IEnumerable<string> columnNames, string internalIdColumnName = null, CancellationToken cancellationToken = default)
    {
        return await database.CloneTableAsync([sourceTable], destinationTable, columnNames, internalIdColumnName, cancellationToken);
    }
    internal static async Task<int> CloneTableAsync(this DatabaseFacade database, IEnumerable<string> sourceTables, string destinationTable, IEnumerable<string> columnNames, string internalIdColumnName = null, CancellationToken cancellationToken = default)
    {
        string columns = columnNames != null && columnNames.Any() ? string.Join(",", columnNames.Select(database.FormatSelectColumn)) : "*";
        if (!string.IsNullOrEmpty(internalIdColumnName))
            columns = $"{columns},CAST(NULL AS INT) AS {database.DelimitIdentifier(internalIdColumnName)}";

        string sql = $"SELECT TOP 0 {columns} INTO {destinationTable} FROM {string.Join(",", sourceTables)}";
        return await database.ExecuteSqlRawAsync(sql, cancellationToken);
    }
    internal static async Task<int> ExecuteSqlAsync(this DatabaseFacade database, string sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
    {
        return await database.ExecuteSqlAsync(sql, null, commandTimeout, cancellationToken);
    }
    internal static async Task<int> ExecuteSqlAsync(this DatabaseFacade database, string sql, object[] parameters = null, int? commandTimeout = null, CancellationToken cancellationToken = default)
    {
        int value;
        int? origCommandTimeout = database.GetCommandTimeout();
        database.SetCommandTimeout(commandTimeout);
        value = parameters != null
            ? await database.ExecuteSqlRawAsync(sql, parameters, cancellationToken)
            : await database.ExecuteSqlRawAsync(sql, cancellationToken);
        database.SetCommandTimeout(origCommandTimeout);
        return value;
    }
    internal static async Task<object> ExecuteScalarAsync(this DatabaseFacade database, string query, object[] parameters = null, int? commandTimeout = null, CancellationToken cancellationToken = default)
    {
        await using var command = database.CreateCommand();
        command.CommandText = query;
        if (commandTimeout.HasValue)
            command.CommandTimeout = commandTimeout.Value;
        if (parameters != null)
            command.Parameters.AddRange(parameters);
        return await command.ExecuteScalarAsync(cancellationToken);
    }
    internal static async Task ToggleIdentityInsertAsync(this DatabaseFacade database, string tableName, bool enable)
    {
        bool hasIdentity = database.TableHasIdentity(tableName);
        if (hasIdentity)
        {
            string boolString = enable ? "ON" : "OFF";
            await database.ExecuteSqlAsync($"SET IDENTITY_INSERT {tableName} {boolString}", database.GetCommandTimeout());
        }
    }
}
