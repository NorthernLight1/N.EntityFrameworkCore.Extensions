using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using N.EntityFrameworkCore.Extensions.Common;
using N.EntityFrameworkCore.Extensions.Enums;
using N.EntityFrameworkCore.Extensions.Extensions;
using N.EntityFrameworkCore.Extensions.Sql;
using N.EntityFrameworkCore.Extensions.Util;

namespace N.EntityFrameworkCore.Extensions;

public static class DbContextExtensionsAsync
{
    public static async Task<int> BulkDeleteAsync<T>(this DbContext context, IEnumerable<T> entities, CancellationToken cancellationToken = default)
    {
        return await context.BulkDeleteAsync(entities, new BulkDeleteOptions<T>(), cancellationToken);
    }
    public static async Task<int> BulkDeleteAsync<T>(this DbContext context, IEnumerable<T> entities, Action<BulkDeleteOptions<T>> optionsAction, CancellationToken cancellationToken = default)
    {
        return await context.BulkDeleteAsync(entities, optionsAction.Build(), cancellationToken);
    }
    public static async Task<int> BulkDeleteAsync<T>(this DbContext context, IEnumerable<T> entities, BulkDeleteOptions<T> options, CancellationToken cancellationToken = default)
    {
        int rowsAffected = 0;
        var tableMapping = context.GetTableMapping(typeof(T), options.EntityType);

        using (var dbTransactionContext = new DbTransactionContext(context, options))
        {
            var dbConnection = dbTransactionContext.Connection;
            var transaction = dbTransactionContext.CurrentTransaction;
            try
            {
                string stagingTableName = CommonUtil.GetStagingTableName(tableMapping, options.UsePermanentTable, dbConnection);
                string destinationTableName = context.DelimitIdentifier(tableMapping.TableName, tableMapping.Schema);
                string[] keyColumnNames = options.DeleteOnCondition != null ? CommonUtil<T>.GetColumns(options.DeleteOnCondition, ["s"])
                    : tableMapping.GetPrimaryKeyColumns().ToArray();

                if (keyColumnNames.Length == 0 && options.DeleteOnCondition == null)
                    throw new InvalidDataException("BulkDelete requires that the entity have a primary key or the Options.DeleteOnCondition must be set.");

                await context.Database.CloneTableAsync(destinationTableName, stagingTableName, keyColumnNames, null, cancellationToken, isTemporary: !options.UsePermanentTable);
                await BulkInsertAsync(entities, options, tableMapping, dbConnection, transaction, stagingTableName, keyColumnNames,
                    false, cancellationToken);
                string joinCondition = CommonUtil<T>.GetJoinConditionSql(context, options.DeleteOnCondition, keyColumnNames);
                // MySQL multi-table DELETE syntax
                string deleteSql = $"DELETE t FROM {stagingTableName} s JOIN {destinationTableName} t ON {joinCondition}";
                rowsAffected = await context.Database.ExecuteSqlRawAsync(deleteSql, cancellationToken);
                context.Database.DropTable(stagingTableName, isTemporary: !options.UsePermanentTable);
                dbTransactionContext.Commit();
            }
            catch (Exception)
            {
                dbTransactionContext.Rollback();
                throw;
            }
            return rowsAffected;
        }
    }
    public static async Task<IEnumerable<T>> BulkFetchAsync<T, U>(this DbSet<T> dbSet, IEnumerable<U> entities, CancellationToken cancellationToken = default) where T : class, new()
    {
        return await dbSet.BulkFetchAsync(entities, new BulkFetchOptions<T>(), cancellationToken);
    }
    public static async Task<IEnumerable<T>> BulkFetchAsync<T, U>(this DbSet<T> dbSet, IEnumerable<U> entities, Action<BulkFetchOptions<T>> optionsAction, CancellationToken cancellationToken = default) where T : class, new()
    {
        return await dbSet.BulkFetchAsync(entities, optionsAction.Build(), cancellationToken);
    }
    public static async Task<IEnumerable<T>> BulkFetchAsync<T, U>(this DbSet<T> dbSet, IEnumerable<U> entities, BulkFetchOptions<T> options, CancellationToken cancellationToken = default) where T : class, new()
    {
        var context = dbSet.GetDbContext();
        var tableMapping = context.GetTableMapping(typeof(T));

        using (var dbTransactionContext = new DbTransactionContext(context, options.CommandTimeout, ConnectionBehavior.New))
        {
            string selectSql;
            var dbConnection = dbTransactionContext.Connection;
            var transaction = dbTransactionContext.CurrentTransaction;
            string stagingTableName = string.Empty;
            try
            {
                stagingTableName = CommonUtil.GetStagingTableName(tableMapping, true, dbConnection);
                string destinationTableName = context.DelimitIdentifier(tableMapping.TableName, tableMapping.Schema);
                string[] keyColumnNames = options.JoinOnCondition != null ? CommonUtil<T>.GetColumns(options.JoinOnCondition, ["s"])
                    : tableMapping.GetPrimaryKeyColumns().ToArray();
                IEnumerable<string> columnNames = CommonUtil.FilterColumns<T>(tableMapping.GetColumns(true), keyColumnNames, options.InputColumns, options.IgnoreColumns);
                IEnumerable<string> columnsToFetch = CommonUtil.FormatColumns(context, "t", columnNames);

                if (keyColumnNames.Length == 0 && options.JoinOnCondition == null)
                    throw new InvalidDataException("BulkFetch requires that the entity have a primary key or the Options.JoinOnCondition must be set.");

                await context.Database.CloneTableAsync(destinationTableName, stagingTableName, keyColumnNames, null, cancellationToken);
                await BulkInsertAsync(entities, options, tableMapping, dbConnection, transaction, stagingTableName, keyColumnNames, false, cancellationToken);
                selectSql = $"SELECT {SqlUtil.ConvertToColumnString(columnsToFetch)} FROM {stagingTableName} s JOIN {destinationTableName} t ON {CommonUtil<T>.GetJoinConditionSql(context, options.JoinOnCondition, keyColumnNames)}";

                dbTransactionContext.Commit();
            }
            catch
            {
                dbTransactionContext.Rollback();
                throw;
            }

            var results = await context.FetchInternalAsync<T>(selectSql, cancellationToken: cancellationToken);
            context.Database.DropTable(stagingTableName);
            return results;
        }
    }
    public static async Task FetchAsync<T>(this IQueryable<T> queryable, Func<FetchResult<T>, Task> action, Action<FetchOptions<T>> optionsAction, CancellationToken cancellationToken = default) where T : class, new()
    {
        await FetchAsync(queryable, action, optionsAction.Build(), cancellationToken);
    }
    public static async Task FetchAsync<T>(this IQueryable<T> queryable, Func<FetchResult<T>, Task> action, FetchOptions<T> options, CancellationToken cancellationToken = default) where T : class, new()
    {
        var dbContext = queryable.GetDbContext();
        var tableMapping = dbContext.GetTableMapping(typeof(T));
        HashSet<string> includedColumns = GetIncludedColumns(tableMapping, options.InputColumns, options.IgnoreColumns);
        int batch = 1;
        int count = 0;
        List<T> entities = [];
        await foreach (var entity in queryable.AsNoTracking().AsAsyncEnumerable().WithCancellation(cancellationToken))
        {
            ClearExcludedColumns(dbContext, tableMapping, entity, includedColumns);
            entities.Add(entity);
            count++;
            if (count == options.BatchSize)
            {
                await action(new FetchResult<T> { Results = entities, Batch = batch });
                entities.Clear();
                count = 0;
                batch++;
            }
            cancellationToken.ThrowIfCancellationRequested();
        }

        if (entities.Count > 0)
            await action(new FetchResult<T> { Results = entities, Batch = batch });
    }
    public static async Task<int> BulkInsertAsync<T>(this DbContext context, IEnumerable<T> entities, CancellationToken cancellationToken = default)
    {
        return await context.BulkInsertAsync<T>(entities, new BulkInsertOptions<T>(), cancellationToken);
    }
    public static async Task<int> BulkInsertAsync<T>(this DbContext context, IEnumerable<T> entities, Action<BulkInsertOptions<T>> optionsAction, CancellationToken cancellationToken = default)
    {
        return await context.BulkInsertAsync<T>(entities, optionsAction.Build(), cancellationToken);
    }
    public static async Task<int> BulkInsertAsync<T>(this DbContext context, IEnumerable<T> entities, BulkInsertOptions<T> options, CancellationToken cancellationToken = default)
    {
        int rowsAffected = 0;
        using (var bulkOperation = new BulkOperation<T>(context, options, options.InputColumns, options.IgnoreColumns))
        {
            try
            {
                bool keepIdentity = options.KeepIdentity || bulkOperation.ShouldPreallocateIdentityValues(options.AutoMapOutput, options.KeepIdentity, entities);
                if (keepIdentity && !options.KeepIdentity)
                    await bulkOperation.PreallocateIdentityValuesAsync(entities, cancellationToken);
                var bulkInsertResult = await bulkOperation.BulkInsertStagingDataAsync(entities, true, true);
                var bulkMergeResult = await bulkOperation.ExecuteMergeAsync(bulkInsertResult.EntityMap, options.InsertOnCondition,
                    options.AutoMapOutput, keepIdentity, options.InsertIfNotExists);
                rowsAffected = bulkMergeResult.RowsAffected;
                bulkOperation.DbTransactionContext.Commit();
            }
            catch (Exception)
            {
                bulkOperation.DbTransactionContext.Rollback();
                throw;
            }
        }
        return rowsAffected;
    }
    public static async Task<BulkMergeResult<T>> BulkMergeAsync<T>(this DbContext context, IEnumerable<T> entities, CancellationToken cancellationToken = default)
    {
        return await BulkMergeAsync(context, entities, new BulkMergeOptions<T>(), cancellationToken);
    }
    public static async Task<BulkMergeResult<T>> BulkMergeAsync<T>(this DbContext context, IEnumerable<T> entities, BulkMergeOptions<T> options, CancellationToken cancellationToken = default)
    {
        return await InternalBulkMergeAsync(context, entities, options, cancellationToken);
    }
    public static async Task<BulkMergeResult<T>> BulkMergeAsync<T>(this DbContext context, IEnumerable<T> entities, Action<BulkMergeOptions<T>> optionsAction, CancellationToken cancellationToken = default)
    {
        return await BulkMergeAsync(context, entities, optionsAction.Build(), cancellationToken);
    }
    public static async Task<int> BulkSaveChangesAsync(this DbContext dbContext)
    {
        return await dbContext.BulkSaveChangesAsync(true);
    }
    public static async Task<int> BulkSaveChangesAsync(this DbContext dbContext, bool acceptAllChangesOnSuccess = true)
    {
        int rowsAffected = 0;
        var stateManager = dbContext.GetDependencies().StateManager;

        dbContext.ChangeTracker.DetectChanges();
        var entries = stateManager.GetEntriesToSave(true);

        foreach (var saveEntryGroup in entries.GroupBy(o => new { o.EntityType, o.EntityState }))
        {
            var key = saveEntryGroup.Key;
            var entities = saveEntryGroup.AsEnumerable();
            if (key.EntityState == EntityState.Added)
            {
                rowsAffected += await dbContext.BulkInsertAsync(entities, o => { o.EntityType = key.EntityType; });
            }
            else if (key.EntityState == EntityState.Modified)
            {
                rowsAffected += await dbContext.BulkUpdateAsync(entities, o => { o.EntityType = key.EntityType; });
            }
            else if (key.EntityState == EntityState.Deleted)
            {
                rowsAffected += await dbContext.BulkDeleteAsync(entities, o => { o.EntityType = key.EntityType; });
            }
        }

        if (acceptAllChangesOnSuccess)
            dbContext.ChangeTracker.AcceptAllChanges();

        return rowsAffected;
    }
    public static async Task<BulkSyncResult<T>> BulkSyncAsync<T>(this DbContext context, IEnumerable<T> entities, CancellationToken cancellationToken = default)
    {
        return await BulkSyncAsync(context, entities, new BulkSyncOptions<T>(), cancellationToken);
    }
    public static async Task<BulkSyncResult<T>> BulkSyncAsync<T>(this DbContext context, IEnumerable<T> entities, Action<BulkSyncOptions<T>> optionsAction, CancellationToken cancellationToken = default)
    {
        return BulkSyncResult<T>.Map(await InternalBulkMergeAsync(context, entities, optionsAction.Build(), cancellationToken));
    }
    public static async Task<BulkSyncResult<T>> BulkSyncAsync<T>(this DbContext context, IEnumerable<T> entities, BulkSyncOptions<T> options, CancellationToken cancellationToken = default)
    {
        return BulkSyncResult<T>.Map(await InternalBulkMergeAsync(context, entities, options, cancellationToken));
    }
    public static async Task<int> BulkUpdateAsync<T>(this DbContext context, IEnumerable<T> entities, CancellationToken cancellationToken = default)
    {
        return await BulkUpdateAsync<T>(context, entities, new BulkUpdateOptions<T>(), cancellationToken);
    }
    public static async Task<int> BulkUpdateAsync<T>(this DbContext context, IEnumerable<T> entities, Action<BulkUpdateOptions<T>> optionsAction, CancellationToken cancellationToken = default)
    {
        return await BulkUpdateAsync<T>(context, entities, optionsAction.Build(), cancellationToken);
    }
    public static async Task<int> BulkUpdateAsync<T>(this DbContext context, IEnumerable<T> entities, BulkUpdateOptions<T> options, CancellationToken cancellationToken = default)
    {
        int rowsUpdated = 0;
        using (var bulkOperation = new BulkOperation<T>(context, options, options.InputColumns, options.IgnoreColumns))
        {
            try
            {
                bulkOperation.ValidateBulkUpdate(options.UpdateOnCondition);
                await bulkOperation.BulkInsertStagingDataAsync(entities, cancellationToken: cancellationToken);
                rowsUpdated = await bulkOperation.ExecuteUpdateAsync(entities, options.UpdateOnCondition, cancellationToken);
                bulkOperation.DbTransactionContext.Commit();
            }
            catch (Exception)
            {
                bulkOperation.DbTransactionContext.Rollback();
                throw;
            }
        }
        return rowsUpdated;
    }
    public static async Task<int> DeleteFromQueryAsync<T>(this IQueryable<T> queryable, int? commandTimeout = null, CancellationToken cancellationToken = default) where T : class
    {
        var dbContext = queryable.GetDbContext();
        using (var dbTransactionContext = new DbTransactionContext(dbContext, commandTimeout))
        {
            try
            {
                int rowsAffected = await queryable.ExecuteDeleteAsync(cancellationToken);
                dbTransactionContext.Commit();
                return rowsAffected;
            }
            catch (Exception)
            {
                dbTransactionContext.Rollback();
                throw;
            }
        }
    }
    public static async Task<int> InsertFromQueryAsync<T>(this IQueryable<T> queryable, string tableName, Expression<Func<T, object>> insertObjectExpression, int? commandTimeout = null,
        CancellationToken cancellationToken = default) where T : class
    {
        var dbContext = queryable.GetDbContext();
        using (var dbTransactionContext = new DbTransactionContext(dbContext, commandTimeout))
        {
            try
            {
                var tableMapping = dbContext.GetTableMapping(typeof(T));
                var columnNames = insertObjectExpression.GetObjectProperties();
                if (!dbContext.Database.TableExists(tableName))
                {
                    await dbContext.Database.CloneTableAsync(tableMapping.FullQualifedTableName, dbContext.Database.DelimitTableName(tableName), tableMapping.GetQualifiedColumnNames(columnNames), cancellationToken: cancellationToken);
                }

                var entities = await queryable.AsNoTracking().ToListAsync(cancellationToken);
                int rowsAffected = (int)(await BulkInsertAsync(entities, new BulkInsertOptions<T> { KeepIdentity = true, AutoMapOutput = false, CommandTimeout = commandTimeout }, tableMapping,
                    dbTransactionContext.Connection, dbTransactionContext.CurrentTransaction, dbContext.Database.DelimitTableName(tableName), columnNames, cancellationToken: cancellationToken)).RowsAffected;
                dbTransactionContext.Commit();
                return rowsAffected;
            }
            catch (Exception)
            {
                dbTransactionContext.Rollback();
                throw;
            }
        }
    }
    public static async Task<int> UpdateFromQueryAsync<T>(this IQueryable<T> queryable, Expression<Func<T, T>> updateExpression, int? commandTimeout = null,
        CancellationToken cancellationToken = default) where T : class
    {
        var dbContext = queryable.GetDbContext();
        using (var dbTransactionContext = new DbTransactionContext(dbContext, commandTimeout))
        {
            try
            {
                int rowsAffected = await queryable.ExecuteUpdateAsync(BuildSetPropertyCalls(updateExpression), cancellationToken);
                dbTransactionContext.Commit();
                return rowsAffected;
            }
            catch (Exception)
            {
                dbTransactionContext.Rollback();
                throw;
            }
        }
    }
    public static async Task<QueryToFileResult> QueryToCsvFileAsync<T>(this IQueryable<T> queryable, string filePath, CancellationToken cancellationToken = default) where T : class
    {
        return await QueryToCsvFileAsync<T>(queryable, filePath, new QueryToFileOptions(), cancellationToken);
    }
    public static async Task<QueryToFileResult> QueryToCsvFileAsync<T>(this IQueryable<T> queryable, Stream stream, CancellationToken cancellationToken = default) where T : class
    {
        return await QueryToCsvFileAsync<T>(queryable, stream, new QueryToFileOptions(), cancellationToken);
    }
    public static async Task<QueryToFileResult> QueryToCsvFileAsync<T>(this IQueryable<T> queryable, string filePath, Action<QueryToFileOptions> optionsAction,
        CancellationToken cancellationToken = default) where T : class
    {
        return await QueryToCsvFileAsync<T>(queryable, filePath, optionsAction.Build(), cancellationToken);
    }
    public static async Task<QueryToFileResult> QueryToCsvFileAsync<T>(this IQueryable<T> queryable, Stream stream, Action<QueryToFileOptions> optionsAction,
        CancellationToken cancellationToken = default) where T : class
    {
        return await QueryToCsvFileAsync<T>(queryable, stream, optionsAction.Build(), cancellationToken);
    }
    public static async Task<QueryToFileResult> QueryToCsvFileAsync<T>(this IQueryable<T> queryable, string filePath, QueryToFileOptions options,
        CancellationToken cancellationToken = default) where T : class
    {
        await using var fileStream = File.Create(filePath);
        return await QueryToCsvFileAsync<T>(queryable, fileStream, options, cancellationToken);
    }
    public static async Task<QueryToFileResult> QueryToCsvFileAsync<T>(this IQueryable<T> queryable, Stream stream, QueryToFileOptions options,
        CancellationToken cancellationToken = default) where T : class
    {
        return await InternalQueryToFileAsync<T>(queryable, stream, options, cancellationToken);
    }
    public static async Task<QueryToFileResult> SqlQueryToCsvFileAsync(this DatabaseFacade database, string filePath, string sqlText, object[] parameters,
        CancellationToken cancellationToken = default)
    {
        return await SqlQueryToCsvFileAsync(database, filePath, new QueryToFileOptions(), sqlText, parameters, cancellationToken);
    }
    public static async Task<QueryToFileResult> SqlQueryToCsvFileAsync(this DatabaseFacade database, Stream stream, string sqlText, object[] parameters,
        CancellationToken cancellationToken = default)
    {
        return await SqlQueryToCsvFileAsync(database, stream, new QueryToFileOptions(), sqlText, parameters, cancellationToken);
    }
    public static async Task<QueryToFileResult> SqlQueryToCsvFileAsync(this DatabaseFacade database, string filePath, Action<QueryToFileOptions> optionsAction, string sqlText, object[] parameters,
        CancellationToken cancellationToken = default)
    {
        return await SqlQueryToCsvFileAsync(database, filePath, optionsAction.Build(), sqlText, parameters, cancellationToken);
    }
    public static async Task<QueryToFileResult> SqlQueryToCsvFileAsync(this DatabaseFacade database, Stream stream, Action<QueryToFileOptions> optionsAction, string sqlText, object[] parameters,
        CancellationToken cancellationToken = default)
    {
        return await SqlQueryToCsvFileAsync(database, stream, optionsAction.Build(), sqlText, parameters, cancellationToken);
    }
    public static async Task<QueryToFileResult> SqlQueryToCsvFileAsync(this DatabaseFacade database, string filePath, QueryToFileOptions options, string sqlText, object[] parameters,
        CancellationToken cancellationToken = default)
    {
        await using var fileStream = File.Create(filePath);
        return await SqlQueryToCsvFileAsync(database, fileStream, options, sqlText, parameters, cancellationToken);
    }
    public static async Task<QueryToFileResult> SqlQueryToCsvFileAsync(this DatabaseFacade database, Stream stream, QueryToFileOptions options, string sqlText, object[] parameters,
        CancellationToken cancellationToken = default)
    {
        return await InternalQueryToFileAsync(database.GetDbConnection(), stream, options, sqlText, parameters, cancellationToken);
    }
    public static async Task ClearAsync<T>(this DbSet<T> dbSet, CancellationToken cancellationToken = default) where T : class
    {
        var dbContext = dbSet.GetDbContext();
        var tableMapping = dbContext.GetTableMapping(typeof(T));
        await dbContext.Database.ClearTableAsync(tableMapping.FullQualifedTableName, cancellationToken);
    }
    public static async Task TruncateAsync<T>(this DbSet<T> dbSet, CancellationToken cancellationToken = default) where T : class
    {
        var dbContext = dbSet.GetDbContext();
        var tableMapping = dbContext.GetTableMapping(typeof(T));
        await dbContext.Database.TruncateTableAsync(tableMapping.FullQualifedTableName, false, cancellationToken);
    }
    internal static async Task<BulkInsertResult<T>> BulkInsertAsync<T>(IEnumerable<T> entities, BulkOptions options, TableMapping tableMapping, DbConnection dbConnection, DbTransaction transaction, string tableName,
        IEnumerable<string> inputColumns = null, bool useInternalId = false, CancellationToken cancellationToken = default)
    {
        using var dataReader = new EntityDataReader<T>(tableMapping, entities, useInternalId);
        if (dbConnection is MySqlConnection mySqlConnection)
        {
            var includeColumns = DbContextExtensions.BuildIncludeColumns(dataReader, inputColumns, useInternalId);
            if (includeColumns.Count == 0)
                return new BulkInsertResult<T> { RowsAffected = 0, EntityMap = dataReader.EntityMap };

            string destTable = DbContextExtensions.UnwrapTableName(tableName);
            string columnList = string.Join(",", includeColumns.Select(c => $"`{c.name}`"));
            const int batchSize = 500;
            int totalInserted = 0;
            var rowBuffer = new List<object[]>(batchSize);

            await using var cmd = mySqlConnection.CreateCommand();
            cmd.Transaction = transaction as MySqlTransaction;
            if (options.CommandTimeout.HasValue)
                cmd.CommandTimeout = options.CommandTimeout.Value;

            async Task FlushBatchAsync()
            {
                if (rowBuffer.Count == 0) return;
                cmd.Parameters.Clear();
                var sb = new System.Text.StringBuilder($"INSERT INTO `{destTable}` ({columnList}) VALUES ");
                for (int r = 0; r < rowBuffer.Count; r++)
                {
                    if (r > 0) sb.Append(',');
                    sb.Append('(');
                    for (int c = 0; c < includeColumns.Count; c++)
                    {
                        if (c > 0) sb.Append(',');
                        string paramName = $"@p{r}_{c}";
                        sb.Append(paramName);
                        cmd.Parameters.AddWithValue(paramName, rowBuffer[r][c] ?? DBNull.Value);
                    }
                    sb.Append(')');
                }
                cmd.CommandText = sb.ToString();
                totalInserted += await cmd.ExecuteNonQueryAsync(cancellationToken);
                rowBuffer.Clear();
            }

            while (dataReader.Read())
            {
                var rowData = new object[includeColumns.Count];
                for (int i = 0; i < includeColumns.Count; i++)
                    rowData[i] = dataReader.GetValue(includeColumns[i].ordinal) ?? DBNull.Value;
                rowBuffer.Add(rowData);
                if (rowBuffer.Count >= batchSize)
                    await FlushBatchAsync();
            }
            await FlushBatchAsync();

            return new BulkInsertResult<T>
            {
                RowsAffected = totalInserted,
                EntityMap = dataReader.EntityMap
            };
        }

        throw new NotSupportedException($"The connection type '{dbConnection.GetType().Name}' is not supported for BulkInsertAsync. Use a MySqlConnection.");
    }
    internal static async Task<BulkQueryResult> BulkQueryAsync(this DbContext context, string sqlText, DbConnection dbConnection, DbTransaction transaction, BulkOptions options, CancellationToken cancellationToken = default)
    {
        List<object[]> results = [];
        List<string> columns = [];
        await using var command = dbConnection.CreateCommand();
        command.CommandText = sqlText;
        command.Transaction = transaction;
        if (options.CommandTimeout.HasValue)
            command.CommandTimeout = options.CommandTimeout.Value;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (columns.Count == 0)
            {
                for (int i = 0; i < reader.FieldCount; i++)
                    columns.Add(reader.GetName(i));
            }
            object[] values = new object[reader.FieldCount];
            reader.GetValues(values);
            results.Add(values);
        }

        return new BulkQueryResult
        {
            Columns = columns,
            Results = results,
            RowsAffected = reader.RecordsAffected
        };
    }
    private static async Task<BulkMergeResult<T>> InternalBulkMergeAsync<T>(this DbContext context, IEnumerable<T> entities, BulkMergeOptions<T> options, CancellationToken cancellationToken = default)
    {
        BulkMergeResult<T> bulkMergeResult;
        using (var bulkOperation = new BulkOperation<T>(context, options))
        {
            try
            {
                bool shouldPreallocate = bulkOperation.ShouldPreallocateIdentityValues(true, false, entities);
                bool keepIdentity = shouldPreallocate || bulkOperation.ShouldKeepIdentityForMerge();
                if (shouldPreallocate)
                    await bulkOperation.PreallocateIdentityValuesAsync(entities, cancellationToken);
                bulkOperation.ValidateBulkMerge(options.MergeOnCondition);
                var bulkInsertResult = await bulkOperation.BulkInsertStagingDataAsync(entities, true, true, cancellationToken);
                bulkMergeResult = await bulkOperation.ExecuteMergeAsync(bulkInsertResult.EntityMap, options.MergeOnCondition, options.AutoMapOutput,
                    keepIdentity, true, true, options.DeleteIfNotMatched, shouldPreallocate, cancellationToken);
                bulkOperation.DbTransactionContext.Commit();
            }
            catch (Exception)
            {
                bulkOperation.DbTransactionContext.Rollback();
                throw;
            }
        }
        return bulkMergeResult;
    }
    private static async Task<QueryToFileResult> InternalQueryToFileAsync<T>(this IQueryable<T> queryable, Stream stream, QueryToFileOptions options,
        CancellationToken cancellationToken = default) where T : class
    {
        return await InternalQueryToFileAsync(queryable.AsNoTracking().AsAsyncEnumerable(), stream, options, cancellationToken);
    }
    private static async Task<QueryToFileResult> InternalQueryToFileAsync(DbConnection dbConnection, Stream stream, QueryToFileOptions options, string sqlText, object[] parameters = null,
        CancellationToken cancellationToken = default)
    {
        int dataRowCount = 0;
        int totalRowCount = 0;
        long bytesWritten = 0;

        if (dbConnection.State == ConnectionState.Closed)
            dbConnection.Open();

        await using var command = dbConnection.CreateCommand();
        command.CommandText = sqlText;
        if (parameters != null)
            command.Parameters.AddRange(parameters);
        if (options.CommandTimeout.HasValue)
            command.CommandTimeout = options.CommandTimeout.Value;

        await using var streamWriter = new StreamWriter(stream, leaveOpen: true);
        using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            if (options.IncludeHeaderRow)
            {
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    streamWriter.Write(options.TextQualifer);
                    streamWriter.Write(reader.GetName(i));
                    streamWriter.Write(options.TextQualifer);
                    if (i != reader.FieldCount - 1)
                    {
                        await streamWriter.WriteAsync(options.ColumnDelimiter);
                    }
                }
                totalRowCount++;
                await streamWriter.WriteAsync(options.RowDelimiter);
            }
            while (await reader.ReadAsync(cancellationToken))
            {
                object[] values = new object[reader.FieldCount];
                reader.GetValues(values);
                for (int i = 0; i < values.Length; i++)
                {
                    streamWriter.Write(options.TextQualifer);
                    streamWriter.Write(values[i]);
                    streamWriter.Write(options.TextQualifer);
                    if (i != values.Length - 1)
                    {
                        await streamWriter.WriteAsync(options.ColumnDelimiter);
                    }
                }
                await streamWriter.WriteAsync(options.RowDelimiter);
                dataRowCount++;
                totalRowCount++;
            }
            await streamWriter.FlushAsync();
            bytesWritten = streamWriter.BaseStream.Length;
        }
        return new QueryToFileResult()
        {
            BytesWritten = bytesWritten,
            DataRowCount = dataRowCount,
            TotalRowCount = totalRowCount
        };
    }
    private static async Task<QueryToFileResult> InternalQueryToFileAsync<T>(IAsyncEnumerable<T> entities, Stream stream, QueryToFileOptions options, CancellationToken cancellationToken) where T : class
    {
        int dataRowCount = 0;
        int totalRowCount = 0;
        long bytesWritten = 0;
        var properties = typeof(T).GetProperties().Where(p => p.CanRead && (!typeof(System.Collections.IEnumerable).IsAssignableFrom(p.PropertyType) || p.PropertyType == typeof(string))).ToArray();

        await using var streamWriter = new StreamWriter(stream, leaveOpen: true);
        if (options.IncludeHeaderRow)
        {
            await WriteCsvRowAsync(streamWriter, properties.Select(p => (object)p.Name), options, cancellationToken);
            totalRowCount++;
        }

        await foreach (var entity in entities.WithCancellation(cancellationToken))
        {
            await WriteCsvRowAsync(streamWriter, properties.Select(p => p.GetValue(entity)), options, cancellationToken);
            dataRowCount++;
            totalRowCount++;
        }

        await streamWriter.FlushAsync(cancellationToken);
        bytesWritten = streamWriter.BaseStream.Length;
        return new QueryToFileResult { BytesWritten = bytesWritten, DataRowCount = dataRowCount, TotalRowCount = totalRowCount };
    }
    private static async Task<IEnumerable<T>> FetchInternalAsync<T>(this DbContext dbContext, string sqlText, object[] parameters = null, CancellationToken cancellationToken = default) where T : class, new()
    {
        List<T> results = [];
        await using var command = dbContext.Database.CreateCommand(ConnectionBehavior.New);
        command.CommandText = sqlText;
        if (parameters != null)
            command.Parameters.AddRange(parameters);

        var tableMapping = dbContext.GetTableMapping(typeof(T), null);
        var reader = await command.ExecuteReaderAsync(cancellationToken);
        var properties = reader.GetProperties(tableMapping);
        var valuesFromProvider = properties.Select(p => tableMapping.GetValueFromProvider(p)).ToArray();

        while (await reader.ReadAsync(cancellationToken))
        {
            var entity = reader.MapEntity<T>(dbContext, properties, valuesFromProvider);
            results.Add(entity);
        }

        await reader.CloseAsync();
        await command.Connection.CloseAsync();
        return results;
    }
    private static HashSet<string> GetIncludedColumns<T>(TableMapping tableMapping, Expression<Func<T, object>> inputColumns, Expression<Func<T, object>> ignoreColumns)
    {
        var includedColumns = inputColumns != null
            ? inputColumns.GetObjectProperties().ToHashSet()
            : tableMapping.Properties.Select(p => p.Name).ToHashSet();

        if (ignoreColumns != null)
            includedColumns.ExceptWith(ignoreColumns.GetObjectProperties());

        return includedColumns;
    }
    private static void ClearExcludedColumns<T>(DbContext dbContext, TableMapping tableMapping, T entity, HashSet<string> includedColumns) where T : class
    {
        var entry = dbContext.Entry(entity);
        foreach (var property in tableMapping.Properties)
        {
            if (includedColumns.Contains(property.Name))
                continue;

            object defaultValue = property.ClrType.IsValueType ? Activator.CreateInstance(property.ClrType) : null;
            if (property.DeclaringType is IComplexType complexType)
            {
                var complexProperty = entry.ComplexProperty(complexType.ComplexProperty);
                if (complexProperty.CurrentValue != null)
                    complexProperty.Property(property).CurrentValue = defaultValue;
            }
            else
            {
                entry.Property(property.Name).CurrentValue = defaultValue;
            }
        }
    }
    private static async Task WriteCsvRowAsync(TextWriter writer, IEnumerable<object> values, QueryToFileOptions options, CancellationToken cancellationToken)
    {
        bool first = true;
        foreach (var value in values)
        {
            if (!first)
                await writer.WriteAsync(options.ColumnDelimiter);

            await writer.WriteAsync(options.TextQualifer);
            await writer.WriteAsync(value?.ToString());
            await writer.WriteAsync(options.TextQualifer);
            first = false;
            cancellationToken.ThrowIfCancellationRequested();
        }
        await writer.WriteAsync(options.RowDelimiter);
    }
    private static Expression<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>> BuildSetPropertyCalls<T>(Expression<Func<T, T>> updateExpression) where T : class
    {
        if (updateExpression.Body is not MemberInitExpression memberInitExpression)
            throw new InvalidOperationException("UpdateFromQuery requires a member initialization expression.");

        var entityParameter = updateExpression.Parameters[0];
        var callsParam = Expression.Parameter(typeof(SetPropertyCalls<T>), "calls");
        var setPropertyMethod = typeof(SetPropertyCalls<T>)
            .GetMethods()
            .Single(m => m.Name == nameof(SetPropertyCalls<T>.SetProperty) && m.GetParameters().Length == 2 && m.GetParameters()[1].ParameterType.IsGenericType);

        Expression current = callsParam;
        foreach (var binding in memberInitExpression.Bindings.OfType<MemberAssignment>())
        {
            var propertyInfo = binding.Member as System.Reflection.PropertyInfo ?? throw new InvalidOperationException("Only property bindings are supported.");
            var propertyLambda = Expression.Lambda(Expression.Property(entityParameter, propertyInfo), entityParameter);
            var valueLambda = Expression.Lambda(binding.Expression, entityParameter);
            current = Expression.Call(current, setPropertyMethod.MakeGenericMethod(propertyInfo.PropertyType), propertyLambda, valueLambda);
        }

        return Expression.Lambda<Func<SetPropertyCalls<T>, SetPropertyCalls<T>>>(current, callsParam);
    }
}
