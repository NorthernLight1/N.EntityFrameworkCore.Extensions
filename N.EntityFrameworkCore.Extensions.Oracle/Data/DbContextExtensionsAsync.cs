using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
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
using Oracle.ManagedDataAccess.Client;

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
        return await Task.FromResult(OracleBulkOperations.BulkDelete(context, entities, options));
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
        return await Task.FromResult(OracleBulkOperations.BulkFetch(context, dbSet, entities, options));
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
        return await Task.FromResult(OracleBulkOperations.BulkInsert(context, entities, options));
    }
    public static async Task<BulkMergeResult<T>> BulkMergeAsync<T>(this DbContext context, IEnumerable<T> entities, CancellationToken cancellationToken = default)
    {
        return await BulkMergeAsync(context, entities, new BulkMergeOptions<T>(), cancellationToken);
    }
    public static async Task<BulkMergeResult<T>> BulkMergeAsync<T>(this DbContext context, IEnumerable<T> entities, BulkMergeOptions<T> options, CancellationToken cancellationToken = default)
    {
        return await Task.FromResult(OracleBulkOperations.BulkMerge(context, entities, options));
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
                var primaryKeyProperties = key.EntityType.FindPrimaryKey()?.Properties;
                bool hasExplicitIdentityKey = primaryKeyProperties != null
                    && primaryKeyProperties.Any(p => p.ValueGenerated != ValueGenerated.Never)
                    && primaryKeyProperties.All(p => !saveEntryGroup.First().HasTemporaryValue(p));
                rowsAffected += await dbContext.BulkInsertAsync(entities, o => { o.EntityType = key.EntityType; o.KeepIdentity = hasExplicitIdentityKey; });
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
        return await BulkSyncAsync(context, entities, optionsAction.Build(), cancellationToken);
    }
    public static async Task<BulkSyncResult<T>> BulkSyncAsync<T>(this DbContext context, IEnumerable<T> entities, BulkSyncOptions<T> options, CancellationToken cancellationToken = default)
    {
        return await Task.FromResult(OracleBulkOperations.BulkSync(context, entities, options));
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
        return await Task.FromResult(OracleBulkOperations.BulkUpdate(context, entities, options));
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
                    if (dbContext.Database.CurrentTransaction != null)
                        dbContext.Database.MarkTransactionalCloneTable(tableName);
                }

                var entities = await queryable.AsNoTracking().ToListAsync(cancellationToken);
                int rowsAffected = OracleBulkOperations.BulkInsertIntoTable(dbContext, entities, tableMapping, dbContext.Database.DelimitTableName(tableName), columnNames, commandTimeout);
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
        IEnumerable<string> inputColumns = null, SqlBulkCopyOptions bulkCopyOptions = SqlBulkCopyOptions.Default, bool useInternalId = false, CancellationToken cancellationToken = default)
    {
        using var dataReader = new EntityDataReader<T>(tableMapping, entities, useInternalId);
        using var sqlBulkCopy = new SqlBulkCopy((SqlConnection)dbConnection, bulkCopyOptions | options.BulkCopyOptions, (SqlTransaction)transaction)
        {
            DestinationTableName = tableName,
            BatchSize = options.BatchSize,
            NotifyAfter = options.NotifyAfter,
            EnableStreaming = options.EnableStreaming,
        };
        sqlBulkCopy.BulkCopyTimeout = options.CommandTimeout.HasValue ? options.CommandTimeout.Value : sqlBulkCopy.BulkCopyTimeout;
        if (options.SqlRowsCopied != null)
            sqlBulkCopy.SqlRowsCopied += options.SqlRowsCopied;
        foreach (SqlBulkCopyColumnOrderHint columnOrderHint in options.ColumnOrderHints)
            sqlBulkCopy.ColumnOrderHints.Add(columnOrderHint);
        foreach (var property in dataReader.TableMapping.Properties)
        {
            var columnName = dataReader.TableMapping.GetColumnName(property);
            if (inputColumns == null || inputColumns.Contains(columnName))
                sqlBulkCopy.ColumnMappings.Add(columnName, columnName);
        }
        if (useInternalId)
        {
            sqlBulkCopy.ColumnMappings.Add(Constants.InternalId_ColumnName, Constants.InternalId_ColumnName);
        }
        await sqlBulkCopy.WriteToServerAsync(dataReader, cancellationToken);

        return new BulkInsertResult<T>
        {
            RowsAffected = sqlBulkCopy.RowsCopied,
            EntityMap = dataReader.EntityMap
        };
    }
    internal static async Task<BulkQueryResult> BulkQueryAsync(this DbContext context, string sqlText, DbConnection dbConnection, DbTransaction transaction, BulkOptions options, CancellationToken cancellationToken = default)
    {
        List<object[]> results = [];
        List<string> columns = [];
        await using var command = dbConnection.CreateCommand();
        if (command is OracleCommand oracleCommand)
            oracleCommand.BindByName = true;
        command.CommandText = DatabaseFacadeExtensions.NormalizeOracleSqlParameters(sqlText);
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
                bulkOperation.ValidateBulkMerge(options.MergeOnCondition);
                var bulkInsertResult = await bulkOperation.BulkInsertStagingDataAsync(entities, true, true, cancellationToken);
                bulkMergeResult = await bulkOperation.ExecuteMergeAsync(bulkInsertResult.EntityMap, options.MergeOnCondition, options.AutoMapOutput,
                    false, true, true, options.DeleteIfNotMatched, cancellationToken);
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
        if (command is OracleCommand oracleCommand)
            oracleCommand.BindByName = true;
        command.CommandText = DatabaseFacadeExtensions.NormalizeOracleSqlParameters(sqlText);
        if (parameters != null)
            DatabaseFacadeExtensions.AddParameters(command, parameters);
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
        await using var connection = dbContext.GetDbConnection(ConnectionBehavior.New);
        if (connection.State == ConnectionState.Closed)
            await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        if (command is OracleCommand oracleCommand)
            oracleCommand.BindByName = true;
        command.CommandText = DatabaseFacadeExtensions.NormalizeOracleSqlParameters(sqlText);
        if (parameters != null)
            DatabaseFacadeExtensions.AddParameters(command, parameters);

        var tableMapping = dbContext.GetTableMapping(typeof(T), null);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var properties = reader.GetProperties(tableMapping);
        var valuesFromProvider = properties.Select(p => tableMapping.GetValueFromProvider(p)).ToArray();

        while (await reader.ReadAsync(cancellationToken))
        {
            var entity = reader.MapEntity<T>(dbContext, properties, valuesFromProvider);
            results.Add(entity);
        }

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
    private static Action<UpdateSettersBuilder<T>> BuildSetPropertyCalls<T>(Expression<Func<T, T>> updateExpression) where T : class
    {
        if (updateExpression.Body is not MemberInitExpression memberInitExpression)
            throw new InvalidOperationException("UpdateFromQuery requires a member initialization expression.");

        var entityParameter = updateExpression.Parameters[0];
        var setPropertyMethod = typeof(UpdateSettersBuilder<T>)
            .GetMethods()
            .Single(m => m.Name == nameof(UpdateSettersBuilder<T>.SetProperty) && m.GetParameters().Length == 2 && m.GetParameters()[1].ParameterType.IsGenericType);

        return setters =>
        {
            object currentBuilder = setters;
            foreach (var binding in memberInitExpression.Bindings.OfType<MemberAssignment>())
            {
                var propertyInfo = binding.Member as System.Reflection.PropertyInfo ?? throw new InvalidOperationException("Only property bindings are supported.");
                var propertyLambda = Expression.Lambda(Expression.Property(entityParameter, propertyInfo), entityParameter);
                var valueLambda = Expression.Lambda(binding.Expression, entityParameter);
                currentBuilder = setPropertyMethod.MakeGenericMethod(propertyInfo.PropertyType).Invoke(currentBuilder, [propertyLambda, valueLambda]);
            }
        };
    }
}
