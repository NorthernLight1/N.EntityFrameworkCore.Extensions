using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using N.EntityFrameworkCore.Extensions.Common;
using N.EntityFrameworkCore.Extensions.Enums;
using N.EntityFrameworkCore.Extensions.Extensions;
using N.EntityFrameworkCore.Extensions.Sql;
using N.EntityFrameworkCore.Extensions.Util;
using Oracle.ManagedDataAccess.Client;

namespace N.EntityFrameworkCore.Extensions;

public static class DbContextExtensions
{
    private static readonly EfExtensionsCommandInterceptor efExtensionsCommandInterceptor;
    static DbContextExtensions()
    {
        efExtensionsCommandInterceptor = new EfExtensionsCommandInterceptor();
    }
    public static void SetupEfCoreExtensions(this DbContextOptionsBuilder builder)
    {
        builder.AddInterceptors(efExtensionsCommandInterceptor);
    }
    public static int BulkDelete<T>(this DbContext context, IEnumerable<T> entities)
    {
        return context.BulkDelete(entities, new BulkDeleteOptions<T>());
    }
    public static int BulkDelete<T>(this DbContext context, IEnumerable<T> entities, Action<BulkDeleteOptions<T>> optionsAction)
    {
        return context.BulkDelete(entities, optionsAction.Build());
    }
    public static int BulkDelete<T>(this DbContext context, IEnumerable<T> entities, BulkDeleteOptions<T> options)
    {
        return OracleBulkOperations.BulkDelete(context, entities, options);
    }
    public static IEnumerable<T> BulkFetch<T, U>(this DbSet<T> dbSet, IEnumerable<U> entities) where T : class, new()
    {
        return dbSet.BulkFetch(entities, new BulkFetchOptions<T>());
    }
    public static IEnumerable<T> BulkFetch<T, U>(this DbSet<T> dbSet, IEnumerable<U> entities, Action<BulkFetchOptions<T>> optionsAction) where T : class, new()
    {
        return dbSet.BulkFetch(entities, optionsAction.Build());
    }
    public static IEnumerable<T> BulkFetch<T, U>(this DbSet<T> dbSet, IEnumerable<U> entities, BulkFetchOptions<T> options) where T : class, new()
    {
        var context = dbSet.GetDbContext();
        return OracleBulkOperations.BulkFetch(context, dbSet, entities, options);
    }
    public static void Fetch<T>(this IQueryable<T> queryable, Action<FetchResult<T>> action, Action<FetchOptions<T>> optionsAction) where T : class, new()
    {
        Fetch(queryable, action, optionsAction.Build());
    }
    public static void Fetch<T>(this IQueryable<T> queryable, Action<FetchResult<T>> action, FetchOptions<T> options) where T : class, new()
    {
        var dbContext = queryable.GetDbContext();
        var tableMapping = dbContext.GetTableMapping(typeof(T));
        HashSet<string> includedColumns = GetIncludedColumns(tableMapping, options.InputColumns, options.IgnoreColumns);
        int batch = 1;
        int count = 0;
        List<T> entities = [];
        foreach (var entity in queryable.AsNoTracking().AsEnumerable())
        {
            ClearExcludedColumns(dbContext, tableMapping, entity, includedColumns);
            entities.Add(entity);
            count++;
            if (count == options.BatchSize)
            {
                action(new FetchResult<T> { Results = entities, Batch = batch });
                entities.Clear();
                count = 0;
                batch++;
            }
        }

        if (entities.Count > 0)
            action(new FetchResult<T> { Results = entities, Batch = batch });
    }
    public static int BulkInsert<T>(this DbContext context, IEnumerable<T> entities)
    {
        return context.BulkInsert<T>(entities, new BulkInsertOptions<T>());
    }
    public static int BulkInsert<T>(this DbContext context, IEnumerable<T> entities, Action<BulkInsertOptions<T>> optionsAction)
    {
        return context.BulkInsert<T>(entities, optionsAction.Build());
    }
    public static int BulkInsert<T>(this DbContext context, IEnumerable<T> entities, BulkInsertOptions<T> options)
    {
        return OracleBulkOperations.BulkInsert(context, entities, options);
    }
    public static BulkMergeResult<T> BulkMerge<T>(this DbContext context, IEnumerable<T> entities)
    {
        return BulkMerge(context, entities, new BulkMergeOptions<T>());
    }
    public static BulkMergeResult<T> BulkMerge<T>(this DbContext context, IEnumerable<T> entities, BulkMergeOptions<T> options)
    {
        return OracleBulkOperations.BulkMerge(context, entities, options);
    }
    public static BulkMergeResult<T> BulkMerge<T>(this DbContext context, IEnumerable<T> entities, Action<BulkMergeOptions<T>> optionsAction)
    {
        return BulkMerge(context, entities, optionsAction.Build());
    }
    public static int BulkSaveChanges(this DbContext dbContext)
    {
        return dbContext.BulkSaveChanges(true);
    }
    public static int BulkSaveChanges(this DbContext dbContext, bool acceptAllChangesOnSuccess = true)
    {
        int rowsAffected = 0;
        var stateManager = dbContext.GetDependencies().StateManager;

        dbContext.ChangeTracker.DetectChanges();
        var entries = stateManager.GetEntriesToSave(true);

        bool ownsTx = dbContext.Database.CurrentTransaction == null;
        using var tx = ownsTx ? dbContext.Database.BeginTransaction() : null;
        try
        {
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
                    rowsAffected += dbContext.BulkInsert(entities, o => { o.EntityType = key.EntityType; o.KeepIdentity = hasExplicitIdentityKey; });
                }
                else if (key.EntityState == EntityState.Modified)
                {
                    rowsAffected += dbContext.BulkUpdate(entities, o => { o.EntityType = key.EntityType; });
                }
                else if (key.EntityState == EntityState.Deleted)
                {
                    rowsAffected += dbContext.BulkDelete(entities, o => { o.EntityType = key.EntityType; });
                }
            }

            if (acceptAllChangesOnSuccess)
                dbContext.ChangeTracker.AcceptAllChanges();

            tx?.Commit();
        }
        catch
        {
            tx?.Rollback();
            throw;
        }

        return rowsAffected;
    }
    public static BulkSyncResult<T> BulkSync<T>(this DbContext context, IEnumerable<T> entities)
    {
        return BulkSync(context, entities, new BulkSyncOptions<T>());
    }
    public static BulkSyncResult<T> BulkSync<T>(this DbContext context, IEnumerable<T> entities, Action<BulkSyncOptions<T>> optionsAction)
    {
        return BulkSync(context, entities, optionsAction.Build());
    }
    public static BulkSyncResult<T> BulkSync<T>(this DbContext context, IEnumerable<T> entities, BulkSyncOptions<T> options)
    {
        return OracleBulkOperations.BulkSync(context, entities, options);
    }
    public static int BulkUpdate<T>(this DbContext context, IEnumerable<T> entities)
    {
        return BulkUpdate<T>(context, entities, new BulkUpdateOptions<T>());
    }
    public static int BulkUpdate<T>(this DbContext context, IEnumerable<T> entities, Action<BulkUpdateOptions<T>> optionsAction)
    {
        return BulkUpdate<T>(context, entities, optionsAction.Build());
    }
    public static int BulkUpdate<T>(this DbContext context, IEnumerable<T> entities, BulkUpdateOptions<T> options)
    {
        return OracleBulkOperations.BulkUpdate(context, entities, options);
    }
    public static int DeleteFromQuery<T>(this IQueryable<T> queryable, int? commandTimeout = null) where T : class
    {
        using (var dbTransactionContext = new DbTransactionContext(queryable.GetDbContext(), commandTimeout))
        {
            try
            {
                int rowsAffected = queryable.ExecuteDelete();
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
    public static int InsertFromQuery<T>(this IQueryable<T> queryable, string tableName, Expression<Func<T, object>> insertObjectExpression, int? commandTimeout = null) where T : class
    {
        using (var dbTransactionContext = new DbTransactionContext(queryable.GetDbContext(), commandTimeout))
        {
            var dbContext = dbTransactionContext.DbContext;
            try
            {
                var tableMapping = dbContext.GetTableMapping(typeof(T));
                var columnNames = insertObjectExpression.GetObjectProperties();
                if (!dbContext.Database.TableExists(tableName))
                {
                    dbContext.Database.CloneTable(tableMapping.FullQualifedTableName, dbContext.Database.DelimitTableName(tableName), tableMapping.GetQualifiedColumnNames(columnNames));
                    if (dbContext.Database.CurrentTransaction != null)
                        dbContext.Database.MarkTransactionalCloneTable(tableName);
                }

                var entities = queryable.AsNoTracking().ToList();
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
    public static int UpdateFromQuery<T>(this IQueryable<T> queryable, Expression<Func<T, T>> updateExpression, int? commandTimeout = null) where T : class
    {
        using (var dbTransactionContext = new DbTransactionContext(queryable.GetDbContext(), commandTimeout))
        {
            try
            {
                int rowsAffected = queryable.ExecuteUpdate(BuildSetPropertyCalls(updateExpression));
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
    public static QueryToFileResult QueryToCsvFile<T>(this IQueryable<T> queryable, string filePath) where T : class
    {
        return QueryToCsvFile<T>(queryable, filePath, new QueryToFileOptions());
    }
    public static QueryToFileResult QueryToCsvFile<T>(this IQueryable<T> queryable, Stream stream) where T : class
    {
        return QueryToCsvFile<T>(queryable, stream, new QueryToFileOptions());
    }
    public static QueryToFileResult QueryToCsvFile<T>(this IQueryable<T> queryable, string filePath, Action<QueryToFileOptions> optionsAction) where T : class
    {
        return QueryToCsvFile<T>(queryable, filePath, optionsAction.Build());
    }
    public static QueryToFileResult QueryToCsvFile<T>(this IQueryable<T> queryable, Stream stream, Action<QueryToFileOptions> optionsAction) where T : class
    {
        return QueryToCsvFile<T>(queryable, stream, optionsAction.Build());
    }
    public static QueryToFileResult QueryToCsvFile<T>(this IQueryable<T> queryable, string filePath, QueryToFileOptions options) where T : class
    {
        using var fileStream = File.Create(filePath);
        return QueryToCsvFile<T>(queryable, fileStream, options);
    }
    public static QueryToFileResult QueryToCsvFile<T>(this IQueryable<T> queryable, Stream stream, QueryToFileOptions options) where T : class
    {
        return InternalQueryToFile<T>(queryable, stream, options);
    }
    public static QueryToFileResult SqlQueryToCsvFile(this DatabaseFacade database, string filePath, string sqlText, params object[] parameters)
    {
        return SqlQueryToCsvFile(database, filePath, new QueryToFileOptions(), sqlText, parameters);
    }
    public static QueryToFileResult SqlQueryToCsvFile(this DatabaseFacade database, Stream stream, string sqlText, params object[] parameters)
    {
        return SqlQueryToCsvFile(database, stream, new QueryToFileOptions(), sqlText, parameters);
    }
    public static QueryToFileResult SqlQueryToCsvFile(this DatabaseFacade database, string filePath, Action<QueryToFileOptions> optionsAction, string sqlText, params object[] parameters)
    {
        return SqlQueryToCsvFile(database, filePath, optionsAction.Build(), sqlText, parameters);
    }
    public static QueryToFileResult SqlQueryToCsvFile(this DatabaseFacade database, Stream stream, Action<QueryToFileOptions> optionsAction, string sqlText, params object[] parameters)
    {
        return SqlQueryToCsvFile(database, stream, optionsAction.Build(), sqlText, parameters);
    }
    public static QueryToFileResult SqlQueryToCsvFile(this DatabaseFacade database, string filePath, QueryToFileOptions options, string sqlText, params object[] parameters)
    {
        using var fileStream = File.Create(filePath);
        return SqlQueryToCsvFile(database, fileStream, options, sqlText, parameters);
    }
    public static QueryToFileResult SqlQueryToCsvFile(this DatabaseFacade database, Stream stream, QueryToFileOptions options, string sqlText, params object[] parameters)
    {
        return InternalQueryToFile(database.GetDbConnection(), stream, options, sqlText, parameters);
    }
    public static void Clear<T>(this DbSet<T> dbSet) where T : class
    {
        var dbContext = dbSet.GetDbContext();
        var tableMapping = dbContext.GetTableMapping(typeof(T));
        dbContext.Database.ClearTable(tableMapping.FullQualifedTableName);
    }
    public static void Truncate<T>(this DbSet<T> dbSet) where T : class
    {
        var dbContext = dbSet.GetDbContext();
        var tableMapping = dbContext.GetTableMapping(typeof(T));
        dbContext.Database.TruncateTable(tableMapping.FullQualifedTableName);
    }
    public static IQueryable<T> UsingTable<T>(this IQueryable<T> queryable, string tableName) where T : class
    {
        var dbContext = queryable.GetDbContext();
        var tableMapping = dbContext.GetTableMapping(typeof(T));
        efExtensionsCommandInterceptor.AddCommand(Guid.NewGuid(),
            new EfExtensionsCommand
            {
                CommandType = EfExtensionsCommandType.ChangeTableName,
                OldValue = tableMapping.FullQualifedTableName,
                NewValue = dbContext.Database.DelimitTableName(tableName),
                Connection = dbContext.GetDbConnection()
            });
        return queryable;
    }
    public static TableMapping GetTableMapping(this DbContext dbContext, Type type, IEntityType entityType = null)
    {
        entityType ??= dbContext.Model.FindEntityType(type);
        return new TableMapping(dbContext, entityType);
    }
    internal static void SetStoreGeneratedValues<T>(this DbContext context, T entity, IEnumerable<IProperty> properties, object[] values)
    {
        int index = 0;
        var updateEntry = entity as InternalEntityEntry;
        if (updateEntry == null)
        {
            var entry = context.Entry(entity);
            updateEntry = entry.GetInfrastructure();
        }

        if (updateEntry != null)
        {
            foreach (var property in properties)
            {
                if ((updateEntry.EntityState == EntityState.Added &&
                    (property.ValueGenerated == ValueGenerated.OnAdd || property.ValueGenerated == ValueGenerated.OnAddOrUpdate)) ||
                  (updateEntry.EntityState == EntityState.Modified &&
                    (property.ValueGenerated == ValueGenerated.OnUpdate || property.ValueGenerated == ValueGenerated.OnAddOrUpdate)) ||
                  updateEntry.EntityState == EntityState.Detached
                )
                {
                    updateEntry.SetStoreGeneratedValue(property, values[index]);
                }
                index++;
            }
            if (updateEntry.EntityState == EntityState.Detached)
                updateEntry.AcceptChanges();
        }
        else
        {
            throw new InvalidOperationException("SetStoreValues() failed because an instance of InternalEntityEntry was not found.");
        }
    }
    internal static BulkInsertResult<T> BulkInsert<T>(IEnumerable<T> entities, BulkOptions options, TableMapping tableMapping, DbConnection dbConnection, DbTransaction transaction, string tableName,
        IEnumerable<string> inputColumns = null, SqlBulkCopyOptions bulkCopyOptions = SqlBulkCopyOptions.Default, bool useInternalId = false)
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
            sqlBulkCopy.ColumnMappings.Add(Constants.InternalId_ColumnName, Constants.InternalId_ColumnName);
        sqlBulkCopy.WriteToServer(dataReader);

        return new BulkInsertResult<T>
        {
            RowsAffected = sqlBulkCopy.RowsCopied,
            EntityMap = dataReader.EntityMap
        };
    }
    internal static BulkQueryResult BulkQuery(this DbContext context, string sqlText, BulkOptions options)
    {
        List<object[]> results = [];
        List<string> columns = [];
        using var command = context.Database.CreateCommand();
        command.CommandText = sqlText;
        if (options.CommandTimeout.HasValue)
            command.CommandTimeout = options.CommandTimeout.Value;
        using var reader = command.ExecuteReader();
        while (reader.Read())
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
    internal static DbContext GetDbContext<T>(this IQueryable<T> queryable) where T : class
    {
        DbContext dbContext;
        try
        {
            if ((queryable as InternalDbSet<T>) != null)
            {
                dbContext = queryable.GetPrivateFieldValue("_context") as DbContext;
            }
            else if ((queryable as EntityQueryable<T>) != null)
            {
                var queryCompiler = queryable.Provider.GetPrivateFieldValue("_queryCompiler");
                var contextFactory = queryCompiler.GetPrivateFieldValue("_queryContextFactory");
                var queryDependencies = contextFactory.GetPrivateFieldValue("Dependencies") as QueryContextDependencies;
                dbContext = queryDependencies.CurrentContext.Context as DbContext;
            }
            else
            {
                throw new InvalidOperationException("This extension method could not find the DbContext for this type that implements IQueryable");
            }
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException("This extension method could not find the DbContext for this type that implements IQueryable", ex);
        }
        return dbContext;
    }
    internal static DbConnection GetDbConnection(this DbContext context, ConnectionBehavior connectionBehavior = ConnectionBehavior.Default)
    {
        var dbConnection = context.Database.GetDbConnection();
        return connectionBehavior == ConnectionBehavior.New ? dbConnection.CloneConnection() : dbConnection;
    }
    private static IEnumerable<T> FetchInternal<T>(this DbContext dbContext, string sqlText, object[] parameters = null) where T : class, new()
    {
        using var connection = dbContext.GetDbConnection(ConnectionBehavior.New);
        if (connection.State == ConnectionState.Closed)
            connection.Open();

        using var command = connection.CreateCommand();
        if (command is OracleCommand oracleCommand)
            oracleCommand.BindByName = true;
        command.CommandText = DatabaseFacadeExtensions.NormalizeOracleSqlParameters(sqlText);
        if (parameters != null)
            DatabaseFacadeExtensions.AddParameters(command, parameters);

        var tableMapping = dbContext.GetTableMapping(typeof(T), null);
        using var reader = command.ExecuteReader();
        var properties = reader.GetProperties(tableMapping);
        var valuesFromProvider = properties.Select(p => tableMapping.GetValueFromProvider(p)).ToArray();

        while (reader.Read())
        {
            var entity = reader.MapEntity<T>(dbContext, properties, valuesFromProvider);
            yield return entity;
        }
    }
    private static BulkMergeResult<T> InternalBulkMerge<T>(this DbContext context, IEnumerable<T> entities, BulkMergeOptions<T> options)
    {
        BulkMergeResult<T> bulkMergeResult;
        using (var bulkOperation = new BulkOperation<T>(context, options))
        {
            try
            {
                bulkOperation.ValidateBulkMerge(options.MergeOnCondition);
                var bulkInsertResult = bulkOperation.BulkInsertStagingData(entities, true, true);
                bulkMergeResult = bulkOperation.ExecuteMerge(bulkInsertResult.EntityMap, options.MergeOnCondition, options.AutoMapOutput,
                    false, true, true, options.DeleteIfNotMatched);
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
    private static void ClearEntityStateToUnchanged<T>(DbContext dbContext, IEnumerable<T> entities)
    {
        foreach (var entity in entities)
        {
            var entry = dbContext.Entry(entity);
            if (entry.State == EntityState.Added || entry.State == EntityState.Modified)
                dbContext.Entry(entity).State = EntityState.Unchanged;
        }
    }
    private static void Validate(TableMapping tableMapping)
    {
        if (!tableMapping.GetPrimaryKeyColumns().Any())
        {
            throw new Exception("You must have a primary key on this table to use this function.");
        }
    }
    private static QueryToFileResult InternalQueryToFile<T>(this IQueryable<T> queryable, Stream stream, QueryToFileOptions options) where T : class
    {
        return InternalQueryToFile(queryable.AsNoTracking().AsEnumerable(), stream, options);
    }
    private static QueryToFileResult InternalQueryToFile(DbConnection dbConnection, Stream stream, QueryToFileOptions options, string sqlText, object[] parameters = null)
    {
        int dataRowCount = 0;
        int totalRowCount = 0;
        long bytesWritten = 0;

        if (dbConnection.State == ConnectionState.Closed)
            dbConnection.Open();

        using var command = dbConnection.CreateCommand();
        if (command is OracleCommand oracleCommand)
            oracleCommand.BindByName = true;
        command.CommandText = DatabaseFacadeExtensions.NormalizeOracleSqlParameters(sqlText);
        if (parameters != null)
            DatabaseFacadeExtensions.AddParameters(command, parameters);
        if (options.CommandTimeout.HasValue)
            command.CommandTimeout = options.CommandTimeout.Value;

        using var streamWriter = new StreamWriter(stream, leaveOpen: true);
        using (var reader = command.ExecuteReader())
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
                        streamWriter.Write(options.ColumnDelimiter);
                    }
                }
                totalRowCount++;
                streamWriter.Write(options.RowDelimiter);
            }
            while (reader.Read())
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
                        streamWriter.Write(options.ColumnDelimiter);
                    }
                }
                streamWriter.Write(options.RowDelimiter);
                dataRowCount++;
                totalRowCount++;
            }
            streamWriter.Flush();
            bytesWritten = streamWriter.BaseStream.Length;
        }
        return new QueryToFileResult()
        {
            BytesWritten = bytesWritten,
            DataRowCount = dataRowCount,
            TotalRowCount = totalRowCount
        };
    }
    private static QueryToFileResult InternalQueryToFile<T>(IEnumerable<T> entities, Stream stream, QueryToFileOptions options)
    {
        int dataRowCount = 0;
        int totalRowCount = 0;
        long bytesWritten = 0;
        var properties = typeof(T).GetProperties().Where(p => p.CanRead && (!typeof(System.Collections.IEnumerable).IsAssignableFrom(p.PropertyType) || p.PropertyType == typeof(string))).ToArray();

        using var streamWriter = new StreamWriter(stream, leaveOpen: true);
        if (options.IncludeHeaderRow)
        {
            WriteCsvRow(streamWriter, properties.Select(p => p.Name), options);
            totalRowCount++;
        }

        foreach (var entity in entities)
        {
            WriteCsvRow(streamWriter, properties.Select(p => p.GetValue(entity)), options);
            dataRowCount++;
            totalRowCount++;
        }

        streamWriter.Flush();
        bytesWritten = streamWriter.BaseStream.Length;
        return new QueryToFileResult { BytesWritten = bytesWritten, DataRowCount = dataRowCount, TotalRowCount = totalRowCount };
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
    private static void WriteCsvRow(TextWriter writer, IEnumerable<object> values, QueryToFileOptions options)
    {
        bool first = true;
        foreach (var value in values)
        {
            if (!first)
                writer.Write(options.ColumnDelimiter);

            writer.Write(options.TextQualifer);
            writer.Write(value);
            writer.Write(options.TextQualifer);
            first = false;
        }
        writer.Write(options.RowDelimiter);
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
                var propertyInfo = binding.Member as PropertyInfo ?? throw new InvalidOperationException("Only property bindings are supported.");
                var propertyLambda = Expression.Lambda(Expression.Property(entityParameter, propertyInfo), entityParameter);
                var valueLambda = Expression.Lambda(binding.Expression, entityParameter);
                currentBuilder = setPropertyMethod.MakeGenericMethod(propertyInfo.PropertyType).Invoke(currentBuilder, [propertyLambda, valueLambda]);
            }
        };
    }
}
