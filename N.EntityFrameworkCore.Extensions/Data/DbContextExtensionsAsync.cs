using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using N.EntityFrameworkCore.Extensions.Common;
using N.EntityFrameworkCore.Extensions.Enums;
using N.EntityFrameworkCore.Extensions.Extensions;
using N.EntityFrameworkCore.Extensions.Sql;
using N.EntityFrameworkCore.Extensions.Util;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace N.EntityFrameworkCore.Extensions
{
    public static class DbContextExtensionsAsync
    {
        public async static Task<int> BulkDeleteAsync<T>(this DbContext context, IEnumerable<T> entities, CancellationToken cancellationToken = default)
        {
            return await context.BulkDeleteAsync(entities, new BulkDeleteOptions<T>(), cancellationToken);
        }
        public async static Task<int> BulkDeleteAsync<T>(this DbContext context, IEnumerable<T> entities, Action<BulkDeleteOptions<T>> optionsAction, CancellationToken cancellationToken = default)
        {
            return await context.BulkDeleteAsync(entities, optionsAction.Build(), cancellationToken);
        }
        public async static Task<int> BulkDeleteAsync<T>(this DbContext context, IEnumerable<T> entities, BulkDeleteOptions<T> options, CancellationToken cancellationToken = default)
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
                    string destinationTableName = string.Format("[{0}].[{1}]", tableMapping.Schema, tableMapping.TableName);
                    string[] keyColumnNames = options.DeleteOnCondition != null ? CommonUtil<T>.GetColumns(options.DeleteOnCondition, new[] { "s" })
                        : tableMapping.GetPrimaryKeyColumns().ToArray();

                    if (keyColumnNames.Length == 0 && options.DeleteOnCondition == null)
                        throw new InvalidDataException("BulkDelete requires that the entity have a primary key or the Options.DeleteOnCondition must be set.");

                    await context.Database.CloneTableAsync(destinationTableName, stagingTableName, keyColumnNames, null, cancellationToken);
                    await BulkInsertAsync(entities, options, tableMapping, dbConnection, transaction, stagingTableName, keyColumnNames, SqlBulkCopyOptions.KeepIdentity, 
                        false, cancellationToken);
                    string deleteSql = string.Format("DELETE t FROM {0} s JOIN {1} t ON {2}", stagingTableName, destinationTableName,
                        CommonUtil<T>.GetJoinConditionSql(options.DeleteOnCondition, keyColumnNames));
                    rowsAffected = await context.Database.ExecuteSqlRawAsync(deleteSql, cancellationToken);
                    context.Database.DropTable(stagingTableName);
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
        public async static Task FetchAsync<T>(this IQueryable<T> querable, Func<FetchResult<T>,Task> action, Action<FetchOptions<T>> optionsAction, CancellationToken cancellationToken = default) where T : class, new()
        {
            await FetchAsync(querable, action, optionsAction.Build(), cancellationToken);
        }
        public async static Task FetchAsync<T>(this IQueryable<T> querable, Func<FetchResult<T>,Task> action, FetchOptions<T> options, CancellationToken cancellationToken = default) where T : class, new()
        {
            var dbContext = querable.GetDbContext();
            var sqlQuery = SqlBuilder.Parse(querable.ToQueryString());
            var tableMapping = dbContext.GetTableMapping(typeof(T));
            if (options.InputColumns != null || options.IgnoreColumns != null)
            {
                IEnumerable<string> columnNames = options.InputColumns != null ? options.InputColumns.GetObjectProperties() : tableMapping.GetColumns(true);
                IEnumerable<string> columnsToFetch = CommonUtil.FormatColumns(columnNames.Where(o => !options.IgnoreColumns.GetObjectProperties().Contains(o)));
                sqlQuery.SelectColumns(columnsToFetch);
            }

            await using var command = dbContext.Database.CreateCommand(ConnectionBehavior.New);
            command.CommandText = sqlQuery.Sql;
            command.Parameters.AddRange(sqlQuery.Parameters.ToArray());
            var reader = await command.ExecuteReaderAsync(cancellationToken);

            var propertySetters = reader.GetPropertyInfos<T>();
            var valuesFromProvider = tableMapping.GetValuesFromProvider().ToList();
            //Read data
            int batch = 1;
            int count = 0;
            int totalCount = 0;
            var entities = new List<T>();
            while (await reader.ReadAsync(cancellationToken))
            {
                var entity = reader.MapEntity<T>(propertySetters, valuesFromProvider);
                entities.Add(entity);
                count++;
                totalCount++;
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
            
            await reader.CloseAsync();
        }
        public async static Task<int> BulkInsertAsync<T>(this DbContext context, IEnumerable<T> entities, CancellationToken cancellationToken = default)
        {
            return await context.BulkInsertAsync<T>(entities, new BulkInsertOptions<T> { }, cancellationToken);
        }
        public async static Task<int> BulkInsertAsync<T>(this DbContext context, IEnumerable<T> entities, Action<BulkInsertOptions<T>> optionsAction, CancellationToken cancellationToken = default)
        {
            return await context.BulkInsertAsync<T>(entities, optionsAction.Build(), cancellationToken);
        }
        public async static Task<int> BulkInsertAsync<T>(this DbContext context, IEnumerable<T> entities, BulkInsertOptions<T> options, CancellationToken cancellationToken = default)
        {
            int rowsAffected = 0;
            using (var bulkOperation = new BulkOperation<T>(context, options, options.InputColumns, options.IgnoreColumns))
            {
                try
                {
                    var bulkInsertResult = await bulkOperation.BulkInsertStagingDataAsync(entities, true, true);
                    var bulkMergeResult = await bulkOperation.ExecuteMergeAsync(bulkInsertResult.EntityMap, options.InsertOnCondition,
                        options.AutoMapOutput, options.InsertIfNotExists);
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
        internal async static Task<BulkInsertResult<T>> BulkInsertAsync<T>(IEnumerable<T> entities, BulkOptions options, TableMapping tableMapping, SqlConnection dbConnection, SqlTransaction transaction, string tableName,
            IEnumerable<string> inputColumns = null, SqlBulkCopyOptions bulkCopyOptions = SqlBulkCopyOptions.Default, bool useInteralId = false, CancellationToken cancellationToken = default)
        {
            var dataReader = new EntityDataReader<T>(tableMapping, entities, useInteralId);

            var sqlBulkCopy = new SqlBulkCopy(dbConnection, bulkCopyOptions, transaction)
            {
                DestinationTableName = tableName,
                BatchSize = options.BatchSize
            };
            if (options.CommandTimeout.HasValue)
            {
                sqlBulkCopy.BulkCopyTimeout = options.CommandTimeout.Value;
            }
            foreach (var property in dataReader.TableMapping.Properties)
            {
                var columnName = dataReader.TableMapping.GetColumnName(property);
                if (inputColumns == null || (inputColumns != null && inputColumns.Contains(columnName)))
                    sqlBulkCopy.ColumnMappings.Add(columnName, columnName);
            }
            if (useInteralId)
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
        public async static Task<BulkMergeResult<T>> BulkMergeAsync<T>(this DbContext context, IEnumerable<T> entities, CancellationToken cancellationToken = default)
        {
            return await BulkMergeAsync(context, entities, new BulkMergeOptions<T>(), cancellationToken);
        }
        public async static Task<BulkMergeResult<T>> BulkMergeAsync<T>(this DbContext context, IEnumerable<T> entities, BulkMergeOptions<T> options, CancellationToken cancellationToken = default)
        {
            return await InternalBulkMergeAsync(context, entities, options, cancellationToken);
        }
        public async static Task<BulkMergeResult<T>> BulkMergeAsync<T>(this DbContext context, IEnumerable<T> entities, Action<BulkMergeOptions<T>> optionsAction, CancellationToken cancellationToken = default)
        {
            return await BulkMergeAsync(context, entities, optionsAction.Build(), cancellationToken);
        }
        public async static Task<int> BulkSaveChangesAsync(this DbContext dbContext)
        {
            return await dbContext.BulkSaveChangesAsync(true);
        }
        public async static Task<int> BulkSaveChangesAsync(this DbContext dbContext, bool acceptAllChangesOnSuccess = true)
        {
            int rowsAffected = 0;
            var stateManager = dbContext.ChangeTracker.GetPrivateFieldValue("StateManager") as StateManager;

            dbContext.ChangeTracker.DetectChanges();
            var entries = stateManager.GetEntriesToSave(true);

            foreach (var saveEntryGroup in entries.GroupBy(o => new { o.EntityType, o.EntityState }))
            {
                var key = saveEntryGroup.Key;
                var entities = saveEntryGroup.AsEnumerable();
                var options = new BulkOptions { EntityType = saveEntryGroup.Key.EntityType };
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
        public async static Task<BulkSyncResult<T>> BulkSyncAsync<T>(this DbContext context, IEnumerable<T> entities, CancellationToken cancellationToken = default)
        {
            return await BulkSyncAsync(context, entities, new BulkSyncOptions<T>(), cancellationToken);
        }
        public async static Task<BulkSyncResult<T>> BulkSyncAsync<T>(this DbContext context, IEnumerable<T> entities, Action<BulkSyncOptions<T>> optionsAction, CancellationToken cancellationToken = default)
        {
            return BulkSyncResult<T>.Map(await InternalBulkMergeAsync(context, entities, optionsAction.Build(), cancellationToken));
        }
        public async static Task<BulkSyncResult<T>> BulkSyncAsync<T>(this DbContext context, IEnumerable<T> entities, BulkSyncOptions<T> options, CancellationToken cancellationToken = default)
        {
            return BulkSyncResult<T>.Map(await InternalBulkMergeAsync(context, entities, options, cancellationToken));
        }
        private async static Task<BulkMergeResult<T>> InternalBulkMergeAsync<T>(this DbContext context, IEnumerable<T> entities, BulkMergeOptions<T> options, CancellationToken cancellationToken = default)
        {
            BulkMergeResult<T> bulkMergeResult;
            using (var bulkOperation = new BulkOperation<T>(context, options))
            {
                try
                {
                    bulkOperation.ValidateBulkMerge(options.MergeOnCondition);
                    var bulkInsertResult = await bulkOperation.BulkInsertStagingDataAsync(entities, true, true, cancellationToken);
                    bulkMergeResult = await bulkOperation.ExecuteMergeAsync(bulkInsertResult.EntityMap, options.MergeOnCondition, options.AutoMapOutput,
                        true, true, options.DeleteIfNotMatched, cancellationToken);
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
        public async static Task<int> BulkUpdateAsync<T>(this DbContext context, IEnumerable<T> entities, CancellationToken cancellationToken = default)
        {
            return await BulkUpdateAsync<T>(context, entities, new BulkUpdateOptions<T>(), cancellationToken);
        }
        public async static Task<int> BulkUpdateAsync<T>(this DbContext context, IEnumerable<T> entities, Action<BulkUpdateOptions<T>> optionsAction, CancellationToken cancellationToken = default)
        {
            return await BulkUpdateAsync<T>(context, entities, optionsAction.Build(), cancellationToken);
        }
        public async static Task<int> BulkUpdateAsync<T>(this DbContext context, IEnumerable<T> entities, BulkUpdateOptions<T> options, CancellationToken cancellationToken = default)
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
        internal async static Task<BulkQueryResult> BulkQueryAsync(this DbContext context, string sqlText, SqlConnection dbConnection, SqlTransaction transaction, BulkOptions options, CancellationToken cancellationToken = default)
        {
            var results = new List<object[]>();
            var columns = new List<string>();
            var command = new SqlCommand(sqlText, dbConnection, transaction);
            if (options.CommandTimeout.HasValue)
            {
                command.CommandTimeout = options.CommandTimeout.Value;
            }
            var reader = await command.ExecuteReaderAsync(cancellationToken);
            //Get column names
            for (int i = 0; i < reader.FieldCount; i++)
            {
                columns.Add(reader.GetName(i));
            }
            try
            {
                //Read data
                while (await reader.ReadAsync(cancellationToken))
                {
                    Object[] values = new Object[reader.FieldCount];
                    reader.GetValues(values);
                    results.Add(values);
                }
            }
            finally
            {
                //close the DataReader
                await reader.CloseAsync();
            }

            return new BulkQueryResult
            {
                Columns = columns,
                Results = results,
                RowsAffected = reader.RecordsAffected
            };
        }
        public async static Task<int> DeleteFromQueryAsync<T>(this IQueryable<T> querable, int? commandTimeout = null, CancellationToken cancellationToken = default) where T : class
        {
            int rowAffected = 0;
            var dbContext = querable.GetDbContext();
            using (var dbTransactionContext = new DbTransactionContext(dbContext, commandTimeout))
            {
                var dbConnection = dbTransactionContext.Connection;
                var dbTransaction = dbTransactionContext.CurrentTransaction;
                try
                {
                    var sqlQuery = SqlBuilder.Parse(querable.ToQueryString());
                    sqlQuery.ChangeToDelete();
                    rowAffected = await dbContext.Database.ExecuteSqlRawAsync(sqlQuery.Sql, sqlQuery.Parameters, cancellationToken);

                    dbTransactionContext.Commit();
                }
                catch (Exception)
                {
                    dbTransactionContext.Rollback();
                    throw;
                }
            }
            return rowAffected;
        }
        public async static Task<int> InsertFromQueryAsync<T>(this IQueryable<T> querable, string tableName, Expression<Func<T, object>> insertObjectExpression, int? commandTimeout = null, 
            CancellationToken cancellationToken = default) where T : class
        {
            int rowAffected = 0;
            var dbContext = querable.GetDbContext();
            using (var dbTransactionContext = new DbTransactionContext(dbContext, commandTimeout))
            {
                var dbConnection = dbTransactionContext.Connection;
                var dbTransaction = dbTransactionContext.CurrentTransaction;
                try
                {
                    var sqlQuery = SqlBuilder.Parse(querable.ToQueryString());
                    if (dbContext.Database.TableExists(tableName))
                    {
                        sqlQuery.ChangeToInsert(tableName, insertObjectExpression);
                        await dbContext.Database.ToggleIdentityInsertAsync(tableName, true);
                        rowAffected = await dbContext.Database.ExecuteSqlRawAsync(sqlQuery.Sql, sqlQuery.Parameters.ToArray(), cancellationToken);
                        await dbContext.Database.ToggleIdentityInsertAsync(tableName, false);
                    }
                    else
                    {
                        sqlQuery.Clauses.First().InputText += string.Format(" INTO {0}", tableName);
                        rowAffected = await dbContext.Database.ExecuteSqlRawAsync(sqlQuery.Sql, sqlQuery.Parameters.ToArray(), cancellationToken);
                    }

                    dbTransactionContext.Commit();
                }
                catch (Exception)
                {
                    dbTransactionContext.Rollback();
                    throw;
                }
            }
            return rowAffected;
        }
        public async static Task<int> UpdateFromQueryAsync<T>(this IQueryable<T> querable, Expression<Func<T, T>> updateExpression, int? commandTimeout = null, 
            CancellationToken cancellationToken = default) where T : class
        {
            int rowAffected = 0;
            var dbContext = querable.GetDbContext();
            using (var dbTransactionContext = new DbTransactionContext(dbContext, commandTimeout))
            {
                var dbConnection = dbTransactionContext.Connection;
                var dbTransaction = dbTransactionContext.CurrentTransaction;
                try
                {
                    var sqlQuery = SqlBuilder.Parse(querable.ToQueryString());
                    string setSqlExpression = updateExpression.ToSqlUpdateSetExpression(sqlQuery.GetTableAlias());
                    sqlQuery.ChangeToUpdate(sqlQuery.GetTableAlias(), setSqlExpression);
                    rowAffected = await dbContext.Database.ExecuteSqlRawAsync(sqlQuery.Sql, sqlQuery.Parameters.ToArray(), cancellationToken);
                    dbTransactionContext.Commit();
                }
                catch (Exception)
                {
                    dbTransactionContext.Rollback();
                    throw;
                }
            }
            return rowAffected;
        }
        public async static Task<QueryToFileResult> QueryToCsvFileAsync<T>(this IQueryable<T> querable, String filePath, CancellationToken cancellationToken = default) where T : class
        {
            return await QueryToCsvFileAsync<T>(querable, filePath, new QueryToFileOptions(), cancellationToken);
        }
        public async static Task<QueryToFileResult> QueryToCsvFileAsync<T>(this IQueryable<T> querable, Stream stream, CancellationToken cancellationToken = default) where T : class
        {
            return await QueryToCsvFileAsync<T>(querable, stream, new QueryToFileOptions(), cancellationToken);
        }
        public async static Task<QueryToFileResult> QueryToCsvFileAsync<T>(this IQueryable<T> querable, String filePath, Action<QueryToFileOptions> optionsAction, 
            CancellationToken cancellationToken = default) where T : class
        {
            return await QueryToCsvFileAsync<T>(querable, filePath, optionsAction.Build(), cancellationToken);
        }
        public async static Task<QueryToFileResult> QueryToCsvFileAsync<T>(this IQueryable<T> querable, Stream stream, Action<QueryToFileOptions> optionsAction, 
            CancellationToken cancellationToken = default) where T : class
        {
            return await QueryToCsvFileAsync<T>(querable, stream, optionsAction.Build(), cancellationToken);
        }
        public async static Task<QueryToFileResult> QueryToCsvFileAsync<T>(this IQueryable<T> querable, String filePath, QueryToFileOptions options, 
            CancellationToken cancellationToken = default) where T : class
        {
            var fileStream = File.Create(filePath);
            return await QueryToCsvFileAsync<T>(querable, fileStream, options, cancellationToken);
        }
        public async static Task<QueryToFileResult> QueryToCsvFileAsync<T>(this IQueryable<T> querable, Stream stream, QueryToFileOptions options, 
            CancellationToken cancellationToken = default) where T : class
        {
            return await InternalQueryToFileAsync<T>(querable, stream, options, cancellationToken);
        }
        public async static Task<QueryToFileResult> SqlQueryToCsvFileAsync(this DatabaseFacade database, string filePath, string sqlText, object[] parameters, 
            CancellationToken cancellationToken = default)
        {
            return await SqlQueryToCsvFileAsync(database, filePath, new QueryToFileOptions(), sqlText, parameters, cancellationToken);
        }
        public async static Task<QueryToFileResult> SqlQueryToCsvFileAsync(this DatabaseFacade database, Stream stream, string sqlText, object[] parameters, 
            CancellationToken cancellationToken = default)
        {
            return await SqlQueryToCsvFileAsync(database, stream, new QueryToFileOptions(), sqlText, parameters, cancellationToken);
        }
        public async static Task<QueryToFileResult> SqlQueryToCsvFileAsync(this DatabaseFacade database, string filePath, Action<QueryToFileOptions> optionsAction, string sqlText, object[] parameters, 
            CancellationToken cancellationToken = default)
        {
            return await SqlQueryToCsvFileAsync(database, filePath, optionsAction.Build(), sqlText, parameters, cancellationToken);
        }
        public async static Task<QueryToFileResult> SqlQueryToCsvFileAsync(this DatabaseFacade database, Stream stream, Action<QueryToFileOptions> optionsAction, string sqlText, object[] parameters,
            CancellationToken cancellationToken = default)
        {
            return await SqlQueryToCsvFileAsync(database, stream, optionsAction.Build(), sqlText, parameters, cancellationToken);
        }
        public async static Task<QueryToFileResult> SqlQueryToCsvFileAsync(this DatabaseFacade database, string filePath, QueryToFileOptions options, string sqlText, object[] parameters, 
            CancellationToken cancellationToken = default)
        {
            var fileStream = File.Create(filePath);
            return await SqlQueryToCsvFileAsync(database, fileStream, options, sqlText, parameters, cancellationToken);
        }
        public async static Task<QueryToFileResult> SqlQueryToCsvFileAsync(this DatabaseFacade database, Stream stream, QueryToFileOptions options, string sqlText, object[] parameters, 
            CancellationToken cancellationToken = default)
        {
            var dbConnection = database.GetDbConnection() as SqlConnection;
            return await InternalQueryToFileAsync(dbConnection, stream, options, sqlText, parameters, cancellationToken);
        }
        public async static Task ClearAsync<T>(this DbSet<T> dbSet, CancellationToken cancellationToken = default) where T : class
        {
            var dbContext = dbSet.GetDbContext();
            var tableMapping = dbContext.GetTableMapping(typeof(T));
            await dbContext.Database.ClearTableAsync(tableMapping.FullQualifedTableName, cancellationToken);
        }
        public async static Task TruncateAsync<T>(this DbSet<T> dbSet, CancellationToken cancellationToken = default) where T : class
        {
            var dbContext = dbSet.GetDbContext();
            var tableMapping = dbContext.GetTableMapping(typeof(T));
            await dbContext.Database.TruncateTableAsync(tableMapping.FullQualifedTableName, false, cancellationToken);
        }
        private async static Task<QueryToFileResult> InternalQueryToFileAsync<T>(this IQueryable<T> querable, Stream stream, QueryToFileOptions options, 
            CancellationToken cancellationToken = default) where T : class
        {
            var dbContext = querable.GetDbContext();
            var dbConnection = dbContext.GetSqlConnection();
            return await InternalQueryToFileAsync(dbConnection, stream, options, querable.ToQueryString(), null, cancellationToken);
        }
        private async static Task<QueryToFileResult> InternalQueryToFileAsync(SqlConnection dbConnection, Stream stream, QueryToFileOptions options, string sqlText, object[] parameters = null,
            CancellationToken cancellationToken = default)
        {
            int dataRowCount = 0;
            int totalRowCount = 0;
            long bytesWritten = 0;

            //Open datbase connection
            if (dbConnection.State == ConnectionState.Closed)
                dbConnection.Open();

            var command = new SqlCommand(sqlText, dbConnection);
            if (parameters != null)
            {
                command.Parameters.AddRange(parameters);
            }
            if (options.CommandTimeout.HasValue)
            {
                command.CommandTimeout = options.CommandTimeout.Value;
            }

            StreamWriter streamWriter = new StreamWriter(stream);
            using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                //Header row
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
                //Write data rows to file
                while (await reader.ReadAsync(cancellationToken))
                {
                    Object[] values = new Object[reader.FieldCount];
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
                streamWriter.Close();
            }
            return new QueryToFileResult()
            {
                BytesWritten = bytesWritten,
                DataRowCount = dataRowCount,
                TotalRowCount = totalRowCount
            };
        }
    }
}
