using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
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
            if (options.InputColumns != null || options.IgnoreColumns != null)
            {
                var tableMapping = dbContext.GetTableMapping(typeof(T));
                IEnumerable<string> columnNames = options.InputColumns != null ? options.InputColumns.GetObjectProperties() : tableMapping.GetColumns(true);
                IEnumerable<string> columnsToFetch = CommonUtil.FormatColumns(columnNames.Where(o => !options.IgnoreColumns.GetObjectProperties().Contains(o)));
                sqlQuery.SelectColumns(columnsToFetch);
            }
            using (var command = dbContext.Database.CreateCommand(ConnectionBehavior.New))
            {
                command.CommandText = sqlQuery.Sql;
                command.Parameters.AddRange(sqlQuery.Parameters.ToArray());
                var reader = await command.ExecuteReaderAsync(cancellationToken);

                List<PropertyInfo> propertySetters = new List<PropertyInfo>();
                var entityType = typeof(T);
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    propertySetters.Add(entityType.GetProperty(reader.GetName(i)));
                }
                //Read data
                int batch = 1;
                int count = 0;
                int totalCount = 0;
                var entities = new List<T>();
                while (await reader.ReadAsync(cancellationToken))
                {
                    var entity = new T();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        var value = reader.GetValue(i);
                        if (value == DBNull.Value)
                            value = null;
                        propertySetters[i].SetValue(entity, value);
                    }
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
                //close the DataReader
                await reader.CloseAsync();
            }
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
            var tableMapping = context.GetTableMapping(typeof(T), options.EntityType);

            using (var dbTransactionContext = new DbTransactionContext(context, options))
            {
                try
                {
                    var dbConnection = dbTransactionContext.Connection;
                    var transaction = dbTransactionContext.CurrentTransaction;
                    string stagingTableName = CommonUtil.GetStagingTableName(tableMapping, options.UsePermanentTable, dbConnection);
                    string destinationTableName = string.Format("[{0}].[{1}]", tableMapping.Schema, tableMapping.TableName);

                    string[] primaryKeyColumnNames = tableMapping.GetPrimaryKeyColumns().ToArray();
                    IEnumerable<string> columnNames = CommonUtil.FilterColumns(tableMapping.GetColumns(options.KeepIdentity), primaryKeyColumnNames, options.InputColumns, options.IgnoreColumns);
                    IEnumerable<string> autoGeneratedColumnNames = options.AutoMapOutput ? tableMapping.GetAutoGeneratedColumns() : new string[] { };
                    IEnumerable<string> columnsToInsert = CommonUtil.FormatColumns(columnNames);
                    if (options.InsertIfNotExists)
                    {
                        columnNames = columnNames.Union(primaryKeyColumnNames);
                    }

                    await context.Database.CloneTableAsync(destinationTableName, stagingTableName, columnNames, Common.Constants.InternalId_ColumnName, cancellationToken);
                    var bulkInsertResult = await BulkInsertAsync(entities, options, tableMapping, dbConnection, transaction, stagingTableName, columnNames, SqlBulkCopyOptions.KeepIdentity, true, cancellationToken);

                    List<string> columnsToOutput = new List<string> { "$Action", string.Format("{0}.{1}", "s", Constants.InternalId_ColumnName) };
                    var entityProperties = new List<IProperty>();
                    var entityType = tableMapping.EntityType;

                    foreach (var autoGeneratedColumnName in autoGeneratedColumnNames)
                    {
                        columnsToOutput.Add(string.Format("inserted.[{0}]", autoGeneratedColumnName));
                        entityProperties.Add(entityType.GetProperty(autoGeneratedColumnName));
                    }

                    string insertSqlText = string.Format("MERGE {0} t USING {1} s ON {2} WHEN NOT MATCHED THEN INSERT ({3}) VALUES ({3}){4};",
                        destinationTableName,
                        stagingTableName,
                        options.InsertIfNotExists ? CommonUtil<T>.GetJoinConditionSql(options.InsertOnCondition, primaryKeyColumnNames, "t", "s") : "1=2",
                        SqlUtil.ConvertToColumnString(columnsToInsert),
                        columnsToOutput.Count > 0 ? " OUTPUT " + SqlUtil.ConvertToColumnString(columnsToOutput) : "");

                    if (options.KeepIdentity && primaryKeyColumnNames.Length > 0)
                        context.Database.ToggleIdentityInsert(true, destinationTableName);
                    var bulkQueryResult = await context.BulkQueryAsync(insertSqlText, dbConnection, transaction, options, cancellationToken);
                    if (options.KeepIdentity && primaryKeyColumnNames.Length > 0)
                        context.Database.ToggleIdentityInsert(false, destinationTableName);
                    rowsAffected = bulkQueryResult.RowsAffected;

                    if (options.AutoMapOutput)
                    {
                        if (rowsAffected == entities.Count())
                        {
                            foreach (var result in bulkQueryResult.Results)
                            {
                                int entityId = (int)result[1];
                                var entity = bulkInsertResult.EntityMap[entityId];
                                var entityValues = result.Skip(2).ToArray();
                                context.SetStoreGeneratedValues(entity, entityProperties, entityValues);
                            }
                        }
                    }

                    context.Database.DropTable(stagingTableName);
                    //ClearEntityStateToUnchanged(context, entities);
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
        private async static Task<BulkInsertResult<T>> BulkInsertAsync<T>(IEnumerable<T> entities, BulkOptions options, TableMapping tableMapping, SqlConnection dbConnection, SqlTransaction transaction, string tableName,
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
                var columnName = property.GetColumnName(dataReader.TableMapping.StoreObjectIdentifier);
                if (inputColumns == null || (inputColumns != null && inputColumns.Contains(columnName)))
                    sqlBulkCopy.ColumnMappings.Add(property.Name, columnName);
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
            int rowsAffected = 0;
            var outputRows = new List<BulkMergeOutputRow<T>>();
            var tableMapping = context.GetTableMapping(typeof(T));
            int rowsInserted = 0;
            int rowsUpdated = 0;
            int rowsDeleted = 0;

            using (var dbTransactionContext = new DbTransactionContext(context, options))
            {
                var dbConnection = dbTransactionContext.Connection;
                var transaction = dbTransactionContext.CurrentTransaction;
                try
                {
                    string stagingTableName = CommonUtil.GetStagingTableName(tableMapping, options.UsePermanentTable, dbConnection);
                    string destinationTableName = string.Format("[{0}].[{1}]", tableMapping.Schema, tableMapping.TableName);
                    string[] stagingColumnNames = tableMapping.GetColumns(true).ToArray();
                    string[] primaryKeyColumnNames = tableMapping.GetPrimaryKeyColumns().ToArray();
                    IEnumerable<string> autoGeneratedColumnNames = options.AutoMapOutput ? tableMapping.GetAutoGeneratedColumns() : new string[] { };

                    if (primaryKeyColumnNames.Length == 0 && options.MergeOnCondition == null)
                        throw new InvalidDataException("BulkMerge requires that the entity have a primary key or the Options.MergeOnCondition must be set.");

                    await context.Database.CloneTableAsync(destinationTableName, stagingTableName, stagingColumnNames, Common.Constants.InternalId_ColumnName, cancellationToken);
                    var bulkInsertResult = await BulkInsertAsync(entities, options, tableMapping, dbConnection, transaction, stagingTableName, stagingColumnNames, SqlBulkCopyOptions.KeepIdentity, true, cancellationToken);

                    string[] columnNames = tableMapping.GetColumns().ToArray();
                    IEnumerable<string> columnsToInsert = CommonUtil.FormatColumns(columnNames.Where(o => !options.GetIgnoreColumnsOnInsert().Contains(o)));
                    IEnumerable<string> columnstoUpdate = CommonUtil.FormatColumns(columnNames.Where(o => !options.GetIgnoreColumnsOnUpdate().Contains(o))).Select(o => string.Format("t.{0}=s.{0}", o));
                    List<string> columnsToOutput = new List<string> { "$Action", string.Format("{0}.{1}", "s", Constants.InternalId_ColumnName) };
                    List<PropertyInfo> propertySetters = new List<PropertyInfo>();
                    Type entityType = typeof(T);

                    foreach (var autoGeneratedColumnName in autoGeneratedColumnNames)
                    {
                        columnsToOutput.Add(string.Format("inserted.[{0}]", autoGeneratedColumnName));
                        columnsToOutput.Add(string.Format("deleted.[{0}]", autoGeneratedColumnName));
                        propertySetters.Add(entityType.GetProperty(autoGeneratedColumnName));
                    }

                    string mergeSqlText = string.Format("MERGE {0} t USING {1} s ON ({2}) WHEN NOT MATCHED BY TARGET THEN INSERT ({3}) VALUES ({3}) WHEN MATCHED THEN UPDATE SET {4}{5}OUTPUT {6};",
                        destinationTableName, stagingTableName, CommonUtil<T>.GetJoinConditionSql(options.MergeOnCondition, primaryKeyColumnNames, "s", "t"),
                        SqlUtil.ConvertToColumnString(columnsToInsert),
                        SqlUtil.ConvertToColumnString(columnstoUpdate),
                        options.DeleteIfNotMatched ? " WHEN NOT MATCHED BY SOURCE THEN DELETE " : " ",
                        SqlUtil.ConvertToColumnString(columnsToOutput)
                        );

                    var bulkQueryResult = await context.BulkQueryAsync(mergeSqlText, dbConnection, transaction, options, cancellationToken);
                    rowsAffected = bulkQueryResult.RowsAffected;

                    if (options.AutoMapOutput)
                    {
                        foreach (var result in bulkQueryResult.Results)
                        {
                            object entity = null;
                            string action = (string)result[0];
                            if (action != SqlMergeAction.Delete)
                            {
                                int entityId = (int)result[1];
                                entity = bulkInsertResult.EntityMap[entityId];
                                if (entity != null)
                                {

                                    for (int i = 2; i < 2 + autoGeneratedColumnNames.Count(); i += 2)
                                    {
                                        propertySetters[i - 2].SetValue(entity, result[i]);
                                    }
                                }
                            }
                            outputRows.Add(new BulkMergeOutputRow<T>(action));

                            if (action == SqlMergeAction.Insert) rowsInserted++;
                            else if (action == SqlMergeAction.Update) rowsUpdated++;
                            else if (action == SqlMergeAction.Delete) rowsDeleted++;
                            cancellationToken.ThrowIfCancellationRequested();
                        }
                    }
                    context.Database.DropTable(stagingTableName);

                    //ClearEntityStateToUnchanged(context, entities);
                    dbTransactionContext.Commit();
                }
                catch (Exception)
                {
                    dbTransactionContext.Rollback();
                    throw;
                }

                return new BulkMergeResult<T>
                {
                    Output = outputRows,
                    RowsAffected = rowsAffected,
                    RowsDeleted = rowsDeleted,
                    RowsInserted = rowsInserted,
                    RowsUpdated = rowsUpdated,
                };
            }
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
            var outputRows = new List<BulkMergeOutputRow<T>>();
            var tableMapping = context.GetTableMapping(typeof(T), options.EntityType);

            using (var dbTransactionContext = new DbTransactionContext(context, options))
            {
                var dbContext = dbTransactionContext.DbContext;
                var dbConnection = dbTransactionContext.Connection;
                var transaction = dbTransactionContext.CurrentTransaction;
                try
                {
                    string stagingTableName = CommonUtil.GetStagingTableName(tableMapping, options.UsePermanentTable, dbConnection);
                    string destinationTableName = string.Format("[{0}].[{1}]", tableMapping.Schema, tableMapping.TableName);
                    string[] primaryKeyColumnNames = tableMapping.GetPrimaryKeyColumns().ToArray();
                    IEnumerable<string> columnNames = CommonUtil.FilterColumns(tableMapping.GetColumns(), primaryKeyColumnNames, options.InputColumns, options.IgnoreColumns);

                    if (primaryKeyColumnNames.Length == 0 && options.UpdateOnCondition == null)
                        throw new InvalidDataException("BulkUpdate requires that the entity have a primary key or the Options.UpdateOnCondition must be set.");

                    await context.Database.CloneTableAsync(destinationTableName, stagingTableName, null, null, cancellationToken);
                    await BulkInsertAsync(entities, options, tableMapping, dbConnection, transaction, stagingTableName, null, SqlBulkCopyOptions.KeepIdentity, false, cancellationToken);

                    IEnumerable<string> columnstoUpdate = CommonUtil.FormatColumns(columnNames.Where(o => !options.IgnoreColumns.GetObjectProperties().Contains(o)));

                    string updateSetExpression = string.Join(",", columnstoUpdate.Select(o => string.Format("t.{0}=s.{0}", o)));
                    string updateSql = string.Format("UPDATE t SET {0} FROM {1} AS s JOIN {2} AS t ON {3}; SELECT @@RowCount;",
                        updateSetExpression, stagingTableName, destinationTableName, CommonUtil<T>.GetJoinConditionSql(options.UpdateOnCondition, primaryKeyColumnNames, "s", "t"));

                    rowsUpdated = await context.Database.ExecuteSqlRawAsync(updateSql, cancellationToken);
                    dbContext.Database.DropTable(stagingTableName);

                    //ClearEntityStateToUnchanged(context, entities);
                    dbTransactionContext.Commit();
                }
                catch (Exception)
                {
                    dbTransactionContext.Rollback();
                    throw;
                }

                return rowsUpdated;
            }
        }
        private async static Task<BulkQueryResult> BulkQueryAsync(this DbContext context, string sqlText, SqlConnection dbConnection, SqlTransaction transaction, BulkOptions options, CancellationToken cancellationToken = default)
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
                        dbContext.Database.ToggleIdentityInsert(true, tableName);
                        rowAffected = await dbContext.Database.ExecuteSqlRawAsync(sqlQuery.Sql, sqlQuery.Parameters.ToArray(), cancellationToken);
                        dbContext.Database.ToggleIdentityInsert(false, tableName);
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
