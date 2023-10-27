﻿using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using N.EntityFrameworkCore.Extensions.Common;
using N.EntityFrameworkCore.Extensions.Enums;
using N.EntityFrameworkCore.Extensions.Extensions;
using N.EntityFrameworkCore.Extensions.Sql;
using N.EntityFrameworkCore.Extensions.Util;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory;

namespace N.EntityFrameworkCore.Extensions
{
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
            var tableMapping = context.GetTableMapping(typeof(T), options.EntityType);

            using (var dbTransactionContext = new DbTransactionContext(context, options))
            {
                var dbConnection = dbTransactionContext.Connection;
                var transaction = dbTransactionContext.CurrentTransaction;
                int rowsAffected;
                try
                {
                    string stagingTableName = CommonUtil.GetStagingTableName(tableMapping, options.UsePermanentTable, dbConnection);
                    string destinationTableName = string.Format("[{0}].[{1}]", tableMapping.Schema, tableMapping.TableName);
                    string[] keyColumnNames = options.DeleteOnCondition != null ? CommonUtil<T>.GetColumns(options.DeleteOnCondition, new[] { "s" })
                        : tableMapping.GetPrimaryKeyColumns().ToArray();

                    if (keyColumnNames.Length == 0 && options.DeleteOnCondition == null)
                        throw new InvalidDataException("BulkDelete requires that the entity have a primary key or the Options.DeleteOnCondition must be set.");

                    context.Database.CloneTable(destinationTableName, stagingTableName, keyColumnNames);
                    BulkInsert(entities, options, tableMapping, dbConnection, transaction, stagingTableName, keyColumnNames, SqlBulkCopyOptions.KeepIdentity, false);
                    string deleteSql = string.Format("DELETE t FROM {0} s JOIN {1} t ON {2}", stagingTableName, destinationTableName,
                        CommonUtil<T>.GetJoinConditionSql(options.DeleteOnCondition, keyColumnNames));
                    rowsAffected = context.Database.ExecuteSql(deleteSql, options.CommandTimeout);

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
        public static IEnumerable<T> BulkFetch<T,U>(this DbSet<T> dbSet, IEnumerable<U> entities) where T : class, new()
        {
            return dbSet.BulkFetch(entities, new BulkFetchOptions<T>());
        }
        public static IEnumerable<T> BulkFetch<T,U>(this DbSet<T> dbSet, IEnumerable<U> entities, Action<BulkFetchOptions<T>> optionsAction) where T : class, new()
        {
            return dbSet.BulkFetch(entities, optionsAction.Build());
        }
        public static IEnumerable<T> BulkFetch<T,U>(this DbSet<T> dbSet, IEnumerable<U> entities, BulkFetchOptions<T> options) where T : class, new()
        {
            var context = dbSet.GetDbContext();
            var tableMapping = context.GetTableMapping(typeof(T));

            using (var dbTransactionContext = new DbTransactionContext(context, options.CommandTimeout, ConnectionBehavior.New))
            {
                string selectSql, stagingTableName = string.Empty;
                var dbConnection = dbTransactionContext.Connection;
                var transaction = dbTransactionContext.CurrentTransaction;
                try
                {
                    stagingTableName = CommonUtil.GetStagingTableName(tableMapping, true, dbConnection);
                    string destinationTableName = string.Format("[{0}].[{1}]", tableMapping.Schema, tableMapping.TableName);
                    string[] keyColumnNames = options.JoinOnCondition != null ? CommonUtil<T>.GetColumns(options.JoinOnCondition, new[] { "s" })
                        : tableMapping.GetPrimaryKeyColumns().ToArray();
                    IEnumerable<string> columnNames = CommonUtil.FilterColumns<T>(tableMapping.GetColumns(true), keyColumnNames, options.InputColumns, options.IgnoreColumns);
                    IEnumerable<string> columnsToFetch = CommonUtil.FormatColumns("t", columnNames);

                    if (keyColumnNames.Length == 0 && options.JoinOnCondition == null)
                        throw new InvalidDataException("BulkFetch requires that the entity have a primary key or the Options.JoinOnCondition must be set.");

                    context.Database.CloneTable(destinationTableName, stagingTableName, keyColumnNames);
                    BulkInsert(entities, options, tableMapping, dbConnection, transaction, stagingTableName, keyColumnNames, SqlBulkCopyOptions.KeepIdentity, false);
                    selectSql = string.Format("SELECT {0} FROM {1} s JOIN {2} t ON {3}", SqlUtil.ConvertToColumnString(columnsToFetch), stagingTableName, destinationTableName,
                        CommonUtil<T>.GetJoinConditionSql(options.JoinOnCondition, keyColumnNames));


                    dbTransactionContext.Commit();
                }
                catch (Exception ex)
                {
                    dbTransactionContext.Rollback();
                    throw;
                }

                foreach (var item in context.FetchInternal<T>(selectSql))
                {
                    yield return item;
                }
                context.Database.DropTable(stagingTableName);
            }
        }
        private static void Validate(TableMapping tableMapping)
        {
            if (!tableMapping.GetPrimaryKeyColumns().Any())
            {
                throw new Exception("You must have a primary key on this table to use this function.");
            }
        }
        public static void Fetch<T>(this IQueryable<T> querable, Action<FetchResult<T>> action, Action<FetchOptions<T>> optionsAction) where T : class, new()
        {
            Fetch(querable, action, optionsAction.Build());
        }
        public static void Fetch<T>(this IQueryable<T> querable, Action<FetchResult<T>> action, FetchOptions<T> options) where T : class, new()
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
                var reader = command.ExecuteReader();

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
                while (reader.Read())
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
                        action(new FetchResult<T> { Results = entities, Batch = batch });
                        entities.Clear();
                        count = 0;
                        batch++;
                    }
                }

                if (entities.Count > 0)
                    action(new FetchResult<T> { Results = entities, Batch = batch });
                //close the DataReader
                reader.Close();
            }
        }
        private static IEnumerable<T> FetchInternal<T>(this DbContext dbContext, string sqlText, object[] parameters = null) where T : class, new()
        {
            using (var command = dbContext.Database.CreateCommand(Enums.ConnectionBehavior.New))
            {
                command.CommandText = sqlText;
                if (parameters != null)
                    command.Parameters.AddRange(parameters);

                var reader = command.ExecuteReader();

                List<PropertyInfo> propertySetters = new List<PropertyInfo>();
                var entityType = typeof(T);
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    propertySetters.Add(entityType.GetProperty(reader.GetName(i)));
                }
                while (reader.Read())
                {
                    var entity = new T();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        var value = reader.GetValue(i);
                        if (value == DBNull.Value)
                            value = null;
                        propertySetters[i].SetValue(entity, value);
                    }
                    yield return entity;
                }
                //close the DataReader
                reader.Close();
            }
        }
        public static int BulkInsert<T>(this DbContext context, IEnumerable<T> entities)
        {
            return context.BulkInsert<T>(entities, new BulkInsertOptions<T> { });
        }
        public static int BulkInsert<T>(this DbContext context, IEnumerable<T> entities, Action<BulkInsertOptions<T>> optionsAction)
        {
            return context.BulkInsert<T>(entities, optionsAction.Build());
        }
        public static int BulkInsert<T>(this DbContext context, IEnumerable<T> entities, BulkInsertOptions<T> options)
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

                    context.Database.CloneTable(destinationTableName, stagingTableName, columnNames, Common.Constants.InternalId_ColumnName);
                    var bulkInsertResult = BulkInsert(entities, options, tableMapping, dbConnection, transaction, stagingTableName, columnNames, SqlBulkCopyOptions.KeepIdentity, true);

                    List<string> columnsToOutput = new List<string> { "$Action", string.Format("[{0}].[{1}]", "s", Constants.InternalId_ColumnName) };
                    var entityProperties = new List<IProperty>();
                    var entityType = tableMapping.EntityType;

                    foreach (var autoGeneratedColumnName in autoGeneratedColumnNames)
                    {
                        columnsToOutput.Add(string.Format("[inserted].[{0}]", autoGeneratedColumnName));
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
                    var bulkQueryResult = context.BulkQuery(insertSqlText, dbConnection, transaction, options);
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
                catch (Exception ex)
                {
                    dbTransactionContext.Rollback();
                    throw;
                }
            }
        }

        internal static void SetStoreGeneratedValues<T>(this DbContext context, T entity, List<IProperty> properties, object[] values)
        {
            int index = 0;
            var updateEntry = entity as InternalEntityEntry;
            if(updateEntry == null)
            {
                var entry = context.Entry(entity);
                updateEntry = entry.GetInfrastructure();
            }

            if(updateEntry != null)
            {
                foreach(var property in properties)
                {
                    updateEntry.SetStoreGeneratedValue(property, values[index]);
                    index++;
                }
                if(updateEntry.EntityState == EntityState.Detached)
                    updateEntry.AcceptChanges();
            }
            else
            {
                throw new InvalidOperationException("SetStoreValues() failed because an instance of InternalEntityEntry was not found.");
            }
        }

        private static BulkInsertResult<T> BulkInsert<T>(IEnumerable<T> entities, BulkOptions options, TableMapping tableMapping, SqlConnection dbConnection, SqlTransaction transaction, string tableName,
            IEnumerable<string> inputColumns = null, SqlBulkCopyOptions bulkCopyOptions = SqlBulkCopyOptions.Default, bool useInteralId = false)
        {
            using (var dataReader = new EntityDataReader<T>(tableMapping, entities, useInteralId))
            {

                var sqlBulkCopy = new SqlBulkCopy(dbConnection, bulkCopyOptions | options.BulkCopyOptions, transaction)
                {
                    DestinationTableName = tableName,
                    BatchSize = options.BatchSize,
                    NotifyAfter = options.NotifyAfter,
                    EnableStreaming = options.EnableStreaming,
                };
                sqlBulkCopy.BulkCopyTimeout = options.CommandTimeout.HasValue ? options.CommandTimeout.Value : sqlBulkCopy.BulkCopyTimeout;
                if (options.SqlRowsCopied != null)
                {
                    sqlBulkCopy.SqlRowsCopied += options.SqlRowsCopied;
                }
                foreach (SqlBulkCopyColumnOrderHint columnOrderHint in options.ColumnOrderHints)
                {
                    sqlBulkCopy.ColumnOrderHints.Add(columnOrderHint);
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
                sqlBulkCopy.WriteToServer(dataReader);

                return new BulkInsertResult<T>
                {
                    RowsAffected = sqlBulkCopy.RowsCopied,
                    EntityMap = dataReader.EntityMap
                };
            }
        }
        public static BulkMergeResult<T> BulkMerge<T>(this DbContext context, IEnumerable<T> entities)
        {
            return BulkMerge(context, entities, new BulkMergeOptions<T>());
        }
        public static BulkMergeResult<T> BulkMerge<T>(this DbContext context, IEnumerable<T> entities, BulkMergeOptions<T> options)
        {
            return InternalBulkMerge(context, entities, options);
        }
        public static BulkMergeResult<T> BulkMerge<T>(this DbContext context, IEnumerable<T> entities, Action<BulkMergeOptions<T>> optionsAction)
        {
            return BulkMerge(context, entities, optionsAction.Build());
        }
        public static int BulkSaveChanges(this DbContext dbContext)
        {
            return dbContext.BulkSaveChanges(true);
        }
        public static int BulkSaveChanges(this DbContext dbContext, bool acceptAllChangesOnSuccess=true)
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
                    rowsAffected += dbContext.BulkInsert(entities, o => { o.EntityType = key.EntityType; });
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
            
            if(acceptAllChangesOnSuccess)
                dbContext.ChangeTracker.AcceptAllChanges();

            return rowsAffected;
        }
        public static BulkSyncResult<T> BulkSync<T>(this DbContext context, IEnumerable<T> entities)
        {
            return BulkSync(context, entities, new BulkSyncOptions<T>());
        }
        public static BulkSyncResult<T> BulkSync<T>(this DbContext context, IEnumerable<T> entities, Action<BulkSyncOptions<T>> optionsAction)
        {
            return BulkSyncResult<T>.Map(InternalBulkMerge(context, entities, optionsAction.Build()));
        }
        public static BulkSyncResult<T> BulkSync<T>(this DbContext context, IEnumerable<T> entities, BulkSyncOptions<T> options)
        {
            return BulkSyncResult<T>.Map(InternalBulkMerge(context, entities, options));
        }
        private static BulkMergeResult<T> InternalBulkMerge<T>(this DbContext context, IEnumerable<T> entities, BulkMergeOptions<T> options)
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
                    string[] columnNames = tableMapping.GetColumns().ToArray();
                    string[] primaryKeyColumnNames = tableMapping.GetPrimaryKeyColumns().ToArray();
                    IEnumerable<string> autoGeneratedColumnNames = options.AutoMapOutput ? tableMapping.GetAutoGeneratedColumns() : new string[] { };

                    if (primaryKeyColumnNames.Length == 0 && options.MergeOnCondition == null)
                        throw new InvalidDataException("BulkMerge requires that the entity have a primary key or the Options.MergeOnCondition must be set.");

                    context.Database.CloneTable(destinationTableName, stagingTableName, null, Common.Constants.InternalId_ColumnName);
                    var bulkInsertResult = BulkInsert(entities, options, tableMapping, dbConnection, transaction, stagingTableName, null, SqlBulkCopyOptions.KeepIdentity, true);

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

                    var bulkQueryResult = context.BulkQuery(mergeSqlText, dbConnection, transaction, options);
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
            int rowsUpdated = 0;
            var outputRows = new List<BulkMergeOutputRow<T>>();
            var tableMapping = context.GetTableMapping(typeof(T), options.EntityType);

            using (var dbTransactionContext = new DbTransactionContext(context, options))
            {
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

                    context.Database.CloneTable(destinationTableName, stagingTableName, null);
                    BulkInsert(entities, options, tableMapping, dbConnection, transaction, stagingTableName, null, SqlBulkCopyOptions.KeepIdentity);

                    IEnumerable<string> columnstoUpdate = CommonUtil.FormatColumns(columnNames.Where(o => !options.IgnoreColumns.GetObjectProperties().Contains(o)));

                    string updateSetExpression = string.Join(",", columnstoUpdate.Select(o => string.Format("t.{0}=s.{0}", o)));
                    string updateSql = string.Format("UPDATE t SET {0} FROM {1} AS s JOIN {2} AS t ON {3}; SELECT @@RowCount;",
                        updateSetExpression, stagingTableName, destinationTableName, CommonUtil<T>.GetJoinConditionSql(options.UpdateOnCondition, primaryKeyColumnNames, "s", "t"));

                    rowsUpdated = context.Database.ExecuteSql(updateSql, options.CommandTimeout);
                    context.Database.DropTable(stagingTableName);

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

        private static void ClearEntityStateToUnchanged<T>(DbContext dbContext, IEnumerable<T> entities)
        {
            foreach (var entity in entities)
            {
                var entry = dbContext.Entry(entity);
                if (entry.State == EntityState.Added || entry.State == EntityState.Modified)
                    dbContext.Entry(entity).State = EntityState.Unchanged;
            }
        }

        private static BulkQueryResult BulkQuery(this DbContext context, string sqlText, SqlConnection dbConnection, SqlTransaction transaction, BulkOptions options)
        {
            var results = new List<object[]>();
            var columns = new List<string>();
            //var command = new SqlCommand(sqlText, dbConnection, transaction);
            var command = context.Database.CreateCommand();
            command.CommandText = sqlText;
            if (options.CommandTimeout.HasValue)
            {
                command.CommandTimeout = options.CommandTimeout.Value;
            }
            var reader = command.ExecuteReader();
            //Get column names
            for (int i = 0; i < reader.FieldCount; i++)
            {
                columns.Add(reader.GetName(i));
            }
            try
            {
                //Read data
                while (reader.Read())
                {
                    Object[] values = new Object[reader.FieldCount];
                    reader.GetValues(values);
                    results.Add(values);
                }
            }
            finally
            {
                //close the DataReader
                reader.Close();
            }

            return new BulkQueryResult
            {
                Columns = columns,
                Results = results,
                RowsAffected = reader.RecordsAffected
            };
        }
        public static int DeleteFromQuery<T>(this IQueryable<T> querable, int? commandTimeout = null) where T : class
        {
            int rowAffected = 0;
            using (var dbTransactionContext = new DbTransactionContext(querable.GetDbContext(), commandTimeout))
            {
                var dbContext = dbTransactionContext.DbContext;
                try
                {
                    var sqlQuery = SqlBuilder.Parse(querable.ToQueryString());
                    sqlQuery.ChangeToDelete();
                    rowAffected =  dbContext.Database.ExecuteSql(sqlQuery.Sql, sqlQuery.Parameters.ToArray());

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
        public static int InsertFromQuery<T>(this IQueryable<T> querable, string tableName, Expression<Func<T, object>> insertObjectExpression, int? commandTimeout = null) where T : class
        {
            int rowAffected = 0;
            using (var dbTransactionContext = new DbTransactionContext(querable.GetDbContext(), commandTimeout))
            {
                var dbContext = dbTransactionContext.DbContext;
                try
                {
                    var sqlQuery = SqlBuilder.Parse(querable.ToQueryString());
                    if (dbContext.Database.TableExists(tableName))
                    {
                        sqlQuery.ChangeToInsert(tableName, insertObjectExpression);
                        dbContext.Database.ToggleIdentityInsert(true, tableName);
                        rowAffected = dbContext.Database.ExecuteSql(sqlQuery.Sql, sqlQuery.Parameters.ToArray());
                        dbContext.Database.ToggleIdentityInsert(false, tableName);
                    }
                    else
                    {
                        sqlQuery.Clauses.First().InputText += string.Format(" INTO {0}", tableName);
                        rowAffected = dbContext.Database.ExecuteSql(sqlQuery.Sql, sqlQuery.Parameters.ToArray());
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
        public static int UpdateFromQuery<T>(this IQueryable<T> querable, Expression<Func<T, T>> updateExpression, int? commandTimeout = null) where T : class
        {
            int rowAffected = 0;
            using (var dbTransactionContext = new DbTransactionContext(querable.GetDbContext(), commandTimeout))
            {
                var dbContext = dbTransactionContext.DbContext;
                try
                {
                    var sqlQuery = SqlBuilder.Parse(querable.ToQueryString());
                    string setSqlExpression = updateExpression.ToSqlUpdateSetExpression(sqlQuery.GetTableAlias());
                    sqlQuery.ChangeToUpdate(sqlQuery.GetTableAlias(), setSqlExpression);
                    rowAffected = dbContext.Database.ExecuteSql(sqlQuery.Sql, sqlQuery.Parameters.ToArray());
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
        public static QueryToFileResult QueryToCsvFile<T>(this IQueryable<T> querable, String filePath) where T : class
        {
            return QueryToCsvFile<T>(querable, filePath, new QueryToFileOptions());
        }
        public static QueryToFileResult QueryToCsvFile<T>(this IQueryable<T> querable, Stream stream) where T : class
        {
            return QueryToCsvFile<T>(querable, stream, new QueryToFileOptions());
        }
        public static QueryToFileResult QueryToCsvFile<T>(this IQueryable<T> querable, String filePath, Action<QueryToFileOptions> optionsAction) where T : class
        {
            return QueryToCsvFile<T>(querable, filePath, optionsAction.Build());
        }
        public static QueryToFileResult QueryToCsvFile<T>(this IQueryable<T> querable, Stream stream, Action<QueryToFileOptions> optionsAction) where T : class
        {
            return QueryToCsvFile<T>(querable, stream, optionsAction.Build());
        }
        public static QueryToFileResult QueryToCsvFile<T>(this IQueryable<T> querable, String filePath, QueryToFileOptions options) where T : class
        {
            var fileStream = File.Create(filePath);
            return QueryToCsvFile<T>(querable, fileStream, options);
        }
        public static QueryToFileResult QueryToCsvFile<T>(this IQueryable<T> querable, Stream stream, QueryToFileOptions options) where T : class
        {
            return InternalQueryToFile<T>(querable, stream, options);
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
            var fileStream = File.Create(filePath);
            return SqlQueryToCsvFile(database, fileStream, options, sqlText, parameters);
        }
        public static QueryToFileResult SqlQueryToCsvFile(this DatabaseFacade database, Stream stream, QueryToFileOptions options, string sqlText, params object[] parameters)
        {
            var dbConnection = database.GetDbConnection() as SqlConnection;
            return InternalQueryToFile(dbConnection, stream, options, sqlText, parameters);
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
            var dbConnection = dbContext.GetSqlConnection();
            dbContext.Database.TruncateTable(tableMapping.FullQualifedTableName);
        }
        private static QueryToFileResult InternalQueryToFile<T>(this IQueryable<T> querable, Stream stream, QueryToFileOptions options) where T : class
        {
            var dbContext = querable.GetDbContext();
            var dbConnection = dbContext.GetSqlConnection();
            return InternalQueryToFile(dbConnection, stream, options, querable.ToQueryString());
        }
        private static QueryToFileResult InternalQueryToFile(SqlConnection dbConnection, Stream stream, QueryToFileOptions options, string sqlText, object[] parameters = null)
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

            var streamWriter = new StreamWriter(stream);
            using (var reader = command.ExecuteReader())
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
                            streamWriter.Write(options.ColumnDelimiter);
                        }
                    }
                    totalRowCount++;
                    streamWriter.Write(options.RowDelimiter);
                }
                //Write data rows to file
                while (reader.Read())
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
                            streamWriter.Write(options.ColumnDelimiter);
                        }
                    }
                    streamWriter.Write(options.RowDelimiter);
                    dataRowCount++;
                    totalRowCount++;
                }
                streamWriter.Flush();
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
        public static IQueryable<T> UsingTable<T>(this IQueryable<T> querable, string tableName) where T : class
        {
            var dbContext = querable.GetDbContext();
            var tableMapping = dbContext.GetTableMapping(typeof(T));
            efExtensionsCommandInterceptor.AddCommand(Guid.NewGuid(),
                new EfExtensionsCommand
                {
                    CommandType = EfExtensionsCommandType.ChangeTableName,
                    OldValue = string.Format("[{0}]", tableMapping.TableName),
                    NewValue = string.Format("[{0}].[{1}]", tableMapping.Schema, tableName),
                    Connection = dbContext.GetSqlConnection()
                });
            return querable;
        }
        internal static DbContext GetDbContext<T>(this IQueryable<T> querable) where T : class
        {
            DbContext dbContext;
            try
            {
                if ((querable as InternalDbSet<T>) != null)
                {
                    dbContext = querable.GetPrivateFieldValue("_context") as DbContext;
                }
                else if ((querable as EntityQueryable<T>) != null)
                {
                    var queryCompiler = querable.Provider.GetPrivateFieldValue("_queryCompiler");
                    var contextFactory = queryCompiler.GetPrivateFieldValue("_queryContextFactory");
                    var queryDependencies = contextFactory.GetPrivateFieldValue("Dependencies") as QueryContextDependencies;
                    dbContext = queryDependencies.CurrentContext.Context as DbContext;
                }
                else
                {
                    throw new Exception("This extension method could not find the DbContext for this type that implements IQuerable");
                }
            }
            catch
            {
                throw new Exception("This extension method could not find the DbContext for this type that implements IQuerable");
            }
            return dbContext;
        }
        internal static SqlConnection GetSqlConnection(this DbContext context, ConnectionBehavior connectionBehavior = ConnectionBehavior.Default)
        {
            var dbConnection = context.Database.GetDbConnection();
            return connectionBehavior == ConnectionBehavior.New ? ((ICloneable)dbConnection).Clone() as SqlConnection : dbConnection as SqlConnection;
            //return context.Database.GetDbConnection() as SqlConnection;
        }

        //private static string ToSqlPredicate<T>(this Expression<T> expression, params string[] parameters)
        //{
        //    var stringBuilder = new StringBuilder((string)expression.Body.GetPrivateFieldValue("DebugView"));
        //    int i = 0;
        //    foreach (var expressionParam in expression.Parameters)
        //    {
        //        if (parameters.Length <= i) break;
        //        stringBuilder.Replace((string)expressionParam.GetPrivateFieldValue("DebugView"), parameters[i]);
        //        i++;
        //    }
        //    stringBuilder.Replace("&&", "AND");
        //    stringBuilder.Replace("==", "=");
        //    return stringBuilder.ToString();
        //}
        //private static string ToSqlUpdateSetExpression<T>(this Expression<T> expression, string tableName)
        //{
        //    List<string> setValues = new List<string>();
        //    var memberInitExpression = expression.Body as MemberInitExpression;
        //    foreach (var binding in memberInitExpression.Bindings)
        //    {
        //        var constantExpression = binding.GetPrivateFieldValue("Expression") as ConstantExpression;
        //        var setValue = "";
        //        if(constantExpression.Value == null)
        //        {
        //            setValue = string.Format("[{0}].[{1}]=NULL", tableName, binding.Member.Name);
        //        }
        //        else
        //        {
        //            setValue = string.Format("[{0}].[{1}]='{2}'", tableName, binding.Member.Name, constantExpression.Value);
        //        }
        //        setValues.Add(setValue);
        //    }
        //    return string.Join(",", setValues);
        //}

        public static TableMapping GetTableMapping(this DbContext dbContext, Type type, IEntityType entityType = null)
        {
            entityType = entityType != null ? entityType : dbContext.Model.FindEntityType(type);
            return new TableMapping(dbContext, entityType);
        }
    }
}

