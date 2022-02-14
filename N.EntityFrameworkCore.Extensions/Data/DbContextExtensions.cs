using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using N.EntityFrameworkCore.Extensions.Common;
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
using System.Text;

namespace N.EntityFrameworkCore.Extensions
{
    public static class DbContextExtensions
    {
        private static EfExtensionsCommandInterceptor efExtensionsCommandInterceptor;
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
            int rowsAffected = 0;
            var tableMapping = context.GetTableMapping(typeof(T));

            using (var dbTransactionContext = new DbTransactionContext(context))
            {
                var dbConnection = dbTransactionContext.Connection;
                var transaction = dbTransactionContext.CurrentTransaction;
                try
                {
                    string stagingTableName = GetStagingTableName(tableMapping, options.UsePermanentTable, dbConnection);
                    string destinationTableName = string.Format("[{0}].[{1}]", tableMapping.Schema, tableMapping.TableName);
                    string[] keyColumnNames = options.DeleteOnCondition != null ? CommonUtil<T>.GetColumns(options.DeleteOnCondition, new[] { "s" })
                        : tableMapping.GetPrimaryKeyColumns().ToArray();

                    if (keyColumnNames.Length == 0 && options.DeleteOnCondition == null)
                        throw new InvalidDataException("BulkDelete requires that the entity have a primary key or the Options.DeleteOnCondition must be set.");


                    SqlUtil.CloneTable(destinationTableName, stagingTableName, keyColumnNames, dbConnection, transaction);
                    BulkInsert(entities, options, tableMapping, dbConnection, transaction, stagingTableName, keyColumnNames, SqlBulkCopyOptions.KeepIdentity, false);
                    string deleteSql = string.Format("DELETE t FROM {0} s JOIN {1} t ON {2}", stagingTableName, destinationTableName,
                        CommonUtil<T>.GetJoinConditionSql(options.DeleteOnCondition, keyColumnNames));
                    rowsAffected = SqlUtil.ExecuteSql(deleteSql, dbConnection, transaction, options.CommandTimeout);
                    SqlUtil.DropTable(stagingTableName, dbConnection, transaction);
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
        private static void Validate(TableMapping tableMapping)
        {
            if (!tableMapping.GetPrimaryKeyColumns().Any())
            {
                throw new Exception("You must have a primary key on this table to use this function.");
            }
        }
        public static void Fetch<T>(this IQueryable<T> querable, Action<FetchResult<T>> action, Action<FetchOptions> optionsAction) where T : class, new()
        {
            Fetch(querable, action, optionsAction.Build());
        }
        public static void Fetch<T>(this IQueryable<T> querable, Action<FetchResult<T>> action, FetchOptions options) where T : class, new()
        {
            var dbContext = querable.GetDbContext();
            var dbConnection = dbContext.GetSqlConnection();
            //Open datbase connection
            if (dbConnection.State == ConnectionState.Closed)
                dbConnection.Open();
            var command = new SqlCommand(querable.ToQueryString(), dbConnection);
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
            var tableMapping = context.GetTableMapping(typeof(T));

            using (var dbTransactionContext = new DbTransactionContext(context))
            {
                try
                {
                    var dbConnection = dbTransactionContext.Connection;
                    var transaction = dbTransactionContext.CurrentTransaction;
                    string stagingTableName = GetStagingTableName(tableMapping, options.UsePermanentTable, dbConnection);
                    string destinationTableName = string.Format("[{0}].[{1}]", tableMapping.Schema, tableMapping.TableName);
                    string[] columnNames = tableMapping.GetColumns(options.KeepIdentity);
                    string[] storeGeneratedColumnNames = tableMapping.GetPrimaryKeyColumns().ToArray();

                    SqlUtil.CloneTable(destinationTableName, stagingTableName, null, dbConnection, transaction, Common.Constants.InternalId_ColumnName);
                    var bulkInsertResult = BulkInsert(entities, options, tableMapping, dbConnection, transaction, stagingTableName, null, SqlBulkCopyOptions.KeepIdentity, true);

                    IEnumerable<string> columnsToInsert = columnNames;

                    List<string> columnsToOutput = new List<string> { "$Action", string.Format("{0}.{1}", "s", Constants.InternalId_ColumnName) };
                    List<PropertyInfo> propertySetters = new List<PropertyInfo>();
                    Type entityType = typeof(T);

                    foreach (var storeGeneratedColumnName in storeGeneratedColumnNames)
                    {
                        columnsToOutput.Add(string.Format("inserted.[{0}]", storeGeneratedColumnName));
                        propertySetters.Add(entityType.GetProperty(storeGeneratedColumnName));
                    }

                    string insertSqlText = string.Format("MERGE {0} t USING {1} s ON {2} WHEN NOT MATCHED THEN INSERT ({3}) VALUES ({3}){4};",
                        destinationTableName,
                        stagingTableName,
                        options.InsertIfNotExists ? CommonUtil<T>.GetJoinConditionSql(options.InsertOnCondition, storeGeneratedColumnNames, "t", "s") : "1=2",
                        SqlUtil.ConvertToColumnString(columnsToInsert),
                        columnsToOutput.Count > 0 ? " OUTPUT " + SqlUtil.ConvertToColumnString(columnsToOutput) : "");

                    if (options.KeepIdentity && storeGeneratedColumnNames.Length > 0)
                        SqlUtil.ToggleIdentityInsert(true, destinationTableName, dbConnection, transaction);
                    var bulkQueryResult = context.BulkQuery(insertSqlText, dbConnection, transaction, options);
                    if (options.KeepIdentity && storeGeneratedColumnNames.Length > 0)
                        SqlUtil.ToggleIdentityInsert(false, destinationTableName, dbConnection, transaction);
                    rowsAffected = bulkQueryResult.RowsAffected;

                    if (options.AutoMapOutputIdentity)
                    {
                        if (rowsAffected == entities.Count())
                        {
                            foreach (var result in bulkQueryResult.Results)
                            {
                                int entityId = (int)result[1];
                                var entity = bulkInsertResult.EntityMap[entityId];
                                for (int i = 2; i < columnsToOutput.Count; i++)
                                {
                                    propertySetters[2 - i].SetValue(entity, result[i]);
                                }
                            }
                        }
                    }

                    SqlUtil.DropTable(stagingTableName, dbConnection, transaction);

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
        private static BulkInsertResult<T> BulkInsert<T>(IEnumerable<T> entities, BulkOptions options, TableMapping tableMapping, SqlConnection dbConnection, SqlTransaction transaction, string tableName,
            string[] inputColumns = null, SqlBulkCopyOptions bulkCopyOptions = SqlBulkCopyOptions.Default, bool useInteralId = false)
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
                if (inputColumns == null || (inputColumns != null && inputColumns.Contains(property.Name)))
                    sqlBulkCopy.ColumnMappings.Add(property.Name, property.Name);
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

            using (var dbTransactionContext = new DbTransactionContext(context))
            {
                var dbConnection = dbTransactionContext.Connection;
                var transaction = dbTransactionContext.CurrentTransaction;
                try
                {
                    string stagingTableName = GetStagingTableName(tableMapping, options.UsePermanentTable, dbConnection);
                    string destinationTableName = string.Format("[{0}].[{1}]", tableMapping.Schema, tableMapping.TableName);
                    string[] columnNames = tableMapping.GetNonValueGeneratedColumns().ToArray();
                    string[] storeGeneratedColumnNames = tableMapping.GetPrimaryKeyColumns().ToArray();

                    if (storeGeneratedColumnNames.Length == 0 && options.MergeOnCondition == null)
                        throw new InvalidDataException("BulkMerge requires that the entity have a primary key or the Options.MergeOnCondition must be set.");

                    SqlUtil.CloneTable(destinationTableName, stagingTableName, null, dbConnection, transaction, Common.Constants.InternalId_ColumnName);
                    var bulkInsertResult = BulkInsert(entities, options, tableMapping, dbConnection, transaction, stagingTableName, null, SqlBulkCopyOptions.KeepIdentity, true);

                    IEnumerable<string> columnsToInsert = columnNames.Where(o => !options.GetIgnoreColumnsOnInsert().Contains(o));
                    IEnumerable<string> columnstoUpdate = columnNames.Where(o => !options.GetIgnoreColumnsOnUpdate().Contains(o)).Select(o => string.Format("t.{0}=s.{0}", o));
                    List<string> columnsToOutput = new List<string> { "$Action", string.Format("{0}.{1}", "s", Constants.InternalId_ColumnName) };
                    List<PropertyInfo> propertySetters = new List<PropertyInfo>();
                    Type entityType = typeof(T);

                    foreach (var storeGeneratedColumnName in storeGeneratedColumnNames)
                    {
                        columnsToOutput.Add(string.Format("inserted.[{0}]", storeGeneratedColumnName));
                        columnsToOutput.Add(string.Format("deleted.[{0}]", storeGeneratedColumnName));
                        propertySetters.Add(entityType.GetProperty(storeGeneratedColumnName));
                    }

                    string mergeSqlText = string.Format("MERGE {0} t USING {1} s ON ({2}) WHEN NOT MATCHED BY TARGET THEN INSERT ({3}) VALUES ({3}) WHEN MATCHED THEN UPDATE SET {4}{5}OUTPUT {6};",
                        destinationTableName, stagingTableName, CommonUtil<T>.GetJoinConditionSql(options.MergeOnCondition, storeGeneratedColumnNames, "s", "t"),
                        SqlUtil.ConvertToColumnString(columnsToInsert),
                        SqlUtil.ConvertToColumnString(columnstoUpdate),
                        options.DeleteIfNotMatched ? " WHEN NOT MATCHED BY SOURCE THEN DELETE " : " ",
                        SqlUtil.ConvertToColumnString(columnsToOutput)
                        );

                    var bulkQueryResult = context.BulkQuery(mergeSqlText, dbConnection, transaction, options);
                    rowsAffected = bulkQueryResult.RowsAffected;

                    foreach (var result in bulkQueryResult.Results)
                    {
                        string id = string.Empty;
                        object entity = null;
                        string action = (string)result[0];
                        if (action != SqlMergeAction.Delete)
                        {
                            int entityId = (int)result[1];
                            id = (storeGeneratedColumnNames.Length > 0 ? Convert.ToString(result[2]) : "PrimaryKeyMissing");
                            entity = bulkInsertResult.EntityMap[entityId];
                            if (options.AutoMapOutputIdentity && entity != null)
                            {

                                for (int i = 2; i < 2 + storeGeneratedColumnNames.Length; i++)
                                {
                                    propertySetters[0].SetValue(entity, result[i]);
                                }
                            }
                        }
                        else
                        {
                            id = Convert.ToString(result[2 + storeGeneratedColumnNames.Length]);
                        }
                        outputRows.Add(new BulkMergeOutputRow<T>(action, id));

                        if (action == SqlMergeAction.Insert) rowsInserted++;
                        else if (action == SqlMergeAction.Update) rowsUpdated++;
                        else if (action == SqlMergeAction.Delete) rowsDeleted++;
                    }
                    SqlUtil.DropTable(stagingTableName, dbConnection, transaction);

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
            var tableMapping = context.GetTableMapping(typeof(T));

            using (var dbTransactionContext = new DbTransactionContext(context))
            {
                var dbConnection = dbTransactionContext.Connection;
                var transaction = dbTransactionContext.CurrentTransaction;
                try
                {
                    string stagingTableName = GetStagingTableName(tableMapping, options.UsePermanentTable, dbConnection);
                    string destinationTableName = string.Format("[{0}].[{1}]", tableMapping.Schema, tableMapping.TableName);
                    string[] columnNames = tableMapping.GetNonValueGeneratedColumns().ToArray();
                    string[] storeGeneratedColumnNames = tableMapping.GetPrimaryKeyColumns().ToArray();

                    if (storeGeneratedColumnNames.Length == 0 && options.UpdateOnCondition == null)
                        throw new InvalidDataException("BulkUpdate requires that the entity have a primary key or the Options.UpdateOnCondition must be set.");

                    SqlUtil.CloneTable(destinationTableName, stagingTableName, null, dbConnection, transaction);
                    BulkInsert(entities, options, tableMapping, dbConnection, transaction, stagingTableName, null, SqlBulkCopyOptions.KeepIdentity);

                    IEnumerable<string> columnstoUpdate = columnNames.Where(o => !options.IgnoreColumnsOnUpdate.GetObjectProperties().Contains(o));

                    string updateSetExpression = string.Join(",", columnstoUpdate.Select(o => string.Format("t.{0}=s.{0}", o)));
                    string updateSql = string.Format("UPDATE t SET {0} FROM {1} AS s JOIN {2} AS t ON {3}; SELECT @@RowCount;",
                        updateSetExpression, stagingTableName, destinationTableName, CommonUtil<T>.GetJoinConditionSql(options.UpdateOnCondition, storeGeneratedColumnNames, "s", "t"));

                    rowsUpdated = SqlUtil.ExecuteSql(updateSql, dbConnection, transaction, options.CommandTimeout);
                    SqlUtil.DropTable(stagingTableName, dbConnection, transaction);

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

        internal static string GetStagingTableName(TableMapping tableMapping, bool usePermanentTable, SqlConnection sqlConnection)
        {
            string tableName = string.Empty;
            if (usePermanentTable)
                tableName = string.Format("[{0}].[tmp_be_xx_{1}_{2}]", tableMapping.Schema, tableMapping.TableName, sqlConnection.ClientConnectionId.ToString());
            else
                tableName = string.Format("[{0}].[#tmp_be_xx_{1}]", tableMapping.Schema, tableMapping.TableName);
            return tableName;
        }

        private static BulkQueryResult BulkQuery(this DbContext context, string sqlText, SqlConnection dbConnection, SqlTransaction transaction, BulkOptions options)
        {
            var results = new List<object[]>();
            var columns = new List<string>();
            var command = new SqlCommand(sqlText, dbConnection, transaction);
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
            using (var dbTransactionContext = new DbTransactionContext(querable.GetDbContext()))
            {
                var dbConnection = dbTransactionContext.Connection;
                var dbTransaction = dbTransactionContext.CurrentTransaction;
                try
                {
                    var sqlQuery = SqlBuilder.Parse(querable.ToQueryString());
                    sqlQuery.ChangeToDelete();
                    rowAffected = SqlUtil.ExecuteSql(sqlQuery.Sql, dbConnection, dbTransaction, sqlQuery.Parameters.ToArray(), commandTimeout);

                    dbTransactionContext.Commit();
                }
                catch (Exception ex)
                {
                    dbTransactionContext.Rollback();
                    throw ex;
                }
            }
            return rowAffected;
        }
        public static int InsertFromQuery<T>(this IQueryable<T> querable, string tableName, Expression<Func<T, object>> insertObjectExpression, int? commandTimeout = null) where T : class
        {
            int rowAffected = 0;
            using (var dbTransactionContext = new DbTransactionContext(querable.GetDbContext()))
            {
                var dbConnection = dbTransactionContext.Connection;
                var dbTransaction = dbTransactionContext.CurrentTransaction;
                try
                {
                    var sqlQuery = SqlBuilder.Parse(querable.ToQueryString());
                    if (SqlUtil.TableExists(tableName, dbConnection, dbTransaction))
                    {
                        sqlQuery.ChangeToInsert(tableName, insertObjectExpression);
                        SqlUtil.ToggleIdentityInsert(true, tableName, dbConnection, dbTransaction);
                        rowAffected = SqlUtil.ExecuteSql(sqlQuery.Sql, dbConnection, dbTransaction, sqlQuery.Parameters.ToArray(), commandTimeout);
                        SqlUtil.ToggleIdentityInsert(false, tableName, dbConnection, dbTransaction);
                    }
                    else
                    {
                        sqlQuery.Clauses.First().InputText += string.Format(" INTO {0}", tableName);
                        rowAffected = SqlUtil.ExecuteSql(sqlQuery.Sql, dbConnection, dbTransaction, sqlQuery.Parameters.ToArray(), commandTimeout);
                    }

                    dbTransactionContext.Commit();
                }
                catch (Exception ex)
                {
                    dbTransactionContext.Rollback();
                    throw ex;
                }
            }
            return rowAffected;
        }
        public static int UpdateFromQuery<T>(this IQueryable<T> querable, Expression<Func<T, T>> updateExpression, int? commandTimeout = null) where T : class
        {
            int rowAffected = 0;
            using (var dbTransactionContext = new DbTransactionContext(querable.GetDbContext()))
            {
                var dbConnection = dbTransactionContext.Connection;
                var dbTransaction = dbTransactionContext.CurrentTransaction;
                try
                {
                    var sqlQuery = SqlBuilder.Parse(querable.ToQueryString());
                    string setSqlExpression = updateExpression.ToSqlUpdateSetExpression(sqlQuery.GetTableAlias());
                    sqlQuery.ChangeToUpdate(sqlQuery.GetTableAlias(), setSqlExpression);
                    rowAffected = SqlUtil.ExecuteSql(sqlQuery.Sql, dbConnection, dbTransaction, sqlQuery.Parameters.ToArray(), commandTimeout);
                    dbTransactionContext.Commit();
                }
                catch (Exception ex)
                {
                    dbTransactionContext.Rollback();
                    throw ex;
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
            var dbConnection = dbContext.GetSqlConnection();
            SqlUtil.ClearTable(tableMapping.FullQualifedTableName, dbConnection, null);
        }
        public static void Truncate<T>(this DbSet<T> dbSet) where T : class
        {
            var dbContext = dbSet.GetDbContext();
            var tableMapping = dbContext.GetTableMapping(typeof(T));
            var dbConnection = dbContext.GetSqlConnection();
            SqlUtil.TruncateTable(tableMapping.FullQualifedTableName, dbConnection, null);
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

            StreamWriter streamWriter = new StreamWriter(stream);
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
                    var queryProvider = querable.GetPrivateFieldValue("_queryProvider");
                    var queryCompiler = queryProvider.GetPrivateFieldValue("_queryCompiler");
                    var contextFactory = queryCompiler.GetPrivateFieldValue("_queryContextFactory");
                    var queryDependencies = contextFactory.GetPrivateFieldValue("_dependencies") as QueryContextDependencies;
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
        internal static SqlConnection GetSqlConnection(this DbContext context)
        {
            return context.Database.GetDbConnection() as SqlConnection;
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

        public static TableMapping GetTableMapping(this DbContext dbContext, Type type)
        {
            var entityType = dbContext.Model.FindEntityType(type);
            return new TableMapping(dbContext, entityType);
        }
    }
}

