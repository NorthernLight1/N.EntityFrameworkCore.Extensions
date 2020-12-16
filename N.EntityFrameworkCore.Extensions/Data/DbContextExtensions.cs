using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using N.EntityFrameworkCore.Extensions.Common;
using N.EntityFrameworkCore.Extensions.Sql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace N.EntityFrameworkCore.Extensions
{
    public static partial class DbContextExtensions
    {
        static DbContextExtensions()
        {

        }
        public static int BulkDelete<T>(this DbContext context, IEnumerable<T> entities)
        {
            return context.BulkDelete(entities, new BulkDeleteOptions<T>());
        }
        public static int BulkDelete<T>(this DbContext context, IEnumerable<T> entities, BulkDeleteOptions<T> options)
        {
            int rowsAffected = 0;
            var tableMapping = context.GetTableMapping(typeof(T));
            Validate(tableMapping);

            var dbConnection = context.GetSqlConnection();

            if (dbConnection.State == ConnectionState.Closed)
                dbConnection.Open();

            using (var transaction = dbConnection.BeginTransaction())
            {
                try
                {
                    string stagingTableName = GetStagingTableName(tableMapping, options.UsePermanentTable, dbConnection);
                    string destinationTableName = string.Format("[{0}].[{1}]", tableMapping.Schema, tableMapping.TableName);
                    string[] storeGeneratedColumnNames = tableMapping.GetPrimaryKeyColumns().ToArray();
                    string deleteCondition = string.Join(" AND ", storeGeneratedColumnNames.Select(o => string.Format("s.{0}=t.{0}", o)));

                    SqlUtil.CloneTable(destinationTableName, stagingTableName, storeGeneratedColumnNames, dbConnection, transaction);
                    BulkInsert(entities, options, tableMapping, dbConnection, transaction, stagingTableName, storeGeneratedColumnNames, SqlBulkCopyOptions.KeepIdentity);
                    string deleteSql = string.Format("DELETE t FROM {0} s JOIN {1} t ON {2}", stagingTableName, destinationTableName, deleteCondition);
                    rowsAffected = SqlUtil.ExecuteSql(deleteSql, dbConnection, transaction);
                    SqlUtil.DeleteTable(stagingTableName, dbConnection, transaction);
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    throw ex;
                }
                finally
                {
                    dbConnection.Close();
                }
                return rowsAffected;
            }
        }

        private static void Validate(TableMapping tableMapping)
        {
            if(!tableMapping.GetPrimaryKeyColumns().Any())
            {
                throw new Exception("You must have a primary key on this table to use this function.");
            }
        }

        public static int BulkInsert<T>(this DbContext context, IEnumerable<T> entities)
        {
            return context.BulkInsert<T>(entities, new BulkInsertOptions<T> { });
        }

        public static int BulkInsert<T>(this DbContext context, IEnumerable<T> entities, BulkInsertOptions<T> options)
        {
            int rowsAffected = 0;
            var tableMapping = context.GetTableMapping(typeof(T));
            var dbConnection = context.GetSqlConnection();

            if (dbConnection.State == ConnectionState.Closed)
                dbConnection.Open();

            using (var transaction = dbConnection.BeginTransaction())
            {
                try
                {
                    string stagingTableName = GetStagingTableName(tableMapping, options.UsePermanentTable, dbConnection);
                    string destinationTableName = string.Format("[{0}].[{1}]", tableMapping.Schema, tableMapping.TableName);
                    string[] columnNames = tableMapping.GetColumns(options.KeepIdentity);
                    string[] storeGeneratedColumnNames = tableMapping.GetPrimaryKeyColumns().ToArray();

                    SqlUtil.CloneTable(destinationTableName, stagingTableName, null, dbConnection, transaction, Common.Constants.Guid_ColumnName);
                    var bulkInsertResult = BulkInsert(entities, options, tableMapping, dbConnection, transaction, stagingTableName, null, SqlBulkCopyOptions.KeepIdentity, true);

                    IEnumerable<string> columnsToInsert = columnNames;

                    List<string> columnsToOutput = new List<string>();
                    List<PropertyInfo> propertySetters = new List<PropertyInfo>();
                    Type entityType = typeof(T);

                    foreach (var storeGeneratedColumnName in storeGeneratedColumnNames)
                    {
                        columnsToOutput.Add(string.Format("inserted.[{0}]", storeGeneratedColumnName));
                        propertySetters.Add(entityType.GetProperty(storeGeneratedColumnName));
                    }

                    string mergeSqlText = string.Format("INSERT INTO {0} ({1}) OUTPUT {2} SELECT {3} FROM {4};",
                        destinationTableName, SqlUtil.ConvertToColumnString(columnsToInsert), SqlUtil.ConvertToColumnString(columnsToOutput), SqlUtil.ConvertToColumnString(columnsToInsert), stagingTableName);

                    if (options.KeepIdentity)
                        SqlUtil.ToggleIdentiyInsert(true, destinationTableName, dbConnection, transaction);
                    var bulkQueryResult = context.BulkQuery(mergeSqlText, dbConnection, transaction);
                    if (options.KeepIdentity)
                        SqlUtil.ToggleIdentiyInsert(false, destinationTableName, dbConnection, transaction);
                    rowsAffected = bulkQueryResult.RowsAffected;

                    if (options.AutoMapOutputIdentity)
                    {
                        if (rowsAffected == entities.Count())
                        {
                            var entityIndex = 1;
                            foreach (var result in bulkQueryResult.Results)
                            {
                                var entity = bulkInsertResult.EntityMap[entityIndex];
                                propertySetters[0].SetValue(entity, result[0]);
                                entityIndex++;
                            }
                        }
                    }

                    SqlUtil.DeleteTable(stagingTableName, dbConnection, transaction);

                    //ClearEntityStateToUnchanged(context, entities);
                    transaction.Commit();
                    return rowsAffected;
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    throw ex;
                }
                finally
                {
                    dbConnection.Close();
                }

            }
        }

        private static BulkInsertResult<T> BulkInsert<T>(IEnumerable<T> entities, BulkOptions options, TableMapping tableMapping, SqlConnection dbConnection, SqlTransaction transaction, string tableName,
            string[] inputColumns = null, SqlBulkCopyOptions bulkCopyOptions = SqlBulkCopyOptions.Default, bool useInteralId=false)
        {
            var dataReader = new EntityDataReader<T>(tableMapping, entities, useInteralId);

            var sqlBulkCopy = new SqlBulkCopy(dbConnection, bulkCopyOptions, transaction)
            {
                DestinationTableName = tableName,
                BatchSize = options.BatchSize
            };
            foreach (var property in dataReader.TableMapping.Properties)
            {
                if (inputColumns == null || (inputColumns != null && inputColumns.Contains(property.Name)))
                    sqlBulkCopy.ColumnMappings.Add(property.Name, property.Name);
            }
            if (useInteralId)
            {
                sqlBulkCopy.ColumnMappings.Add(Constants.Guid_ColumnName, Constants.Guid_ColumnName);
            }
            sqlBulkCopy.WriteToServer(dataReader);

            return new BulkInsertResult<T> {
                RowsCopied = sqlBulkCopy.RowsCopied,
                EntityMap = dataReader.EntityMap
            };
        }

        public static BulkMergeResult<T> BulkMerge<T>(this DbContext context, IEnumerable<T> entities, BulkMergeOptions<T> options)
        {
            int rowsAffected = 0;
            var outputRows = new List<BulkMergeOutputRow<T>>();
            var tableMapping = context.GetTableMapping(typeof(T));
            var dbConnection = context.GetSqlConnection();
            int rowsInserted = 0;
            int rowsUpdated = 0;
            int rowsDeleted = 0;

            if (dbConnection.State == ConnectionState.Closed)
                dbConnection.Open();

            using (var transaction = dbConnection.BeginTransaction())
            {
                try
                {
                    string stagingTableName = GetStagingTableName(tableMapping, options.UsePermanentTable, dbConnection);
                    string destinationTableName = string.Format("[{0}].[{1}]", tableMapping.Schema, tableMapping.TableName);
                    string[] columnNames = tableMapping.GetNonValueGeneratedColumns().ToArray();
                    string[] storeGeneratedColumnNames = tableMapping.GetPrimaryKeyColumns().ToArray();

                    SqlUtil.CloneTable(destinationTableName, stagingTableName, null, dbConnection, transaction, Common.Constants.Guid_ColumnName);
                    var bulkInsertResult = BulkInsert(entities, options, tableMapping, dbConnection, transaction, stagingTableName, null, SqlBulkCopyOptions.KeepIdentity, true);

                    IEnumerable<string> columnsToInsert = columnNames.Where(o => !options.GetIgnoreColumnsOnInsert().Contains(o));
                    IEnumerable<string> columnstoUpdate = columnNames.Where(o => !options.GetIgnoreColumnsOnUpdate().Contains(o)).Select(o => string.Format("t.{0}=s.{0}", o));
                    List<string> columnsToOutput = new List<string> { "$Action", string.Format("{0}.{1}","s", Constants.Guid_ColumnName) };
                    List<PropertyInfo> propertySetters = new List<PropertyInfo>();
                    Type entityType = typeof(T);

                    foreach (var storeGeneratedColumnName in storeGeneratedColumnNames)
                    {
                        //columnsToOutput.Add(string.Format("deleted.[{0}]", storeGeneratedColumnName)); Not Yet Supported
                        columnsToOutput.Add(string.Format("inserted.[{0}]", storeGeneratedColumnName));
                        propertySetters.Add(entityType.GetProperty(storeGeneratedColumnName));
                    }

                    string mergeSqlText = string.Format("MERGE {0} t USING {1} s ON ({2}) WHEN NOT MATCHED BY TARGET THEN INSERT ({3}) VALUES ({3}) WHEN MATCHED THEN UPDATE SET {4} OUTPUT {5};",
                        destinationTableName, stagingTableName, options.MergeOnCondition.ToSqlPredicate("s", "t"),
                        SqlUtil.ConvertToColumnString(columnsToInsert),
                        SqlUtil.ConvertToColumnString(columnstoUpdate),
                        SqlUtil.ConvertToColumnString(columnsToOutput)
                        );

                    var bulkQueryResult = context.BulkQuery(mergeSqlText, dbConnection, transaction);
                    rowsAffected = bulkQueryResult.RowsAffected;

                    //var entitiesEnumerator = entities.GetEnumerator();
                    //entitiesEnumerator.MoveNext();
                    foreach (var result in bulkQueryResult.Results)
                    {
                        string action = (string)result[0];
                        int id = (int)result[1];
                        var entity = bulkInsertResult.EntityMap[id];
                        outputRows.Add(new BulkMergeOutputRow<T>(action, entity));
                        if (options.AutoMapOutputIdentity && entity != null)
                        {
                            
                            for (int i = 2; i < result.Length; i++)
                            {
                                propertySetters[0].SetValue(entity, result[i]);
                            }
                        }
                        if (action == SqlMergeAction.Insert) rowsInserted++;
                        else if (action == SqlMergeAction.Update) rowsUpdated++;
                        else if (action == SqlMergeAction.Detete) rowsDeleted++;
                    }
                    SqlUtil.DeleteTable(stagingTableName, dbConnection, transaction);

                    //ClearEntityStateToUnchanged(context, entities);
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    throw ex;
                }
                finally
                {
                    dbConnection.Close();
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
        public static int BulkUpdate<T>(this DbContext context, IEnumerable<T> entities, BulkUpdateOptions<T> options)
        {
            int rowsUpdated = 0;
            var outputRows = new List<BulkMergeOutputRow<T>>();
            var tableMapping = context.GetTableMapping(typeof(T));
            var dbConnection = context.GetSqlConnection();

            if (dbConnection.State == ConnectionState.Closed)
                dbConnection.Open();

            using (var transaction = dbConnection.BeginTransaction())
            {
                try
                {
                    string stagingTableName = GetStagingTableName(tableMapping, options.UsePermanentTable, dbConnection);
                    string destinationTableName = string.Format("[{0}].[{1}]", tableMapping.Schema, tableMapping.TableName);
                    string[] columnNames = tableMapping.GetNonValueGeneratedColumns().ToArray();
                    string[] storeGeneratedColumnNames = tableMapping.GetPrimaryKeyColumns().ToArray();

                    SqlUtil.CloneTable(destinationTableName, stagingTableName, null, dbConnection, transaction);
                    BulkInsert(entities, options, tableMapping, dbConnection, transaction, stagingTableName, null, SqlBulkCopyOptions.KeepIdentity);

                    IEnumerable<string> columnstoUpdate = columnNames.Where(o => !options.IgnoreColumnsOnUpdate.GetObjectProperties().Contains(o));

                    string updateSetExpression = string.Join(",", columnstoUpdate.Select(o => string.Format("t.{0}=s.{0}", o)));
                    string updateOnExpression = string.Join(" AND ", storeGeneratedColumnNames.Select(o => string.Format("s.{0}=t.{0}", o)));
                    string updateSql = string.Format("UPDATE t SET {0} FROM {1} AS s JOIN {2} AS t ON {3}; SELECT @@RowCount;",
                        updateSetExpression, stagingTableName, destinationTableName, updateOnExpression);

                    rowsUpdated = SqlUtil.ExecuteSql(updateSql, dbConnection, transaction);
                    SqlUtil.DeleteTable(stagingTableName, dbConnection, transaction);

                    //ClearEntityStateToUnchanged(context, entities);
                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    throw ex;
                }
                finally
                {
                    dbConnection.Close();
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

        private static string GetStagingTableName(TableMapping tableMapping, bool usePermanentTable, SqlConnection sqlConnection)
        {
            string tableName = string.Empty;
            if (usePermanentTable)
                tableName = string.Format("[{0}].[tmp_be_xx_{1}_{2}]", tableMapping.Schema, tableMapping.TableName, sqlConnection.ClientConnectionId.ToString());
            else
                tableName = string.Format("[{0}].[#tmp_be_xx_{1}]", tableMapping.Schema, tableMapping.TableName);
            return tableName;
        }

        private static BulkQueryResult BulkQuery(this DbContext context, string sqlText, SqlConnection dbConnection, SqlTransaction transaction)
        {
            var results = new List<object[]>();
            var columns = new List<string>();
            var command = new SqlCommand(sqlText, dbConnection, transaction);
            var reader = command.ExecuteReader();
            //Get column names
            for (int i = 0; i < reader.FieldCount; i++)
            {
                columns.Add(reader.GetName(i));
            }
            //Read data
            while (reader.Read())
            {
                Object[] values = new Object[reader.FieldCount];
                reader.GetValues(values);
                results.Add(values);
            }

            //close the DataReader
            reader.Close();
            return new BulkQueryResult
            {
                Columns = columns,
                Results = results,
                RowsAffected = reader.RecordsAffected
            };
        }
        public static int DeleteFromQuery<T>(this IQueryable<T> querable) where T : class
        {
            int rowAffected = 0;
            var dbContext = GetDbContextFromIQuerable(querable);
            var dbConnection = dbContext.GetSqlConnection();
            //Open datbase connection
            if (dbConnection.State == ConnectionState.Closed)
                dbConnection.Open();
            using (var dbTransaction = dbConnection.BeginTransaction())
            {
                try
                {
                    var sqlQuery = SqlQuery.Parse(querable.ToQueryString());
                    sqlQuery.ChangeToDelete("[o]");
                    rowAffected = SqlUtil.ExecuteSql(sqlQuery.Sql, dbConnection, dbTransaction);

                    dbTransaction.Commit();
                }
                catch (Exception ex)
                {
                    dbTransaction.Rollback();
                    throw ex;
                }
                finally
                {
                    dbConnection.Close();
                }
            }
            return rowAffected;
        }
        public static int InsertFromQuery<T>(this IQueryable<T> querable, string tableName, Expression<Func<T, object>> insertObjectExpression) where T: class
        {
            int rowAffected = 0;
            var dbContext = GetDbContextFromIQuerable(querable);
            var dbConnection = dbContext.GetSqlConnection();
            //Open datbase connection
            if (dbConnection.State == ConnectionState.Closed)
                dbConnection.Open();

            using (var dbTransaction = dbConnection.BeginTransaction())
            {
                try
                {
                    var sqlQuery = SqlQuery.Parse(querable.ToQueryString());
                    if (SqlUtil.TableExists(tableName, dbConnection, dbTransaction))
                    {
                        sqlQuery.ChangeToInsert(tableName, insertObjectExpression);
                        SqlUtil.ToggleIdentiyInsert(true, tableName, dbConnection, dbTransaction);
                        rowAffected = SqlUtil.ExecuteSql(sqlQuery.Sql, dbConnection, dbTransaction);
                        SqlUtil.ToggleIdentiyInsert(false, tableName, dbConnection, dbTransaction);
                    }
                    else
                    {
                        sqlQuery.Clauses.First().InputText += string.Format(" INTO {0}", tableName);
                        rowAffected = SqlUtil.ExecuteSql(sqlQuery.Sql, dbConnection, dbTransaction);
                    }

                    dbTransaction.Commit();
                }
                catch (Exception ex)
                {
                    dbTransaction.Rollback();
                    throw ex;
                }
                finally
                {
                    dbConnection.Close();
                }
            }
            return rowAffected;
        }
        public static int UpdateFromQuery<T>(this IQueryable<T> querable, Expression<Func<T, T>> updateExpression) where T: class
        {
            int rowAffected = 0;
            var dbContext = GetDbContextFromIQuerable(querable);
            var dbConnection = dbContext.GetSqlConnection();
            //Open datbase connection
            if (dbConnection.State == ConnectionState.Closed)
                dbConnection.Open();

            using (var dbTransaction = dbConnection.BeginTransaction())
            {
                try
                {
                    var sqlQuery = SqlQuery.Parse(querable.ToQueryString());
                    string setSqlExpression = updateExpression.ToSqlUpdateSetExpression("o");
                    sqlQuery.ChangeToUpdate("[o]", setSqlExpression);
                    rowAffected = SqlUtil.ExecuteSql(sqlQuery.Sql, dbConnection, dbTransaction);
                    dbTransaction.Commit();
                }
                catch (Exception ex)
                {
                    dbTransaction.Rollback();
                    throw ex;
                }
                finally
                {
                    dbConnection.Close();
                }
            }
            return rowAffected;
        }
        private static DbContext GetDbContextFromIQuerable<T>(IQueryable<T> querable) where T : class
        {
            DbContext dbContext;
            try
            {
                if ((querable as DbSet<T>) != null)
                {
                    dbContext = querable.GetPrivateFieldValue("_context") as DbContext;
                }
                else if((querable as EntityQueryable<T>) != null)
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
        private static SqlConnection GetSqlConnection(this DbContext context)
        
        {
            return context.Database.GetDbConnection() as SqlConnection;
        }

        private static string ToSqlPredicate<T>(this Expression<T> expression, params string[] parameters)
        {
            var stringBuilder = new StringBuilder((string)expression.Body.GetPrivateFieldValue("DebugView"));
            int i = 0;
            foreach (var expressionParam in expression.Parameters)
            {
                if (parameters.Length <= i) break;
                stringBuilder.Replace((string)expressionParam.GetPrivateFieldValue("DebugView"), parameters[i]);
                i++;
            }
            stringBuilder.Replace("&&", "AND");
            stringBuilder.Replace("==", "=");
            return stringBuilder.ToString();
        }
        private static string ToSqlUpdateSetExpression<T>(this Expression<T> expression, string tableName)
        {
            List<string> setValues = new List<string>();
            var memberInitExpression = expression.Body as MemberInitExpression;
            foreach (var binding in memberInitExpression.Bindings)
            {
                var constantExpression = binding.GetPrivateFieldValue("Expression") as ConstantExpression;
                setValues.Add(string.Format("[{0}].[{1}]='{2}'", tableName, binding.Member.Name, constantExpression.Value));
            }
            return string.Join(",", setValues);
        }

        public static TableMapping GetTableMapping(this DbContext dbContext, Type type)
        {
            var entityType = dbContext.Model.FindEntityType(type);
            return new TableMapping(entityType);
        }
    }
}

