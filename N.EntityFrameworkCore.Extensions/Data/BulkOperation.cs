using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.Extensions.Options;
using N.EntityFrameworkCore.Extensions.Util;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Transactions;

namespace N.EntityFrameworkCore.Extensions
{
    internal partial class BulkOperation<T> : IDisposable
    {
        internal SqlConnection Connection => DbTransactionContext.Connection;
        internal DbContext Context { get; }
        internal bool StagingTableCreated { get; set; }
        internal string StagingTableName { get; }
        internal string[] PrimaryKeyColumnNames { get; }
        internal BulkOptions Options { get; }
        internal Expression<Func<T, object>> InputColumns { get; }
        internal Expression<Func<T, object>> IgnoreColumns { get; }
        internal DbTransactionContext DbTransactionContext { get; }
        internal SqlTransaction Transaction => DbTransactionContext.CurrentTransaction;
        internal TableMapping TableMapping { get; }
        internal IEnumerable<string> SchemaQualifiedTableNames => TableMapping.GetSchemaQualifiedTableNames();
        

        public BulkOperation(DbContext dbContext, BulkOptions options, Expression<Func<T, object>> inputColumns, Expression<Func<T, object>> ignoreColumns)
        {
            Context = dbContext;
            Options = options;
            InputColumns = inputColumns;
            IgnoreColumns = ignoreColumns;

            DbTransactionContext = new DbTransactionContext(dbContext, options.CommandTimeout);
            TableMapping = dbContext.GetTableMapping(typeof(T), options.EntityType);
            StagingTableName = CommonUtil.GetStagingTableName(TableMapping, options.UsePermanentTable, Connection);
            PrimaryKeyColumnNames = TableMapping.GetPrimaryKeyColumns().ToArray();
        }
        internal BulkInsertResult<T> BulkInsertStagingData(IEnumerable<T> entities, bool keepIdentity=false)
        {
            IEnumerable<string> columnsToInsert = GetStagingColumnNames(keepIdentity);
            Context.Database.CloneTable(SchemaQualifiedTableNames, StagingTableName, TableMapping.GetQualifiedColumnNames(columnsToInsert));
            StagingTableCreated = true;
            return DbContextExtensions.BulkInsert(entities, Options, TableMapping, Connection, Transaction, StagingTableName, columnsToInsert, SqlBulkCopyOptions.KeepIdentity);
        }
        internal int ExecuteUpdate(IEnumerable<T> entities, Expression<Func<T, T, bool>> updateOnCondition)
        {
            int rowsUpdated = 0;
            foreach (var entityType in TableMapping.EntityTypes)
            {
                IEnumerable<string> columnstoUpdate = CommonUtil.FormatColumns(GetColumnNames(entityType));
                string updateSetExpression = string.Join(",", columnstoUpdate.Select(o => string.Format("t.{0}=s.{0}", o)));
                string updateSql = string.Format("UPDATE t SET {0} FROM {1} AS s JOIN {2} AS t ON {3}; SELECT @@RowCount;",
                    updateSetExpression, StagingTableName, entityType.GetTableName(), CommonUtil<T>.GetJoinConditionSql(updateOnCondition, PrimaryKeyColumnNames, "s", "t"));
                rowsUpdated = Context.Database.ExecuteSql(updateSql, Options.CommandTimeout);
            }
            return rowsUpdated;
        }
        internal void ValidateBulkUpdate<T>(Expression<Func<T, T, bool>> updateOnCondition)
        {
            if (PrimaryKeyColumnNames.Length == 0 && updateOnCondition == null)
                throw new InvalidDataException("BulkUpdate requires that the entity have a primary key or the Options.UpdateOnCondition must be set.");

        }
        public void Dispose()
        {
            if(StagingTableCreated)
            {
                Context.Database.DropTable(StagingTableName);
            }
        }
        internal IEnumerable<string> GetColumnNames(IEntityType entityType, bool keepIdentity=false)
        {
            IEnumerable<string> columnNames = CommonUtil.FilterColumns(TableMapping.GetColumns(keepIdentity), PrimaryKeyColumnNames, InputColumns, IgnoreColumns);
            return TableMapping.GetColumnNames(entityType).Intersect(columnNames);
        }
        internal IEnumerable<string> GetStagingColumnNames(bool keepIdentity=false)
        {
            IEnumerable<string> columnNames = CommonUtil.FilterColumns(TableMapping.GetColumns(keepIdentity), PrimaryKeyColumnNames, InputColumns, IgnoreColumns);
            return columnNames.Union(PrimaryKeyColumnNames);
        }
    }
}
