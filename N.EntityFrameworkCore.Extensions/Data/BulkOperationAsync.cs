using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using N.EntityFrameworkCore.Extensions.Util;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace N.EntityFrameworkCore.Extensions
{
    internal partial class BulkOperation<T>
    {
        internal async Task<BulkInsertResult<T>> BulkInsertStagingDataAsync(IEnumerable<T> entities, bool keepIdentity = false)
        {
            IEnumerable<string> columnsToInsert = GetStagingColumnNames(keepIdentity);
            await Context.Database.CloneTableAsync(SchemaQualifiedTableNames, StagingTableName, TableMapping.GetQualifiedColumnNames(columnsToInsert));
            StagingTableCreated = true;
            return await DbContextExtensionsAsync.BulkInsertAsync(entities, Options, TableMapping, Connection, Transaction, StagingTableName, columnsToInsert, SqlBulkCopyOptions.KeepIdentity);
        }
        internal async Task<int> ExecuteUpdateAsync(IEnumerable<T> entities, Expression<Func<T, T, bool>> updateOnCondition)
        {
            int rowsUpdated = 0;
            foreach (var entityType in TableMapping.EntityTypes)
            {
                IEnumerable<string> columnstoUpdate = CommonUtil.FormatColumns(GetColumnNames(entityType));
                string updateSetExpression = string.Join(",", columnstoUpdate.Select(o => string.Format("t.{0}=s.{0}", o)));
                string updateSql = string.Format("UPDATE t SET {0} FROM {1} AS s JOIN {2} AS t ON {3}; SELECT @@RowCount;",
                    updateSetExpression, StagingTableName, entityType.GetTableName(), CommonUtil<T>.GetJoinConditionSql(updateOnCondition, PrimaryKeyColumnNames, "s", "t"));
                rowsUpdated = await Context.Database.ExecuteSqlAsync(updateSql, Options.CommandTimeout);
            }
            return rowsUpdated;
        }
    }
}
