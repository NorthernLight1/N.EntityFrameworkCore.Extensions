using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using N.EntityFrameworkCore.Extensions.Common;
using N.EntityFrameworkCore.Extensions.Sql;
using N.EntityFrameworkCore.Extensions.Util;

namespace N.EntityFrameworkCore.Extensions;

internal sealed partial class BulkOperation<T> : IDisposable
{
    internal DbConnection Connection => DbTransactionContext.Connection;
    internal DbContext Context { get; }
    internal bool StagingTableCreated { get; set; }
    internal string StagingTableName { get; }
    internal string[] PrimaryKeyColumnNames { get; }
    internal BulkOptions Options { get; }
    internal Expression<Func<T, object>> InputColumns { get; }
    internal Expression<Func<T, object>> IgnoreColumns { get; }
    internal DbTransactionContext DbTransactionContext { get; }
    internal Type EntityType => typeof(T);
    internal DbTransaction Transaction => DbTransactionContext.CurrentTransaction;
    internal TableMapping TableMapping { get; }
    internal IEnumerable<string> SchemaQualifiedTableNames => TableMapping.GetSchemaQualifiedTableNames();


    public BulkOperation(DbContext dbContext, BulkOptions options, Expression<Func<T, object>> inputColumns = null, Expression<Func<T, object>> ignoreColumns = null)
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
    public void Dispose()
    {
        if (StagingTableCreated)
        {
            Context.Database.DropTable(StagingTableName, true);
        }
    }
    internal bool ShouldKeepIdentityForPostgresMerge()
    {
        return Context.Database.IsPostgreSql()
            && GetGeneratedPrimaryKeyProperty()?.PropertyInfo != null
            && PrimaryKeyColumnNames.Length == 1;
    }
    internal bool ShouldPreallocateIdentityValues(bool autoMapOutput, bool keepIdentity, IEnumerable<T> entities)
    {
        if (!Context.Database.IsPostgreSql() || keepIdentity || !autoMapOutput)
            return false;

        var identityProperty = GetGeneratedPrimaryKeyProperty();
        if (identityProperty?.PropertyInfo == null || PrimaryKeyColumnNames.Length != 1)
            return false;

        var entityList = entities as IList<T> ?? entities.ToList();
        if (entityList.Count == 0)
            return false;

        // For BulkSaveChanges, entities are InternalEntityEntry (Added state) — always preallocate
        if (entityList[0] is InternalEntityEntry)
            return true;

        // For regular POCOs, only preallocate if all entities have the default PK value
        object defaultValue = identityProperty.ClrType.IsValueType ? Activator.CreateInstance(identityProperty.ClrType) : null;
        return entityList.All(entity => Equals(identityProperty.PropertyInfo.GetValue(entity), defaultValue));
    }
    internal void PreallocateIdentityValues(IEnumerable<T> entities)
    {
        var identityProperty = GetGeneratedPrimaryKeyProperty();
        if (identityProperty?.PropertyInfo == null)
            return;

        var entityList = entities.ToList();
        if (entityList.Count == 0)
            return;

        string tableName = Context.DelimitIdentifier(TableMapping.EntityType.GetTableName(), TableMapping.EntityType.GetSchema() ?? Context.Database.GetDefaultSchema());
        string sequenceSql = $"SELECT nextval(pg_get_serial_sequence('{tableName}', '{identityProperty.GetColumnName()}')) FROM generate_series(1, {entityList.Count})";
        using var command = Connection.CreateCommand();
        command.CommandText = sequenceSql;
        command.Transaction = Transaction;
        using var reader = command.ExecuteReader();
        foreach (var entity in entityList)
        {
            if (!reader.Read())
                throw new InvalidDataException("Failed to allocate PostgreSql identity values.");

            object sequenceValue = Convert.ChangeType(reader.GetValue(0), identityProperty.ClrType);
#pragma warning disable EF1001
            if (entity is InternalEntityEntry internalEntry)
                internalEntry.SetStoreGeneratedValue(identityProperty, sequenceValue);
#pragma warning restore EF1001
            else
                identityProperty.PropertyInfo.SetValue(entity, sequenceValue);
        }
    }
    internal BulkInsertResult<T> BulkInsertStagingData(IEnumerable<T> entities, bool keepIdentity = true, bool useInternalId = false)
    {
        IEnumerable<string> columnsToInsert = GetColumnNames(keepIdentity);
        string internalIdColumn = useInternalId ? Common.Constants.InternalId_ColumnName : null;
        Context.Database.CloneTable(SchemaQualifiedTableNames, StagingTableName, TableMapping.GetQualifiedColumnNames(columnsToInsert), internalIdColumn);
        StagingTableCreated = true;
        return DbContextExtensions.BulkInsert(entities, Options, TableMapping, Connection, Transaction, StagingTableName, columnsToInsert, SqlBulkCopyOptions.KeepIdentity, useInternalId);
    }
    internal BulkMergeResult<T> ExecuteMerge(Dictionary<long, T> entityMap, Expression<Func<T, T, bool>> mergeOnCondition,
        bool autoMapOutput, bool keepIdentity, bool insertIfNotExists, bool update = false, bool delete = false, bool preallocatedIds = false)
    {
        if (Context.Database.IsPostgreSql())
            return ExecuteMergePostgreSql(entityMap, mergeOnCondition, autoMapOutput, keepIdentity, insertIfNotExists, update, delete, preallocatedIds);

        Dictionary<IEntityType, int> rowsInserted = [];
        Dictionary<IEntityType, int> rowsUpdated = [];
        Dictionary<IEntityType, int> rowsDeleted = [];
        Dictionary<IEntityType, int> rowsAffected = [];
        List<BulkMergeOutputRow<T>> outputRows = [];

        foreach (var entityType in TableMapping.EntityTypes)
        {
            rowsInserted[entityType] = 0;
            rowsUpdated[entityType] = 0;
            rowsDeleted[entityType] = 0;
            rowsAffected[entityType] = 0;

            var columnsToInsert = GetColumnNames(entityType, keepIdentity);
            var columnsToUpdate = update ? GetColumnNames(entityType) : [];
            var autoGeneratedColumns = autoMapOutput ? TableMapping.GetAutoGeneratedColumns(entityType) : [];
            var columnsToOutput = autoMapOutput ? GetMergeOutputColumns(autoGeneratedColumns, delete) : [];
            var deleteEntityType = TableMapping.EntityType == entityType && delete ? delete : false;

            string mergeOnConditionSql = insertIfNotExists ? CommonUtil<T>.GetJoinConditionSql(mergeOnCondition, PrimaryKeyColumnNames, "t", "s") : "1=2";
            bool toggleIdentity = keepIdentity && TableMapping.HasIdentityColumn;
            var mergeStatement = SqlStatement.CreateMerge(StagingTableName, entityType.GetSchemaQualifiedTableName(),
                mergeOnConditionSql, columnsToInsert, columnsToUpdate, columnsToOutput, deleteEntityType, toggleIdentity);

            if (autoMapOutput)
            {
                List<IProperty> allProperties =
                [
                    .. TableMapping.GetEntityProperties(entityType, ValueGenerated.OnAdd).ToArray(),
                        .. TableMapping.GetEntityProperties(entityType, ValueGenerated.OnAddOrUpdate).ToArray()
                ];

                var bulkQueryResult = Context.BulkQuery(mergeStatement.Sql, Options);
                rowsAffected[entityType] = bulkQueryResult.RowsAffected;

                foreach (var result in bulkQueryResult.Results)
                {
                    string action = (string)result[0];
                    outputRows.Add(new BulkMergeOutputRow<T>(action));

                    if (action == SqlMergeAction.Delete)
                    {
                        rowsDeleted[entityType]++;
                    }
                    else
                    {
                        int entityId = (int)result[1];
                        var entity = entityMap[entityId];
                        if (action == SqlMergeAction.Insert)
                        {
                            rowsInserted[entityType]++;
                            if (allProperties.Count != 0)
                            {
                                var entityValues = GetMergeOutputValues(columnsToOutput, result, allProperties);
                                Context.SetStoreGeneratedValues(entity, allProperties, entityValues);
                            }
                        }
                        else if (action == SqlMergeAction.Update)
                        {
                            rowsUpdated[entityType]++;
                            if (allProperties.Count != 0)
                            {
                                var entityValues = GetMergeOutputValues(columnsToOutput, result, allProperties);
                                Context.SetStoreGeneratedValues(entity, allProperties, entityValues);
                            }
                        }
                    }
                }
            }
            else
            {
                rowsAffected[entityType] = Context.Database.ExecuteSqlInternal(mergeStatement.Sql, Options.CommandTimeout);
            }
        }
        return new BulkMergeResult<T>
        {
            Output = outputRows,
            RowsAffected = rowsAffected.Values.LastOrDefault(),
            RowsDeleted = rowsDeleted.Values.LastOrDefault(),
            RowsInserted = rowsInserted.Values.LastOrDefault(),
            RowsUpdated = rowsUpdated.Values.LastOrDefault()
        };
    }

    private IEnumerable<string> GetMergeOutputColumns(IEnumerable<string> autoGeneratedColumns, bool delete = false)
    {
        List<string> columnsToOutput = ["$action", $"[s].[{Constants.InternalId_ColumnName}]"];
        columnsToOutput.AddRange(autoGeneratedColumns.Select(o => $"[inserted].[{o}]"));
        return columnsToOutput;
    }
    private object[] GetMergeOutputValues(IEnumerable<string> columns, object[] values, IEnumerable<IProperty> properties)
    {
        var columnList = columns.ToList();
        var valuesIndex = properties.Select(o => columnList.IndexOf($"[inserted].[{o.GetColumnName()}]"));
        return valuesIndex.Select(i => values[i]).ToArray();
    }
    internal int ExecuteUpdate(IEnumerable<T> entities, Expression<Func<T, T, bool>> updateOnCondition)
    {
        if (Context.Database.IsPostgreSql())
            return ExecuteUpdatePostgreSql(updateOnCondition);

        int rowsUpdated = 0;
        foreach (var entityType in TableMapping.EntityTypes)
        {
            IEnumerable<string> columnsToUpdate = CommonUtil.FormatColumns(GetColumnNames(entityType));
            string updateSetExpression = string.Join(",", columnsToUpdate.Select(o => $"t.{o}=s.{o}"));
            string updateSql = $"UPDATE t SET {updateSetExpression} FROM {StagingTableName} AS s JOIN {CommonUtil.FormatTableName(entityType.GetSchemaQualifiedTableName())} AS t ON {CommonUtil<T>.GetJoinConditionSql(updateOnCondition, PrimaryKeyColumnNames, "s", "t")}; SELECT @@RowCount;";
            rowsUpdated = Context.Database.ExecuteSqlInternal(updateSql, Options.CommandTimeout);
        }
        return rowsUpdated;
    }
    private BulkMergeResult<T> ExecuteMergePostgreSql(Dictionary<long, T> entityMap, Expression<Func<T, T, bool>> mergeOnCondition,
        bool autoMapOutput, bool keepIdentity, bool insertIfNotExists, bool update, bool delete, bool preallocatedIds = false)
    {
        Dictionary<IEntityType, int> rowsInserted = [];
        Dictionary<IEntityType, int> rowsUpdated = [];
        Dictionary<IEntityType, int> rowsDeleted = [];
        List<BulkMergeOutputRow<T>> outputRows = [];

        foreach (var entityType in TableMapping.EntityTypes)
        {
            var targetTableName = Context.DelimitIdentifier(entityType.GetTableName(), entityType.GetSchema() ?? Context.Database.GetDefaultSchema());
            var columnsToInsert = GetColumnNames(entityType, keepIdentity).ToList();
            var columnsToUpdate = update ? GetColumnNames(entityType).ToList() : [];
            var autoGeneratedColumns = autoMapOutput ? TableMapping.GetAutoGeneratedColumns(entityType).ToList() : [];
            var allProperties = autoMapOutput
                ? TableMapping.GetEntityProperties(entityType, ValueGenerated.OnAdd).Concat(TableMapping.GetEntityProperties(entityType, ValueGenerated.OnAddOrUpdate)).ToList()
                : [];

            string matchJoinCondition = CommonUtil<T>.GetJoinConditionSql(Context, mergeOnCondition, PrimaryKeyColumnNames, "s", "t");
            string pkJoinCondition = CommonUtil<T>.GetJoinConditionSql(Context, null, PrimaryKeyColumnNames, "s", "t");
            string joinCondition = insertIfNotExists ? matchJoinCondition : "1=2";

            HashSet<int> matchedIds = autoMapOutput && update
                ? GetMatchedInternalIds(targetTableName, matchJoinCondition)
                : [];

            rowsUpdated[entityType] = 0;
            if (columnsToUpdate.Count > 0)
            {
                string updateSetExpression = string.Join(",", columnsToUpdate.Select(c => $"{Context.DelimitIdentifier(c)}={Context.DelimitMemberAccess("s", c)}"));
                string updateSql = $"UPDATE {targetTableName} AS t SET {updateSetExpression} FROM {StagingTableName} AS s WHERE {joinCondition}";
                rowsUpdated[entityType] = Context.Database.ExecuteSqlInternal(updateSql, Options.CommandTimeout);
            }

            string insertColumnsSql = string.Join(",", columnsToInsert.Select(Context.DelimitIdentifier));
            string sourceColumnsSql = string.Join(",", columnsToInsert.Select(c => Context.DelimitMemberAccess("s", c)));
            string insertSql = $"INSERT INTO {targetTableName} ({insertColumnsSql}) SELECT {sourceColumnsSql} FROM {StagingTableName} AS s WHERE NOT EXISTS (SELECT 1 FROM {targetTableName} AS t WHERE {joinCondition})";
            rowsInserted[entityType] = Context.Database.ExecuteSqlInternal(insertSql, Options.CommandTimeout);
            if (keepIdentity && rowsInserted[entityType] > 0)
                SyncPostgreSqlIdentitySequence(entityType);

            rowsDeleted[entityType] = 0;
            if (TableMapping.EntityType == entityType && delete)
            {
                // When IDs were preallocated (entities had Id=0), staging PKs are new sequences that don't match
                // existing target PKs (UPDATE excludes PK from SET). Use the merge condition to identify rows to keep.
                // When entities had explicit IDs, staging PKs match inserted/updated target rows → use PK-based delete.
                string deleteJoinCondition = (preallocatedIds && mergeOnCondition != null) ? matchJoinCondition : pkJoinCondition;
                string deleteSql = $"DELETE FROM {targetTableName} AS t WHERE NOT EXISTS (SELECT 1 FROM {StagingTableName} AS s WHERE {deleteJoinCondition})";
                rowsDeleted[entityType] = Context.Database.ExecuteSqlInternal(deleteSql, Options.CommandTimeout);
                for (int i = 0; i < rowsDeleted[entityType]; i++)
                    outputRows.Add(new BulkMergeOutputRow<T>(SqlMergeAction.Delete));
            }

            if (autoMapOutput)
            {
                string outputColumnsSql = autoGeneratedColumns.Any()
                    ? "," + string.Join(",", autoGeneratedColumns.Select(c => Context.DelimitMemberAccess("t", c)))
                    : string.Empty;
                var outputQuery = $"SELECT {Context.DelimitMemberAccess("s", Constants.InternalId_ColumnName)}{outputColumnsSql} FROM {StagingTableName} AS s JOIN {targetTableName} AS t ON {matchJoinCondition}";
                var bulkQueryResult = Context.BulkQuery(outputQuery, Options);
                var autoGeneratedColumnList = autoGeneratedColumns.ToList();
                foreach (var result in bulkQueryResult.Results)
                {
                    int entityId = Convert.ToInt32(result[0]);
                    bool wasMatched = matchedIds.Contains(entityId);
                    string action = wasMatched ? SqlMergeAction.Update : SqlMergeAction.Insert;
                    outputRows.Add(new BulkMergeOutputRow<T>(action));

                    if (entityMap.TryGetValue(entityId, out var entity) && allProperties.Count > 0)
                    {
                        object[] entityValues = allProperties.Select(p => result[1 + autoGeneratedColumnList.IndexOf(p.GetColumnName())]).ToArray();
                        Context.SetStoreGeneratedValues(entity, allProperties, entityValues);
                    }
                }
            }
        }

        return new BulkMergeResult<T>
        {
            Output = outputRows,
            RowsAffected = rowsInserted.Values.LastOrDefault() + rowsUpdated.Values.LastOrDefault() + rowsDeleted.Values.LastOrDefault(),
            RowsDeleted = rowsDeleted.Values.LastOrDefault(),
            RowsInserted = rowsInserted.Values.LastOrDefault(),
            RowsUpdated = rowsUpdated.Values.LastOrDefault()
        };
    }
    private int ExecuteUpdatePostgreSql(Expression<Func<T, T, bool>> updateOnCondition)
    {
        int rowsUpdated = 0;
        foreach (var entityType in TableMapping.EntityTypes)
        {
            IEnumerable<string> columnsToUpdate = GetColumnNames(entityType);
            string updateSetExpression = string.Join(",", columnsToUpdate.Select(c => $"{Context.DelimitIdentifier(c)}={Context.DelimitMemberAccess("s", c)}"));
            string targetTableName = Context.DelimitIdentifier(entityType.GetTableName(), entityType.GetSchema() ?? Context.Database.GetDefaultSchema());
            string updateSql = $"UPDATE {targetTableName} AS t SET {updateSetExpression} FROM {StagingTableName} AS s WHERE {CommonUtil<T>.GetJoinConditionSql(Context, updateOnCondition, PrimaryKeyColumnNames, "s", "t")}";
            rowsUpdated = Context.Database.ExecuteSqlInternal(updateSql, Options.CommandTimeout);
        }
        return rowsUpdated;
    }
    private HashSet<int> GetMatchedInternalIds(string targetTableName, string joinCondition)
    {
        var results = Context.BulkQuery(
            $"SELECT {Context.DelimitMemberAccess("s", Constants.InternalId_ColumnName)} FROM {StagingTableName} AS s JOIN {targetTableName} AS t ON {joinCondition}",
            Options);
        return results.Results.Select(r => Convert.ToInt32(r[0])).ToHashSet();
    }
    private IProperty GetGeneratedPrimaryKeyProperty()
    {
        return TableMapping.EntityType.GetProperties().SingleOrDefault(o => o.IsPrimaryKey() && o.ValueGenerated != ValueGenerated.Never);
    }
    private void SyncPostgreSqlIdentitySequence(IEntityType entityType)
    {
        var identityProperty = entityType.GetProperties().SingleOrDefault(o => o.IsPrimaryKey() && o.ValueGenerated != ValueGenerated.Never);
        if (identityProperty == null)
            return;

        string tableName = Context.DelimitIdentifier(entityType.GetTableName(), entityType.GetSchema() ?? Context.Database.GetDefaultSchema());
        string columnName = Context.DelimitIdentifier(identityProperty.GetColumnName());
        string sequenceSql = $"SELECT setval(pg_get_serial_sequence('{tableName}', '{identityProperty.GetColumnName()}'), COALESCE(MAX({columnName}), 0)) FROM {tableName}";
        Context.Database.ExecuteSqlInternal(sequenceSql, Options.CommandTimeout);
    }
    internal void ValidateBulkMerge(Expression<Func<T, T, bool>> mergeOnCondition)
    {
        if (PrimaryKeyColumnNames.Length == 0 && mergeOnCondition == null)
            throw new InvalidDataException("BulkMerge requires that the entity have a primary key or that Options.MergeOnCondition be set");
    }
    internal void ValidateBulkUpdate(Expression<Func<T, T, bool>> updateOnCondition)
    {
        if (PrimaryKeyColumnNames.Length == 0 && updateOnCondition == null)
            throw new InvalidDataException("BulkUpdate requires that the entity have a primary key or the Options.UpdateOnCondition must be set.");

    }
    internal IEnumerable<string> GetColumnNames(bool includePrimaryKeys = false)
    {
        return GetColumnNames(null, includePrimaryKeys);
    }
    internal IEnumerable<string> GetColumnNames(IEntityType entityType, bool includePrimaryKeys = false)
    {
        return CommonUtil.FilterColumns(TableMapping.GetColumnNames(entityType, includePrimaryKeys), PrimaryKeyColumnNames, InputColumns, IgnoreColumns);
    }
}
