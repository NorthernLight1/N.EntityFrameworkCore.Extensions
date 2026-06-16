using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using N.EntityFrameworkCore.Extensions.Util;
using Oracle.ManagedDataAccess.Client;

namespace N.EntityFrameworkCore.Extensions;

internal static class OracleBulkOperations
{
    internal static int BulkInsert<T>(DbContext context, IEnumerable<T> entities, BulkInsertOptions<T> options)
    {
        var items = entities?.ToList() ?? [];
        if (items.Count == 0)
            return 0;

        var tableMapping = context.GetTableMapping(typeof(T), options?.EntityType);
        if (options?.InsertIfNotExists == true)
        {
            var keyProperty = tableMapping.EntityType.FindPrimaryKey()?.Properties.SingleOrDefault();
            if (keyProperty != null)
            {
                var existingKeys = GetSet<T>(context).Cast<T>()
                    .AsEnumerable()
                    .Select(entity => GetPropertyValue(entity!, keyProperty.Name))
                    .Where(value => value != null)
                    .Select(value => value.ToString())
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);

                items = items.Where(item =>
                {
                    var keyValue = GetPropertyValue(item!, keyProperty.Name);
                    return keyValue == null || !existingKeys.Contains(keyValue.ToString());
                }).ToList();
            }
        }

        if (items.Count == 0)
            return 0;

        var allowedColumns = GetInsertColumns(tableMapping, options?.InputColumns, options?.IgnoreColumns, options?.KeepIdentity ?? false);
        foreach (var item in items)
        {
            ApplyInsertColumnRules(item!, tableMapping, allowedColumns, options?.KeepIdentity ?? false);
        }

        ApplyInsertDefaults(context, tableMapping, items, options?.KeepIdentity ?? false);

        // TPT maps one entity to multiple physical tables, so each table gets its own bulk copy.
        ExecuteOracleBulkCopyForMappedTables(context, items, tableMapping, allowedColumns, options?.CommandTimeout);
        return items.Count;
    }

    internal static int BulkInsertIntoTable<T>(DbContext context, IEnumerable<T> entities, TableMapping tableMapping, string tableName, IEnumerable<string> columns, int? commandTimeout)
    {
        var items = entities?.ToList() ?? [];
        if (items.Count == 0)
            return 0;

        return ExecuteOracleBulkCopy(context, items, tableMapping, columns, commandTimeout, tableName);
    }

    private static int ExecuteOracleBulkCopy<T>(DbContext context, List<T> items, TableMapping tableMapping, IEnumerable<string> allowedColumns, int? commandTimeout = null, string destinationTableName = null)
    {
        var columns = allowedColumns?.Distinct(StringComparer.OrdinalIgnoreCase).ToList() ?? [];
        if (items.Count == 0 || columns.Count == 0)
            return 0;

        var connection = (OracleConnection)context.Database.GetDbConnection();
        if (connection.State != ConnectionState.Open)
            connection.Open();

        using var dataReader = new EntityDataReader<T>(tableMapping, items, false);
        using var bulkCopy = new OracleBulkCopy(connection);

        bulkCopy.DestinationTableName = destinationTableName ?? tableMapping.FullQualifedTableName;
        bulkCopy.BatchSize = items.Count;
        if (commandTimeout.HasValue)
            bulkCopy.BulkCopyTimeout = commandTimeout.Value;

        foreach (var column in columns)
        {
            bulkCopy.ColumnMappings.Add(column, $"\"{column}\"");
        }

        bulkCopy.WriteToServer(dataReader);
        return items.Count;
    }

    private static void ExecuteOracleBulkCopyForMappedTables<T>(DbContext context, List<T> items, TableMapping tableMapping, IEnumerable<string> allowedColumns, int? commandTimeout = null)
    {
        var allowed = allowedColumns?.ToHashSet(StringComparer.OrdinalIgnoreCase) ?? [];
        foreach (var entityType in tableMapping.EntityTypes)
        {
            var tableColumns = tableMapping.GetColumnNames(entityType, true)
                .Where(column => allowed.Count == 0 || allowed.Contains(column))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (tableColumns.Count == 0)
                continue;

            var tableName = context.DelimitIdentifier(entityType.GetTableName(), entityType.GetSchema() ?? context.Database.GetDefaultSchema());
            if (context.Database.CurrentTransaction != null && tableMapping.EntityTypes.Skip(1).Any())
                ExecuteOracleArrayInsert(context, items, tableMapping, tableColumns, commandTimeout, tableName);
            else
                ExecuteOracleBulkCopy(context, items, tableMapping, tableColumns, commandTimeout, tableName);
        }
    }

    private static int ExecuteOracleArrayInsert<T>(DbContext context, List<T> items, TableMapping tableMapping, IEnumerable<string> allowedColumns, int? commandTimeout, string destinationTableName)
    {
        var columns = allowedColumns?.Distinct(StringComparer.OrdinalIgnoreCase).ToList() ?? [];
        if (items.Count == 0 || columns.Count == 0)
            return 0;

        var connection = (OracleConnection)context.Database.GetDbConnection();
        if (connection.State != ConnectionState.Open)
            connection.Open();

        using var command = connection.CreateCommand();
        ConfigureCommand(context, command, string.Empty, commandTimeout);
        var insertColumns = columns.Select(column => $"\"{column}\"");
        var parameters = columns.Select((_, index) => $":p{index}").ToList();
        command.CommandText = $"INSERT INTO {destinationTableName} ({string.Join(", ", insertColumns)}) VALUES ({string.Join(", ", parameters)})";

        var properties = columns
            .Select(column => tableMapping.Properties.FirstOrDefault(p => string.Equals(tableMapping.GetColumnName(p), column, StringComparison.OrdinalIgnoreCase)))
            .ToList();

        var rowsInserted = 0;
        foreach (var item in items)
        {
            command.Parameters.Clear();
            for (var i = 0; i < columns.Count; i++)
            {
                var value = properties[i] == null ? null : GetPropertyValue(item!, properties[i]);
                command.Parameters.Add(new OracleParameter
                {
                    ParameterName = $"p{i}",
                    Value = value ?? DBNull.Value
                });
            }

            rowsInserted += command.ExecuteNonQuery();
        }

        return rowsInserted;
    }

    private static int ExecuteNonQuery(DbContext context, string sql, int? commandTimeout = null)
    {
        var connection = context.Database.GetDbConnection();
        if (connection.State != ConnectionState.Open)
            connection.Open();

        using var command = connection.CreateCommand();
        ConfigureCommand(context, command, sql, commandTimeout);
        return command.ExecuteNonQuery();
    }

    private static void ConfigureCommand(DbContext context, IDbCommand command, string sql, int? commandTimeout)
    {
        command.CommandText = sql;
        command.CommandTimeout = commandTimeout ?? 300;
        if (context.Database.CurrentTransaction != null)
            command.Transaction = context.Database.CurrentTransaction.GetDbTransaction();
    }

    private static string CreateStagingTable(DbContext context, TableMapping tableMapping)
    {
        string stagingTableName = $"STG_{tableMapping.TableName}_{Guid.NewGuid().ToString("N")[..8]}";

        var columns = tableMapping.GetColumns(true);
        var columnDefs = new List<string>();

        foreach (var column in columns)
        {
            var property = tableMapping.Properties.FirstOrDefault(p =>
                string.Equals(tableMapping.GetColumnName(p), column, StringComparison.OrdinalIgnoreCase));

            string oracleType = property != null
                ? GetOracleColumnType(property)
                : "NVARCHAR2(4000)";
            columnDefs.Add($"\"{column}\" {oracleType}");
        }

        if (columnDefs.Count == 0)
            throw new InvalidOperationException($"Unable to create staging table for '{tableMapping.TableName}' because no columns were resolved.");

        string createTableSql = $"CREATE GLOBAL TEMPORARY TABLE {stagingTableName} ({string.Join(", ", columnDefs)}) ON COMMIT PRESERVE ROWS";
        ExecuteNonQuery(context, createTableSql);
        return stagingTableName;
    }

    private static void DropStagingTable(DbContext context, string stagingTableName)
    {
        if (context.Database.CurrentTransaction != null)
            return;

        try
        {
            ExecuteNonQuery(context, $"DROP TABLE {stagingTableName}");
        }
        catch
        {
            // Ignore errors when dropping staging table
        }
    }

    private static int ExecuteDeleteFromStaging(DbContext context, TableMapping tableMapping, string stagingTableName, IEnumerable<string> matchingColumns)
    {
        var tableName = tableMapping.FullQualifedTableName;
        var matchColumns = matchingColumns.ToList();

        // Build JOIN condition: t.col1 = s.col1 AND t.col2 = s.col2 ...
        var joinConditions = matchColumns.Select(col => $"t.\"{col}\" = s.\"{col}\"");
        string joinClause = string.Join(" AND ", joinConditions);

        string deleteSql = $@"DELETE FROM {tableName} t 
WHERE EXISTS (SELECT 1 FROM {stagingTableName} s WHERE {joinClause})";

        return ExecuteNonQuery(context, deleteSql);
    }

    private static int ExecuteMergeForUpdate(DbContext context, TableMapping tableMapping, string stagingTableName, IEnumerable<string> matchingColumns, IEnumerable<string> updatableColumns, string tableName = null)
    {
        tableName ??= tableMapping.FullQualifedTableName;
        var matchColumns = matchingColumns.ToList();
        var updateColumns = updatableColumns.ToList();

        if (matchColumns.Count == 0 || updateColumns.Count == 0)
            return 0;

        // Build JOIN condition
        var joinConditions = matchColumns.Select(col => $"t.\"{col}\" = s.\"{col}\"");
        string joinClause = string.Join(" AND ", joinConditions);

        // Build SET clause
        var setClauses = updateColumns.Select(col => $"t.\"{col}\" = s.\"{col}\"");
        string setClause = string.Join(", ", setClauses);

        string updateSql = $@"MERGE INTO {tableName} t
USING {stagingTableName} s
ON ({joinClause})
WHEN MATCHED THEN UPDATE SET {setClause}";

        return ExecuteNonQuery(context, updateSql);
    }

    private static int ExecuteMergeForUpdateForMappedTables(DbContext context, TableMapping tableMapping, string stagingTableName, IEnumerable<string> matchingColumns, IEnumerable<string> updatableColumns)
    {
        var matchColumns = matchingColumns.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        var updateColumns = updatableColumns.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        var rowsUpdated = 0;

        foreach (var entityType in tableMapping.EntityTypes)
        {
            var tableColumns = tableMapping.GetColumnNames(entityType, true)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var tableMatchColumns = matchColumns.Where(tableColumns.Contains).ToList();
            if (tableMatchColumns.Count != matchColumns.Count)
                continue;

            var tableUpdateColumns = updateColumns
                .Where(tableColumns.Contains)
                .Except(tableMatchColumns, StringComparer.OrdinalIgnoreCase)
                .ToList();
            if (tableUpdateColumns.Count == 0)
                continue;

            var tableName = context.DelimitIdentifier(entityType.GetTableName(), entityType.GetSchema() ?? context.Database.GetDefaultSchema());
            rowsUpdated = Math.Max(rowsUpdated, ExecuteMergeForUpdate(context, tableMapping, stagingTableName, tableMatchColumns, tableUpdateColumns, tableName));
        }

        return rowsUpdated;
    }

    private static (int rowsInserted, int rowsUpdated) ExecuteMergeForMerge(
        DbContext context,
        TableMapping tableMapping,
        string stagingTableName,
        IEnumerable<string> matchingColumns,
        IEnumerable<string> insertColumns,
        IEnumerable<string> updatableColumns,
        string tableName = null)
    {
        tableName ??= tableMapping.FullQualifedTableName;
        var matchColumns = matchingColumns.ToList();
        var insertCols = insertColumns.ToList();
        var updateCols = updatableColumns.ToList();

        if (matchColumns.Count == 0)
            return (0, 0);

        // Build MERGE condition
        var joinConditions = matchColumns.Select(col => $"t.\"{col}\" = s.\"{col}\"");
        string mergeCondition = string.Join(" AND ", joinConditions);

        int rowsUpdated = 0;
        int rowsInserted = 0;

        if (updateCols.Count > 0)
        {
            var setClauses = updateCols.Select(col => $"t.\"{col}\" = s.\"{col}\"");
            string setClause = string.Join(", ", setClauses);
            string updateMergeSql = $@"MERGE INTO {tableName} t
USING {stagingTableName} s
ON ({mergeCondition})
WHEN MATCHED THEN UPDATE SET {setClause}";
            rowsUpdated = ExecuteNonQuery(context, updateMergeSql);
        }

        if (insertCols.Count > 0)
        {
            var insertColNames = insertCols.Select(col => $"\"{col}\"");
            var insertStagingCols = insertCols.Select(col => $"s.\"{col}\"");
            string insertMergeSql = $@"MERGE INTO {tableName} t
USING {stagingTableName} s
ON ({mergeCondition})
WHEN NOT MATCHED THEN INSERT ({string.Join(", ", insertColNames)}) VALUES ({string.Join(", ", insertStagingCols)})";
            rowsInserted = ExecuteNonQuery(context, insertMergeSql);
        }

        return (rowsInserted, rowsUpdated);
    }

    private static (int rowsInserted, int rowsUpdated) ExecuteMergeForMergeForMappedTables(
        DbContext context,
        TableMapping tableMapping,
        string stagingTableName,
        IEnumerable<string> matchingColumns,
        IEnumerable<string> insertColumns,
        IEnumerable<string> updatableColumns)
    {
        var matchColumns = matchingColumns.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        var insertCols = insertColumns.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        var updateCols = updatableColumns.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        var rowsInserted = 0;
        var rowsUpdated = 0;

        foreach (var entityType in tableMapping.EntityTypes)
        {
            var tableColumns = tableMapping.GetColumnNames(entityType, true)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var tableMatchColumns = matchColumns.Where(tableColumns.Contains).ToList();
            if (tableMatchColumns.Count != matchColumns.Count)
                continue;

            var tableInsertColumns = insertCols
                .Where(tableColumns.Contains)
                .ToList();
            var tableUpdateColumns = updateCols
                .Where(tableColumns.Contains)
                .Except(tableMatchColumns, StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (tableInsertColumns.Count == 0 && tableUpdateColumns.Count == 0)
                continue;

            var tableName = context.DelimitIdentifier(entityType.GetTableName(), entityType.GetSchema() ?? context.Database.GetDefaultSchema());
            var (tableRowsInserted, tableRowsUpdated) = ExecuteMergeForMerge(
                context,
                tableMapping,
                stagingTableName,
                tableMatchColumns,
                tableInsertColumns,
                tableUpdateColumns,
                tableName);

            rowsInserted = Math.Max(rowsInserted, tableRowsInserted);
            rowsUpdated = Math.Max(rowsUpdated, tableRowsUpdated);
        }

        return (rowsInserted, rowsUpdated);
    }

    private static int ExecuteDeleteUnmatched(DbContext context, TableMapping tableMapping, string stagingTableName, IEnumerable<string> matchingColumns)
    {
        var tableName = tableMapping.FullQualifedTableName;
        var matchColumns = matchingColumns.ToList();

        if (matchColumns.Count == 0)
            return 0;

        // Delete rows from target that don't exist in staging by the merge condition.
        // Also preserve rows that were just inserted from staging when the merge condition uses nullable non-key columns.
        var joinConditions = matchColumns.Select(col => $"t.\"{col}\" = s.\"{col}\"");
        var primaryKeyColumns = tableMapping.GetPrimaryKeyColumns().Except(matchColumns, StringComparer.OrdinalIgnoreCase).ToList();
        var primaryKeyConditions = primaryKeyColumns.Select(col => $"t.\"{col}\" = s.\"{col}\"").ToList();
        var existsConditions = new List<string> { string.Join(" AND ", joinConditions) };
        if (primaryKeyConditions.Count > 0)
            existsConditions.Add(string.Join(" AND ", primaryKeyConditions));
        string joinClause = string.Join(" OR ", existsConditions.Select(condition => $"({condition})"));

        string deleteSql = $@"DELETE FROM {tableName} t 
WHERE NOT EXISTS (SELECT 1 FROM {stagingTableName} s WHERE {joinClause})";

        return ExecuteNonQuery(context, deleteSql);
    }

    private static string GetOracleColumnType(IProperty property)
    {
        var storeType = property.GetRelationalTypeMapping()?.StoreType;
        if (!string.IsNullOrWhiteSpace(storeType))
            return SanitizeOracleColumnType(storeType);

        return GetOracleDataType(property.ClrType);
    }

    private static string SanitizeOracleColumnType(string storeType)
    {
        int cutIndex = storeType.Length;
        foreach (var keyword in new[] { " GENERATED", " DEFAULT", " IDENTITY", " VISIBLE", " NOT NULL" })
        {
            int index = storeType.IndexOf(keyword, StringComparison.OrdinalIgnoreCase);
            if (index >= 0 && index < cutIndex)
                cutIndex = index;
        }

        var sanitized = storeType[..cutIndex].TrimEnd();
        if (string.Equals(sanitized, "BOOLEAN", StringComparison.OrdinalIgnoreCase))
            return "NUMBER(1)";

        if (sanitized.StartsWith("DECIMAL", StringComparison.OrdinalIgnoreCase))
            return "NUMBER" + sanitized["DECIMAL".Length..];

        return sanitized;
    }

    private static string GetOracleDataType(Type clrType)
    {
        var underlyingType = Nullable.GetUnderlyingType(clrType) ?? clrType;

        return underlyingType switch
        {
            _ when underlyingType == typeof(int) => "NUMBER(10)",
            _ when underlyingType == typeof(long) => "NUMBER(19)",
            _ when underlyingType == typeof(short) => "NUMBER(5)",
            _ when underlyingType == typeof(byte) => "NUMBER(3)",
            _ when underlyingType == typeof(decimal) => "NUMBER(18,2)",
            _ when underlyingType == typeof(double) => "BINARY_DOUBLE",
            _ when underlyingType == typeof(float) => "BINARY_FLOAT",
            _ when underlyingType == typeof(bool) => "NUMBER(1)",
            _ when underlyingType == typeof(string) => "NVARCHAR2(4000)",
            _ when underlyingType == typeof(DateTime) => "TIMESTAMP",
            _ when underlyingType == typeof(Guid) => "RAW(16)",
            _ when underlyingType.IsEnum => "NUMBER(10)",
            _ => "NVARCHAR2(4000)"
        };
    }

    internal static int BulkDelete<T>(DbContext context, IEnumerable<T> entities, BulkDeleteOptions<T> options)
    {
        var items = entities?.ToList() ?? [];
        if (items.Count == 0)
            return 0;

        var tableMapping = context.GetTableMapping(typeof(T), options?.EntityType);
        var matchingColumns = GetMatchingColumns<T>(options?.DeleteOnCondition, tableMapping.GetPrimaryKeyColumns().ToArray());
        var columnsToLoad = matchingColumns.ToList();

        if (columnsToLoad.Count == 0)
            return 0;

        // Create staging table and execute delete via native SQL
        string stagingTableName = CreateStagingTable(context, tableMapping);
        try
        {
            // Populate staging table with matching column data
            ExecuteOracleBulkCopy(context, items, tableMapping, columnsToLoad, options?.CommandTimeout, stagingTableName);

            // Execute DELETE with JOIN to staging table
            int rowsDeleted = ExecuteDeleteFromStaging(context, tableMapping, stagingTableName, matchingColumns);
            return rowsDeleted;
        }
        finally
        {
            DropStagingTable(context, stagingTableName);
        }
    }

    internal static int BulkUpdate<T>(DbContext context, IEnumerable<T> entities, BulkUpdateOptions<T> options)
    {
        var items = entities?.ToList() ?? [];
        if (items.Count == 0)
            return 0;

        var tableMapping = context.GetTableMapping(typeof(T), options?.EntityType);
        var updatableColumns = GetUpdatableColumns(tableMapping, options?.InputColumns, options?.IgnoreColumns);
        var matchingColumns = GetMatchingColumns<T>(options?.UpdateOnCondition, tableMapping.GetPrimaryKeyColumns().ToArray());

        var columnsToLoad = matchingColumns.Concat(updatableColumns).Distinct().ToList();
        if (columnsToLoad.Count == 0)
            return 0;

        // Create staging table and execute update via native SQL
        string stagingTableName = CreateStagingTable(context, tableMapping);
        try
        {
            // Populate staging table with all needed columns
            ExecuteOracleBulkCopy(context, items, tableMapping, columnsToLoad, options?.CommandTimeout, stagingTableName);

            // Execute MERGE with UPDATE clause
            int rowsUpdated = ExecuteMergeForUpdateForMappedTables(context, tableMapping, stagingTableName, matchingColumns, updatableColumns);
            return rowsUpdated;
        }
        finally
        {
            DropStagingTable(context, stagingTableName);
        }
    }

    internal static BulkMergeResult<T> BulkMerge<T>(DbContext context, IEnumerable<T> entities, BulkMergeOptions<T> options)
    {
        var items = entities?.ToList() ?? [];
        if (items.Count == 0)
        {
            return new BulkMergeResult<T>
            {
                RowsAffected = 0,
                RowsInserted = 0,
                RowsUpdated = 0,
                RowsDeleted = 0,
                Output = []
            };
        }

        var tableMapping = context.GetTableMapping(typeof(T), options?.EntityType);
        var mergeColumns = GetMatchingColumns<T>(options?.MergeOnCondition, tableMapping.GetPrimaryKeyColumns().ToArray());
        var insertColumns = GetInsertColumns(tableMapping, null, options?.IgnoreColumnsOnInsert, false);
        var updatableColumns = GetUpdatableColumns(tableMapping, null, options?.IgnoreColumnsOnUpdate);

        var columnsToLoad = mergeColumns.Concat(insertColumns).Concat(updatableColumns).Distinct().ToList();
        if (columnsToLoad.Count == 0)
        {
            return new BulkMergeResult<T>
            {
                RowsAffected = 0,
                RowsInserted = 0,
                RowsUpdated = 0,
                RowsDeleted = 0,
                Output = []
            };
        }

        // Create staging table and execute merge via native SQL
        string stagingTableName = CreateStagingTable(context, tableMapping);
        try
        {
            // Apply column rules and defaults before inserting into staging
            foreach (var item in items)
            {
                ApplyInsertColumnRules(item!, tableMapping, insertColumns, false);
            }
            ApplyInsertDefaults(context, tableMapping, items, false);

            // Populate staging table with all needed columns
            ExecuteOracleBulkCopy(context, items, tableMapping, columnsToLoad, options?.CommandTimeout, stagingTableName);

            // Execute full MERGE (insert + update)
            (int rowsInserted, int rowsUpdated) = ExecuteMergeForMergeForMappedTables(context, tableMapping, stagingTableName, mergeColumns, insertColumns, updatableColumns);
            return new BulkMergeResult<T>
            {
                RowsAffected = rowsInserted + rowsUpdated,
                RowsInserted = rowsInserted,
                RowsUpdated = rowsUpdated,
                RowsDeleted = 0,
                Output = []
            };
        }
        finally
        {
            DropStagingTable(context, stagingTableName);
        }
    }

    internal static BulkSyncResult<T> BulkSync<T>(DbContext context, IEnumerable<T> entities, BulkSyncOptions<T> options)
    {
        var items = entities?.ToList() ?? [];
        if (items.Count == 0)
        {
            return new BulkSyncResult<T>
            {
                RowsAffected = 0,
                RowsInserted = 0,
                RowsUpdated = 0,
                RowsDeleted = 0
            };
        }

        var tableMapping = context.GetTableMapping(typeof(T), options?.EntityType);
        var mergeColumns = GetMatchingColumns<T>(options?.MergeOnCondition, tableMapping.GetPrimaryKeyColumns().ToArray());
        var insertColumns = GetInsertColumns(tableMapping, null, options?.IgnoreColumnsOnInsert, false);
        var updatableColumns = GetUpdatableColumns(tableMapping, null, options?.IgnoreColumnsOnUpdate);

        var columnsToLoad = mergeColumns.Concat(insertColumns).Concat(updatableColumns).Distinct().ToList();
        if (columnsToLoad.Count == 0)
        {
            return new BulkSyncResult<T>
            {
                RowsAffected = 0,
                RowsInserted = 0,
                RowsUpdated = 0,
                RowsDeleted = 0
            };
        }

        // Create staging table and execute sync via native SQL
        string stagingTableName = CreateStagingTable(context, tableMapping);
        try
        {
            // Apply column rules and defaults before inserting into staging
            foreach (var item in items)
            {
                ApplyInsertColumnRules(item!, tableMapping, insertColumns, false);
            }
            ApplyInsertDefaults(context, tableMapping, items, false);

            // Populate staging table with all needed columns
            ExecuteOracleBulkCopy(context, items, tableMapping, columnsToLoad, options?.CommandTimeout, stagingTableName);

            // Execute merge (insert + update)
            (int rowsInserted, int rowsUpdated) = ExecuteMergeForMergeForMappedTables(context, tableMapping, stagingTableName, mergeColumns, insertColumns, updatableColumns);

            // Delete rows not in staging table (marked as deleted)
            int rowsDeleted = ExecuteDeleteUnmatched(context, tableMapping, stagingTableName, mergeColumns);

            return new BulkSyncResult<T>
            {
                RowsAffected = rowsInserted + rowsUpdated + rowsDeleted,
                RowsInserted = rowsInserted,
                RowsUpdated = rowsUpdated,
                RowsDeleted = rowsDeleted
            };
        }
        finally
        {
            DropStagingTable(context, stagingTableName);
        }
    }

    internal static IEnumerable<T> BulkFetch<T, U>(DbContext context, DbSet<T> dbSet, IEnumerable<U> entities, BulkFetchOptions<T> options) where T : class, new()
    {
        var sourceItems = entities?.ToList() ?? [];
        if (sourceItems.Count == 0)
            return [];

        var tableMapping = context.GetTableMapping(typeof(T));
        var existingRows = dbSet.AsNoTracking().ToList();
        var joinColumns = GetMatchingColumns<T>(options?.JoinOnCondition, tableMapping.GetPrimaryKeyColumns().ToArray());
        var ignoredColumns = options?.IgnoreColumns?.GetObjectProperties().ToHashSet(StringComparer.OrdinalIgnoreCase) ?? [];
        var hasColumnFilters = options?.InputColumns != null || options?.IgnoreColumns != null;
        var selectedColumns = hasColumnFilters
            ? GetUpdatableColumns(tableMapping, options?.InputColumns, options?.IgnoreColumns)
            : [];

        var results = new List<T>();
        foreach (var source in sourceItems)
        {
            var match = FindMatch(existingRows, source, joinColumns);
            if (match == null)
                continue;

            if (!hasColumnFilters && source is T sourceEntity)
            {
                results.Add(sourceEntity);
                continue;
            }

            if (hasColumnFilters)
                ClearExcludedColumns(tableMapping, match, selectedColumns, ignoredColumns);
            results.Add(match);
        }

        return results;
    }

    private static void ApplyInsertDefaults<T>(DbContext context, TableMapping tableMapping, IReadOnlyCollection<T> items, bool keepIdentity)
    {
        if (items.Count == 0)
            return;

        var keyProperty = tableMapping.EntityType.FindPrimaryKey()?.Properties.FirstOrDefault();
        long generatedKey = 1;
        HashSet<long> usedKeys = null;
        if (!keepIdentity && keyProperty != null && IsNumericKey(keyProperty.ClrType))
        {
            usedKeys = [];
            foreach (var item in items)
            {
                var current = GetPropertyValue(item!, keyProperty.Name);
                if (!IsDefaultValue(current, keyProperty.ClrType))
                    usedKeys.Add(Convert.ToInt64(current));
            }

            generatedKey = GetNextNumericKey(context, tableMapping, keyProperty);
            while (usedKeys.Contains(generatedKey))
                generatedKey++;
        }

        foreach (var item in items)
        {
            foreach (var complexProperty in tableMapping.EntityType.GetComplexProperties())
            {
                if (complexProperty.IsNullable)
                    continue;

                if (GetPropertyValue(item!, complexProperty.Name) == null)
                    SetPropertyValue(item!, complexProperty.Name, Activator.CreateInstance(complexProperty.ClrType));
            }

            if (keyProperty != null && !keepIdentity && IsNumericKey(keyProperty.ClrType))
            {
                var current = GetPropertyValue(item!, keyProperty.Name);
                if (IsDefaultValue(current, keyProperty.ClrType) || Convert.ToDecimal(current) <= 0)
                {
                    while (usedKeys.Contains(generatedKey))
                        generatedKey++;

                    SetPropertyValue(item!, keyProperty.Name, ConvertKey(generatedKey, keyProperty.ClrType));
                    usedKeys.Add(generatedKey);
                    generatedKey++;
                }
            }

            foreach (var property in tableMapping.GetEntityProperties(valueGenerated: ValueGenerated.OnAdd).Concat(tableMapping.GetEntityProperties(valueGenerated: ValueGenerated.OnAddOrUpdate)))
            {
                if (property.Name == keyProperty?.Name)
                    continue;

                if (property.ClrType == typeof(DateTime) || property.ClrType == typeof(DateTime?))
                {
                    SetPropertyValue(item!, property.Name, DateTime.Now);
                }
                else if (property.ClrType == typeof(bool) || property.ClrType == typeof(bool?))
                {
                    if (IsDefaultValue(GetPropertyValue(item!, property.Name), property.ClrType))
                        SetPropertyValue(item!, property.Name, true);
                }
            }
        }
    }

    private static IEnumerable<string> GetMatchingColumns<T>(Expression<Func<T, T, bool>> condition, params string[] fallbackColumns)
    {
        var columns = condition != null ? CommonUtil<T>.GetColumns(condition, ["s"]) : [];
        if (columns.Length > 0)
            return columns;
        return fallbackColumns ?? [];
    }

    private static IEnumerable<string> GetUpdatableColumns<T>(TableMapping tableMapping, Expression<Func<T, object>> inputColumns, Expression<Func<T, object>> ignoreColumns)
    {
        var includedColumns = tableMapping.GetColumns(false).ToHashSet(StringComparer.OrdinalIgnoreCase);
        if (inputColumns != null)
        {
            includedColumns.IntersectWith(inputColumns.GetObjectProperties());
        }

        if (ignoreColumns != null)
        {
            foreach (var ignored in ignoreColumns.GetObjectProperties())
                includedColumns.Remove(ignored);
        }

        return includedColumns;
    }

    private static IEnumerable<string> GetInsertColumns<T>(TableMapping tableMapping, Expression<Func<T, object>> inputColumns, Expression<Func<T, object>> ignoreColumns, bool keepIdentity)
    {
        var includedColumns = tableMapping.GetColumns(true).ToHashSet(StringComparer.OrdinalIgnoreCase);
        if (inputColumns != null)
        {
            includedColumns.IntersectWith(inputColumns.GetObjectProperties());
        }

        if (ignoreColumns != null)
        {
            includedColumns.ExceptWith(ignoreColumns.GetObjectProperties());
        }

        var primaryKey = tableMapping.EntityType.FindPrimaryKey();
        if (keepIdentity)
        {
            foreach (var key in primaryKey?.Properties ?? [])
                includedColumns.Add(tableMapping.GetColumnName(key));
        }
        else
        {
            foreach (var key in primaryKey?.Properties.Where(o => IsNumericKey(o.ClrType)) ?? [])
                includedColumns.Add(tableMapping.GetColumnName(key));
        }

        return includedColumns;
    }

    private static T FindMatch<T, U>(IEnumerable<T> candidates, U source, IEnumerable<string> matchingColumns)
    {
        foreach (var candidate in candidates)
        {
            if (MatchesCondition(source, candidate, matchingColumns))
                return candidate;
        }

        return default;
    }

    private static bool MatchesCondition<U, T>(U source, T target, IEnumerable<string> columns)
    {
        if (columns == null)
            return false;

        foreach (var column in columns)
        {
            var sourceValue = GetPropertyValue(source, column);
            var targetValue = GetPropertyValue(target, column);
            if (sourceValue == null || targetValue == null || !Equals(sourceValue, targetValue))
                return false;
        }

        return columns.Any();
    }

    private static void CopyKeyValues<T>(T source, T target, TableMapping tableMapping)
    {
        if (source == null || target == null)
            return;

        var keyNames = tableMapping.GetPrimaryKeyColumns().ToHashSet(StringComparer.OrdinalIgnoreCase);
        foreach (var property in source.GetType().GetProperties().Where(o => o.CanRead && o.CanWrite))
        {
            if (!keyNames.Contains(property.Name))
                continue;

            var targetProperty = target.GetType().GetProperty(property.Name);
            if (targetProperty?.CanWrite == true)
                targetProperty.SetValue(target, property.GetValue(source));
        }
    }

    private static void ClearExcludedColumns<T>(TableMapping tableMapping, T entity, IEnumerable<string> includedColumns, IEnumerable<string> ignoredColumns)
    {
        var included = includedColumns?.ToHashSet(StringComparer.OrdinalIgnoreCase) ?? [];
        var ignored = ignoredColumns?.ToHashSet(StringComparer.OrdinalIgnoreCase) ?? [];

        foreach (var property in tableMapping.Properties)
        {
            if (tableMapping.GetPrimaryKeyColumns().Contains(property.Name))
                continue;

            if (ignored.Contains(property.Name) || !included.Contains(property.Name))
            {
                SetPropertyValue(entity!, property.Name, GetDefaultValue(property.ClrType));
            }
        }
    }

    private static void ApplyInsertColumnRules<T>(T entity, TableMapping tableMapping, IEnumerable<string> allowedColumns, bool keepIdentity)
    {
        var allowed = allowedColumns?.ToHashSet(StringComparer.OrdinalIgnoreCase) ?? [];
        var keyNames = tableMapping.GetPrimaryKeyColumns().ToHashSet(StringComparer.OrdinalIgnoreCase);

        foreach (var property in tableMapping.Properties)
        {
            if (keyNames.Contains(property.Name) && !keepIdentity)
                continue;

            if (allowed.Count > 0 && !allowed.Contains(property.Name))
                SetPropertyValue(entity!, property.Name, GetDefaultValue(property.ClrType));
        }
    }

    private static void ApplyUpdateColumnRules(EntityEntry entry, IEnumerable<string> updatableColumns)
    {
        var allowed = updatableColumns?.ToHashSet(StringComparer.OrdinalIgnoreCase) ?? [];
        foreach (var property in entry.Properties)
        {
            if (property.Metadata.IsPrimaryKey())
                continue;

            if (allowed.Count > 0 && !allowed.Contains(property.Metadata.Name))
            {
                property.CurrentValue = property.OriginalValue;
                property.IsModified = false;
            }
        }
    }

    private static object GetPropertyValue(object entity, string propertyName)
    {
        if (entity == null)
            return null;

        if (entity is InternalEntityEntry internalEntry)
            return internalEntry.ToEntityEntry().CurrentValues[propertyName];

        if (entity is EntityEntry entry)
            return entry.CurrentValues[propertyName];

        var property = entity.GetType().GetProperty(propertyName);
        return property?.GetValue(entity);
    }

    private static object GetPropertyValue(object entity, IProperty property)
    {
        if (entity == null || property == null)
            return null;

        object value;
        if (property.DeclaringType is IComplexType complexType)
        {
            var complexObject = ReadMember(entity, complexType.ComplexProperty.Name);
            value = complexObject == null ? null : ReadMember(complexObject, property.Name);
        }
        else
        {
            value = ReadMember(entity, property.Name);
        }

        var converter = property.GetTypeMapping()?.Converter;
        if (converter != null && value != null)
            value = converter.ConvertToProvider(value);

        return value;
    }

    private static object ReadMember(object target, string memberName)
    {
        if (target == null)
            return null;

        if (target is InternalEntityEntry internalEntry)
            return internalEntry.ToEntityEntry().CurrentValues[memberName];

        if (target is EntityEntry entry)
            return entry.CurrentValues[memberName];

        var type = target.GetType();
        const BindingFlags flags = BindingFlags.Public | BindingFlags.Instance;
        var property = type.GetProperty(memberName, flags);
        if (property != null)
            return property.GetValue(target);

        var field = type.GetField(memberName, flags);
        return field?.GetValue(target);
    }

    private static void SetPropertyValue(object entity, string propertyName, object value)
    {
        if (entity is InternalEntityEntry internalEntry)
        {
            internalEntry.ToEntityEntry().CurrentValues[propertyName] = value;
            return;
        }

        if (entity is EntityEntry entry)
        {
            entry.CurrentValues[propertyName] = value;
            return;
        }

        var property = entity?.GetType().GetProperty(propertyName);
        if (property?.CanWrite == true)
            property.SetValue(entity, value);
    }

    private static bool IsDefaultValue(object value, Type type)
    {
        if (value == null)
            return true;

        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;
        return Equals(value, underlyingType.IsValueType ? Activator.CreateInstance(underlyingType) : null);
    }

    private static bool IsNumericKey(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        return type == typeof(short) || type == typeof(int) || type == typeof(long) || type == typeof(byte)
            || type == typeof(decimal) || type == typeof(double) || type == typeof(float);
    }

    private static long GetNextNumericKey<T>(DbContext context, IProperty keyProperty)
    {
        var currentValues = GetSet<T>(context).Cast<T>().ToList()
            .Select(entity => GetPropertyValue(entity!, keyProperty.Name))
            .Where(value => value != null)
            .Select(value => Convert.ToInt64(value))
            .ToList();
        return currentValues.Count == 0 ? 1 : currentValues.Max() + 1;
    }

    private static long GetNextNumericKey(DbContext context, TableMapping tableMapping, IProperty keyProperty)
    {
        var columnName = tableMapping.GetColumnName(keyProperty);
        var sql = $"SELECT MAX(\"{columnName}\") FROM {tableMapping.FullQualifedTableName}";
        var connection = context.Database.GetDbConnection();
        if (connection.State != ConnectionState.Open)
            connection.Open();

        using var command = connection.CreateCommand();
        ConfigureCommand(context, command, sql, null);
        var value = command.ExecuteScalar();
        return value == null || value == DBNull.Value ? 1 : Convert.ToInt64(value) + 1;
    }

    private static IQueryable GetSet<T>(DbContext context)
    {
        var method = typeof(DbContext).GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Single(o => o.Name == nameof(DbContext.Set) && o.IsGenericMethodDefinition && o.GetParameters().Length == 0);
        return (IQueryable)method.MakeGenericMethod(typeof(T)).Invoke(context, null);
    }

    private static object ConvertKey(long value, Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        return Convert.ChangeType(value, type);
    }

    private static object GetDefaultValue(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        return type.IsValueType ? Activator.CreateInstance(type) : null;
    }

}
