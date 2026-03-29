using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.Data.SqlClient;

namespace N.EntityFrameworkCore.Extensions.Util;

internal static class CommonUtil
{
    internal static string GetStagingTableName(TableMapping tableMapping, bool usePermanentTable, SqlConnection sqlConnection)
    {
        if (usePermanentTable)
            return $"[{tableMapping.Schema}].[tmp_be_xx_{tableMapping.TableName}_{sqlConnection.ClientConnectionId}]";
        return $"[{tableMapping.Schema}].[#tmp_be_xx_{tableMapping.TableName}]";
    }
    internal static IEnumerable<string> FormatColumns(IEnumerable<string> columns)
    {
        return columns.Select(s => FormatColumn(s));
    }
    internal static IEnumerable<string> FormatColumns(string tableAlias, IEnumerable<string> columns)
    {
        return columns.Select(s => s.StartsWith('[') && s.EndsWith(']') ? $"[{tableAlias}].{s}" : $"[{tableAlias}].[{s}]");
    }
    internal static IEnumerable<string> FilterColumns<T>(IEnumerable<string> columnNames, string[] primaryKeyColumnNames, Expression<Func<T, object>> inputColumns, Expression<Func<T, object>> ignoreColumns)
    {
        var filteredColumnNames = columnNames;
        if (inputColumns != null)
        {
            var inputColumnNames = inputColumns.GetObjectProperties();
            filteredColumnNames = filteredColumnNames.Intersect(inputColumnNames.Union(primaryKeyColumnNames));
        }
        if (ignoreColumns != null)
        {
            var ignoreColumnNames = ignoreColumns.GetObjectProperties();
            if (ignoreColumnNames.Intersect(primaryKeyColumnNames).Any())
            {
                throw new InvalidDataException("Primary key columns can not be ignored in BulkInsertOptions.IgnoreColumns");
            }
            else
            {
                filteredColumnNames = filteredColumnNames.Except(ignoreColumnNames);
            }
        }
        return filteredColumnNames;
    }
    internal static string FormatTableName(string tableName)
    {
        return string.Join(".", tableName.Split('.').Select(s => $"[{RemoveQualifier(s)}]"));
    }
    private static string FormatColumn(string column)
    {
        var parts = column.Split('.');
        return string.Join(".", parts.Select(p => p.StartsWith('$') || (p.StartsWith('[') && p.EndsWith(']')) ? p : $"[{p}]"));
    }
    private static string RemoveQualifier(string name)
    {
        return name.TrimStart('[').TrimEnd(']');
    }
}
internal static class CommonUtil<T>
{
    internal static string[] GetColumns(Expression<Func<T, T, bool>> expression, string[] tableNames)
    {
        List<string> foundColumns = [];
        string sqlText = (string)expression.Body.GetPrivateFieldValue("DebugView");
        var sqlSpan = sqlText.AsSpan();

        int offset = 0;
        while (offset < sqlSpan.Length)
        {
            int startIndex = sqlSpan[offset..].IndexOf('$');
            if (startIndex == -1) break;
            startIndex += offset;

            var remaining = sqlSpan[startIndex..];
            int spaceIndex = remaining.IndexOf(' ');
            var columnSpan = spaceIndex == -1 ? remaining : remaining[..spaceIndex];

            int dotIndex = columnSpan.IndexOf('.');
            if (dotIndex >= 0)
            {
                var tablePart = columnSpan[1..dotIndex]; // skip leading '$'
                var columnPart = columnSpan[(dotIndex + 1)..];
                if (tableNames == null || tableNames.Contains(tablePart.ToString()))
                {
                    foundColumns.Add(columnPart.ToString());
                }
            }

            offset = startIndex + 1;
        }

        return foundColumns.ToArray();
    }
    internal static string GetJoinConditionSql(Expression<Func<T, T, bool>> joinKeyExpression, string[] storeGeneratedColumnNames, string sourceTableName = "s", string targetTableName = "t")
    {
        if (joinKeyExpression != null)
            return joinKeyExpression.ToSqlPredicate(sourceTableName, targetTableName);

        return string.Join(" AND ", storeGeneratedColumnNames.Select(c => $"{sourceTableName}.[{c}]={targetTableName}.[{c}]"));
    }
}
