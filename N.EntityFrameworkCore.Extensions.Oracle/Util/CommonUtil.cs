using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace N.EntityFrameworkCore.Extensions.Util;

internal static class CommonUtil
{
    internal static IEnumerable<string> FormatColumns(DbContext dbContext, IEnumerable<string> columns)
    {
        return columns.Select(s => FormatColumn(dbContext, s));
    }
    internal static IEnumerable<string> FormatColumns(IEnumerable<string> columns)
    {
        return columns.Select(FormatColumnLegacy);
    }
    internal static IEnumerable<string> FormatColumns(DbContext dbContext, string tableAlias, IEnumerable<string> columns)
    {
        return columns.Select(s => dbContext.DelimitMemberAccess(tableAlias, RemoveQualifier(s)));
    }
    internal static IEnumerable<string> FilterColumns<T>(IEnumerable<string> columnNames, string[] primaryKeyColumnNames, Expression<Func<T, object>> inputColumns, Expression<Func<T, object>> ignoreColumns)
    {
        var filteredColumnNames = columnNames;
        if (inputColumns != null)
        {
            var inputColumnNames = GetInputColumnNames(inputColumns);
            filteredColumnNames = filteredColumnNames.Intersect(inputColumnNames.Union(primaryKeyColumnNames), StringComparer.OrdinalIgnoreCase);
        }
        if (ignoreColumns != null)
        {
            var ignoreColumnNames = GetInputColumnNames(ignoreColumns);
            if (ignoreColumnNames.Intersect(primaryKeyColumnNames, StringComparer.OrdinalIgnoreCase).Any())
            {
                throw new InvalidDataException("Primary key columns cannot be ignored in BulkInsertOptions.IgnoreColumns");
            }
            filteredColumnNames = filteredColumnNames.Except(ignoreColumnNames, StringComparer.OrdinalIgnoreCase);
        }
        return filteredColumnNames;
    }

    private static IEnumerable<string> GetInputColumnNames<T>(Expression<Func<T, object>> expression)
    {
        var names = expression.GetObjectProperties();
        if (names.Count > 0)
            return names;

        Expression body = expression.Body;
        while (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked or ExpressionType.Quote } unary)
            body = unary.Operand;

        if (body is NewExpression or MemberInitExpression)
            return body.Type.GetProperties().Select(p => p.Name);

        return names;
    }
    internal static string FormatTableName(DatabaseFacade database, string tableName)
    {
        return database.DelimitTableName(tableName);
    }
    internal static string FormatTableName(string tableName)
    {
        var parts = tableName.Split('.');
        var sb = new StringBuilder();
        for (int i = 0; i < parts.Length; i++)
        {
            if (i > 0)
                sb.Append('.');
            sb.Append('"').Append(RemoveQualifier(parts[i])).Append('"');
        }
        return sb.ToString();
    }
    private static string FormatColumn(DbContext dbContext, string column)
    {
        var parts = column.Split('.');
        var sb = new StringBuilder();
        for (int i = 0; i < parts.Length; i++)
        {
            if (i > 0)
                sb.Append('.');
            var part = parts[i];
            sb.Append(part.Length > 0 && part[0] == '$' ? part : dbContext.DelimitIdentifier(RemoveQualifier(part)));
        }
        return sb.ToString();
    }
    private static string FormatColumnLegacy(string column)
    {
        var parts = column.Split('.');
        var sb = new StringBuilder();
        for (int i = 0; i < parts.Length; i++)
        {
            if (i > 0)
                sb.Append('.');
            var part = parts[i];
            if (part.Length > 0 && part[0] == '$' || part.Length >= 2 && part[0] == '[' && part[^1] == ']')
                sb.Append(part);
            else
                sb.Append('[').Append(part).Append(']');
        }
        return sb.ToString();
    }
    private static string RemoveQualifier(string name)
    {
        return RemoveQualifier(name.AsSpan()).ToString();
    }
    private static ReadOnlySpan<char> RemoveQualifier(ReadOnlySpan<char> name)
    {
        name = name.TrimStart('[').TrimEnd(']');
        return name.Trim('"');
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
                var tablePart = columnSpan[1..dotIndex];
                var columnPart = columnSpan[(dotIndex + 1)..];
                if (tableNames == null || tableNames.Contains(tablePart.ToString()))
                {
                    foundColumns.Add(columnPart.ToString());
                }
            }

            offset = startIndex + 1;
        }

        return foundColumns.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
    }
    internal static string GetJoinConditionSql(Expression<Func<T, T, bool>> joinKeyExpression, string[] storeGeneratedColumnNames, string sourceTableName = "s", string targetTableName = "t")
    {
        if (joinKeyExpression != null)
            return joinKeyExpression.ToSqlPredicate(sourceTableName, targetTableName);

        return BuildJoinConditionSql(storeGeneratedColumnNames, sourceTableName, targetTableName);
    }
    internal static string GetJoinConditionSql(DbContext dbContext, Expression<Func<T, T, bool>> joinKeyExpression, string[] storeGeneratedColumnNames, string sourceTableName = "s", string targetTableName = "t")
    {
        if (joinKeyExpression != null)
            return joinKeyExpression.ToSqlPredicate(dbContext, sourceTableName, targetTableName);

        var sb = new StringBuilder();
        for (int i = 0; i < storeGeneratedColumnNames.Length; i++)
        {
            if (i > 0)
                sb.Append(" AND ");
            sb.Append(sourceTableName).Append('.').Append(dbContext.DelimitIdentifier(storeGeneratedColumnNames[i]))
                .Append('=')
                .Append(targetTableName).Append('.').Append(dbContext.DelimitIdentifier(storeGeneratedColumnNames[i]));
        }
        return sb.ToString();
    }
    private static string BuildJoinConditionSql(string[] storeGeneratedColumnNames, string sourceTableName, string targetTableName)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < storeGeneratedColumnNames.Length; i++)
        {
            if (i > 0)
                sb.Append(" AND ");
            sb.Append(sourceTableName).Append(".\"").Append(storeGeneratedColumnNames[i]).Append("\"=")
                .Append(targetTableName).Append(".\"").Append(storeGeneratedColumnNames[i]).Append('"');
        }
        return sb.ToString();
    }
}