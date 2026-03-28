using System.Collections.Generic;
using System.Linq;
using System.Text;
using N.EntityFrameworkCore.Extensions.Extensions;

namespace N.EntityFrameworkCore.Extensions.Sql;

internal class SqlStatement
{
    internal string Sql => ToSql();
    List<SqlPart> SqlParts { get; }
    SqlStatement()
    {
        SqlParts = new List<SqlPart>();
    }
    internal void CreatePart(SqlKeyword keyword, SqlExpression expression = null) =>
        SqlParts.Add(new SqlPart(keyword, expression));
    internal void SetIdentityInsert(string tableName, bool enable)
    {
        CreatePart(SqlKeyword.Set);
        CreatePart(SqlKeyword.Identity_Insert, SqlExpression.Table(tableName));
        if (enable)
            CreatePart(SqlKeyword.On);
        else
            CreatePart(SqlKeyword.Off);
        CreatePart(SqlKeyword.Semicolon);
    }
    internal static SqlStatement CreateMerge(string sourceTableName, string targetTableName, string joinOnCondition,
        IEnumerable<string> insertColumns, IEnumerable<string> updateColumns, IEnumerable<string> outputColumns,
        bool deleteIfNotMatched = false, bool hasIdentityColumn = false)
    {
        var statement = new SqlStatement();
        if (hasIdentityColumn)
            statement.SetIdentityInsert(targetTableName, true);
        statement.CreatePart(SqlKeyword.Merge, SqlExpression.Table(targetTableName, "t"));
        statement.CreatePart(SqlKeyword.Using, SqlExpression.Table(sourceTableName, "s"));
        statement.CreatePart(SqlKeyword.On, SqlExpression.String(joinOnCondition));
        statement.CreatePart(SqlKeyword.When);
        statement.CreatePart(SqlKeyword.Not);
        statement.CreatePart(SqlKeyword.Matched);
        statement.CreatePart(SqlKeyword.Then);
        statement.WriteInsert(insertColumns);
        if (updateColumns.Any())
        {
            var updateSetColumns = updateColumns.Select(c => $"t.[{c}]=s.[{c}]");
            statement.CreatePart(SqlKeyword.When);
            statement.CreatePart(SqlKeyword.Matched);
            statement.CreatePart(SqlKeyword.Then);
            statement.CreatePart(SqlKeyword.Update);
            statement.CreatePart(SqlKeyword.Set, SqlExpression.Set(updateSetColumns));
        }
        if (deleteIfNotMatched)
        {
            statement.CreatePart(SqlKeyword.When);
            statement.CreatePart(SqlKeyword.Not);
            statement.CreatePart(SqlKeyword.Matched);
            statement.CreatePart(SqlKeyword.By);
            statement.CreatePart(SqlKeyword.Source);
            statement.CreatePart(SqlKeyword.Then);
            statement.CreatePart(SqlKeyword.Delete);
        }
        if (outputColumns.Any())
            statement.CreatePart(SqlKeyword.Output, SqlExpression.Columns(outputColumns));
        statement.CreatePart(SqlKeyword.Semicolon);

        if (hasIdentityColumn)
            statement.SetIdentityInsert(targetTableName, false);
        return statement;
    }

    private string ToSql()
    {
        StringBuilder sbSql = new StringBuilder();
        foreach (var part in SqlParts)
        {
            if (part.Keyword == SqlKeyword.Semicolon)
            {
                int lastIndex = sbSql.Length - 1;
                if (lastIndex > -1 && sbSql[lastIndex] == ' ')
                {
                    sbSql[lastIndex] = ';';
                    sbSql.Append("\n");
                }
                else
                {
                    sbSql.Append(";\n");
                }
            }
            else if (!part.IgnoreOutput)
            {
                sbSql.Append(part.Keyword.ToString().ToUpper());
                sbSql.Append(" ");
                bool useParenthese = part.Keyword == SqlKeyword.Insert || part.Keyword == SqlKeyword.Values;

                if (part.Expression != null)
                {
                    string expressionSql = useParenthese ? $"({part.Expression.Sql})" : part.Expression.Sql;
                    sbSql.Append(expressionSql);
                    sbSql.Append(" ");
                }
            }
        }
        return sbSql.ToString();
    }
}