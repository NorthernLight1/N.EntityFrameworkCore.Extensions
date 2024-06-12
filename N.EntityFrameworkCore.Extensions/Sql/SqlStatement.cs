using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using N.EntityFrameworkCore.Extensions.Extensions;
using N.EntityFrameworkCore.Extensions.Util;

namespace N.EntityFrameworkCore.Extensions.Sql;

internal class SqlStatement
{
    internal string Sql => ToSql();
    List<SqlPart> SqlParts { get; }
    SqlStatement()
    {
            SqlParts = new List<SqlPart>();
        }
    //static SqlStatement CreateSelectInto(TableMapping souceTableMapping, string targetTable, string[] columns)
    //{
    //    foreach(var property in souceTableMapping.Properties)
    //    {
    //        var t = property;
    //    }
    //    var statement = new SqlStatement();
    //    statement.CreatePart(SqlKeyword.Select, SqlPartExpression.Columns("column1","column2"));
    //    statement.CreatePart(SqlKeyword.Into, SqlPartExpression.Table(targetTable));
    //    statement.CreatePart(SqlKeyword.From, SqlPartExpression.Table("Table"));
    //    return null;
    //}
    internal void CreatePart(SqlKeyword keyword, SqlExpression expression = null)
    {
            SqlParts.Add(new SqlPart(keyword, expression));
        }
    internal void SetIdentityInsert(string tableName, bool enable)
    {
            this.CreatePart(SqlKeyword.Set);
            this.CreatePart(SqlKeyword.Identity_Insert, SqlExpression.Table(tableName));
            if (enable)
                this.CreatePart(SqlKeyword.On);
            else
                this.CreatePart(SqlKeyword.Off);
            this.CreatePart(SqlKeyword.Semicolon);
        }
    //internal static SqlStatement CreateMergeInsert(string sourceTableName, string targetTableName, string mergeOnCondition,
    //    IEnumerable<string> insertColumns, IEnumerable<string> outputColumns, bool deleteIfNotMatched = false)
    //{

    //}
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
                    string format = useParenthese ? "({0})" : "{0}";

                    if (part.Expression != null)
                    {
                        sbSql.Append(string.Format(format, part.Expression.Sql));
                        sbSql.Append(" ");
                    }
                }
            }
            //Output a semicolon for certain SQL Statments
            //if(SqlParts.First().Keyword == SqlKeyword.Merge)
            //{
            //    sbSql.Append(";");
            //}
            return sbSql.ToString();
        }
}