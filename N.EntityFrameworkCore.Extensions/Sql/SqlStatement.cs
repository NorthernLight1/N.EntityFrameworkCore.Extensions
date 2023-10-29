using Microsoft.Extensions.Options;
using N.EntityFrameworkCore.Extensions.Extensions;
using N.EntityFrameworkCore.Extensions.Util;
using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace N.EntityFrameworkCore.Extensions.Sql
{
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
        internal static SqlStatement CreateMergeInsert(string sourceTableName, string targetTableName, string joinOnCondition, IEnumerable<string> insertColumns, IEnumerable<string> outputColumns)
        {
            var statement = new SqlStatement();
            statement.CreatePart(SqlKeyword.Merge, SqlExpression.Table(targetTableName, "t"));
            statement.CreatePart(SqlKeyword.Using, SqlExpression.Table(sourceTableName, "s"));
            statement.CreatePart(SqlKeyword.On, SqlExpression.String(joinOnCondition));
            statement.CreatePart(SqlKeyword.When);
            statement.CreatePart(SqlKeyword.Not);
            statement.CreatePart(SqlKeyword.Matched);
            statement.CreatePart(SqlKeyword.Then);
            statement.WriteInsert(insertColumns);
            statement.CreatePart(SqlKeyword.Output, SqlExpression.Columns(outputColumns));
            return statement;
        }

        private string ToSql()
        {
            StringBuilder sbSql = new StringBuilder();
            foreach(var part in SqlParts)
            {
                if (!part.IgnoreOutput)
                {
                    sbSql.Append(part.Keyword.ToString().ToUpper() + " ");
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
            if(SqlParts.First().Keyword == SqlKeyword.Merge)
            {
                sbSql.Append(";");
            }
            return sbSql.ToString();
        }
    }
}
