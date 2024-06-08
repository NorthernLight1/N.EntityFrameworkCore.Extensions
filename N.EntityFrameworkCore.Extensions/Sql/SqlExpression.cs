using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.InteropServices.ObjectiveC;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using N.EntityFrameworkCore.Extensions.Util;

namespace N.EntityFrameworkCore.Extensions.Sql
{
    internal class SqlExpression
    {
        internal SqlExpressionType ExpressionType { get; }
        List<object> Items { get; set; }
        internal string Sql => ToSql();
        string Alias { get; }
        public bool IsEmpty => Items.Count == 0;

        SqlExpression(SqlExpressionType expressionType, object item, string alias = null)
        {
            ExpressionType = expressionType;
            Items = new List<object>();
            if (item is IEnumerable<string> values)
            {
                Items.AddRange(values.ToArray());
            }
            else
            {
                Items.Add(item);
            }
            Alias = alias;
        }
        SqlExpression(SqlExpressionType expressionType, object[] items, string alias = null)
        {
            ExpressionType = expressionType;
            Items = new List<object>();
            Items.AddRange(items);
            Alias = alias;
        }
        internal static SqlExpression Columns(IEnumerable<string> columns)
        {
            return new SqlExpression(SqlExpressionType.Columns, columns);
        }

        internal static SqlExpression Set(IEnumerable<string> columns)
        {
            return new SqlExpression(SqlExpressionType.Set, columns);
        }

        internal static SqlExpression String(string joinOnCondition)
        {
            return new SqlExpression(SqlExpressionType.String, joinOnCondition);
        }

        internal static SqlExpression Table(string tableName, string alias = null)
        {
            return new SqlExpression(SqlExpressionType.Table, Util.CommonUtil.FormatTableName(tableName), alias);
        }

        private string ToSql()
        {
            var values = Items.Select(o => o.ToString()).ToArray();
            StringBuilder sbSql = new StringBuilder();
            if (ExpressionType == SqlExpressionType.Columns)
            {
                sbSql.Append(string.Join(",", CommonUtil.FormatColumns(values)));
            }
            else
            {
                sbSql.Append(string.Join(",", Items.Select(o => o.ToString())));
            }
            if (Alias != null)
            {
                sbSql.Append(" ");
                sbSql.Append(SqlKeyword.As.ToString().ToUpper());
                sbSql.Append(" ");
                sbSql.Append(Alias);
            }
            //var test = Items.Select(o => o.ToString()).ToArray();
            return sbSql.ToString();
        }
    }
}