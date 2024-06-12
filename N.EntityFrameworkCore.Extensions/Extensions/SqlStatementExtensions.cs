using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using N.EntityFrameworkCore.Extensions.Sql;

namespace N.EntityFrameworkCore.Extensions.Extensions;

internal static class SqlStatementExtensions
{
    internal static void WriteInsert(this SqlStatement statement, IEnumerable<string> insertColumns)
    {
            statement.CreatePart(SqlKeyword.Insert, SqlExpression.Columns(insertColumns));
            statement.CreatePart(SqlKeyword.Values, SqlExpression.Columns(insertColumns));
        }
}