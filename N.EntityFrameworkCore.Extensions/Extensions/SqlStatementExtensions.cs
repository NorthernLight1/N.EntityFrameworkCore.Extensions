﻿using System.Collections.Generic;
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