using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.Identity.Client;

namespace N.EntityFrameworkCore.Extensions.Sql;

internal class SqlPart
{
    internal SqlKeyword Keyword { get; }
    internal SqlExpression Expression { get; }
    internal bool IgnoreOutput => GetIgnoreOutput();
    public SqlPart(SqlKeyword keyword, SqlExpression expression)
    {
            Keyword = keyword;
            Expression = expression;
        }
    private bool GetIgnoreOutput()
    {
            if (Keyword == SqlKeyword.Output && (Expression == null || Expression.IsEmpty))
                return true;
            else
                return false;
        }


}