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