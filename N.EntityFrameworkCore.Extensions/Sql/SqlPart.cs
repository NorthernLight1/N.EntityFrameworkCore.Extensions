namespace N.EntityFrameworkCore.Extensions.Sql;

internal sealed class SqlPart
{
    internal SqlKeyword Keyword { get; }
    internal SqlExpression Expression { get; }
    internal bool IgnoreOutput => GetIgnoreOutput();
    internal SqlPart(SqlKeyword keyword, SqlExpression expression)
    {
        Keyword = keyword;
        Expression = expression;
    }
    private bool GetIgnoreOutput() => Keyword == SqlKeyword.Output && (Expression == null || Expression.IsEmpty);
}