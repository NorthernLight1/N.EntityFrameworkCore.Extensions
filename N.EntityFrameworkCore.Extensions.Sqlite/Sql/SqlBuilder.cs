using System;
using System.Collections.Generic;
using System.Linq;

namespace N.EntityFrameworkCore.Extensions.Sql;

internal sealed class SqlBuilder
{
    private static readonly string[] keywords = ["DECLARE", "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY"];
    internal List<SqlClause> Clauses { get; private set; }
    private SqlBuilder(string sql)
    {
        Clauses = [];
        Initialize(sql);
    }

    internal string Count() =>
        $"SELECT COUNT(*) FROM ({string.Join("\r\n", Clauses.Where(o => o.Name != "ORDER BY").Select(o => o.ToString()))}) s";
    public override string ToString() => string.Join("\r\n", Clauses.Select(o => o.ToString()));
    internal static SqlBuilder Parse(string sql) => new SqlBuilder(sql);

    private void Initialize(string sqlText)
    {
        string curClause = string.Empty;
        int curClauseIndex = 0;
        for (int i = 0; i < sqlText.Length;)
        {
            string keyword = StartsWithString(sqlText.AsSpan(i), keywords, StringComparison.OrdinalIgnoreCase);
            bool isWordStart = i == 0 || sqlText[i - 1] == ' ' || (i > 1 && sqlText[i - 2] == '\r' && sqlText[i - 1] == '\n');
            if (keyword != null && isWordStart)
            {
                string inputText = sqlText[curClauseIndex..i];
                if (!string.IsNullOrEmpty(curClause) && curClause != "DECLARE")
                    Clauses.Add(SqlClause.Parse(curClause, inputText));
                curClause = keyword;
                curClauseIndex = i + curClause.Length;
                i = curClauseIndex;
            }
            else
            {
                i++;
            }
        }
        if (!string.IsNullOrEmpty(curClause) && curClause != "DECLARE")
            Clauses.Add(SqlClause.Parse(curClause, sqlText[curClauseIndex..]));
    }

    private static string StartsWithString(ReadOnlySpan<char> textToSearch, string[] valuesToFind, StringComparison stringComparison)
    {
        foreach (var valueToFind in valuesToFind)
        {
            if (textToSearch.StartsWith(valueToFind, stringComparison))
                return valueToFind;
        }

        return null;
    }
}
