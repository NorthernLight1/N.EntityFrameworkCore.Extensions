using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.Data.SqlClient;

namespace N.EntityFrameworkCore.Extensions.Sql;

internal sealed class SqlBuilder
{
    private static readonly string[] keywords = ["DECLARE", "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY"];
    public string Sql => ToString();
    public List<SqlClause> Clauses { get; private set; }
    public List<SqlParameter> Parameters { get; private set; }
    private SqlBuilder(string sql)
    {
        Clauses = [];
        Parameters = [];
        Initialize(sql);
    }

    public string Count() =>
        $"SELECT COUNT(*) FROM ({string.Join("\r\n", Clauses.Where(o => o.Name != "ORDER BY").Select(o => o.ToString()))}) s";
    public override string ToString() => string.Join("\r\n", Clauses.Select(o => o.ToString()));
    public static SqlBuilder Parse(string sql) => new SqlBuilder(sql);
    public string GetTableAlias()
    {
        var sqlFromClause = Clauses.First(o => o.Name == "FROM");
        var startIndex = sqlFromClause.InputText.LastIndexOf(" AS ");
        return startIndex > 0 ? sqlFromClause.InputText[(startIndex + 4)..] : "";
    }
    public void ChangeToDelete()
    {
        Validate();
        var sqlClause = Clauses.FirstOrDefault();
        var sqlFromClause = Clauses.First(o => o.Name == "FROM");
        if (sqlClause != null)
        {
            sqlClause.Name = "DELETE";
            int aliasStartIndex = sqlFromClause.InputText.IndexOf("AS ") + 3;
            int aliasLength = sqlFromClause.InputText.IndexOf(']', aliasStartIndex) - aliasStartIndex + 1;
            sqlClause.InputText = sqlFromClause.InputText[aliasStartIndex..(aliasStartIndex + aliasLength)];
        }
    }
    public void ChangeToUpdate(string updateExpression, string setExpression)
    {
        Validate();
        var sqlClause = Clauses.FirstOrDefault();
        if (sqlClause != null)
        {
            sqlClause.Name = "UPDATE";
            sqlClause.InputText = updateExpression;
            Clauses.Insert(1, new SqlClause { Name = "SET", InputText = setExpression });
        }
    }
    internal void ChangeToInsert<T>(string tableName, Expression<Func<T, object>> insertObjectExpression)
    {
        Validate();
        var sqlSelectClause = Clauses.FirstOrDefault();
        string columnsToInsert = string.Join(",", insertObjectExpression.GetObjectProperties());
        string insertValueExpression = $"INTO {tableName} ({columnsToInsert})";
        Clauses.Insert(0, new SqlClause { Name = "INSERT", InputText = insertValueExpression });
        sqlSelectClause.InputText = columnsToInsert;
    }
    internal void SelectColumns(IEnumerable<string> columns)
    {
        var tableAlias = GetTableAlias();
        var sqlClause = Clauses.FirstOrDefault();
        if (sqlClause.Name == "SELECT")
        {
            sqlClause.InputText = string.Join(",", columns.Select(c => $"{tableAlias}.{c}"));
        }
    }
    private void Initialize(string sqlText)
    {
        string curClause = string.Empty;
        int curClauseIndex = 0;
        for (int i = 0; i < sqlText.Length;)
        {
            int maxLenToSearch = sqlText.Length - i >= 10 ? 10 : sqlText.Length - i;
            string keyword = StartsWithString(sqlText[i..(i + maxLenToSearch)], keywords, StringComparison.OrdinalIgnoreCase);
            bool isWordStart = i > 0 ? sqlText[i - 1] == ' ' || (i > 1 && sqlText[(i - 2)..i] == "\r\n") : true;
            if (keyword != null && isWordStart)
            {
                string inputText = sqlText[curClauseIndex..i];
                if (!string.IsNullOrEmpty(curClause))
                {
                    if (curClause == "DECLARE")
                    {
                        var declareParts = inputText[..inputText.IndexOf(';')].Trim().Split(' ');
                        int sizeStartIndex = declareParts[1].IndexOf('(');
                        int sizeLength = declareParts[1].IndexOf(')') - (sizeStartIndex + 1);
                        string dbTypeString = sizeStartIndex != -1 ? declareParts[1][..sizeStartIndex] : declareParts[1];
                        SqlDbType dbType = (SqlDbType)Enum.Parse(typeof(SqlDbType), dbTypeString, true);
                        int size = sizeStartIndex != -1 ?
                            Convert.ToInt32(declareParts[1][(sizeStartIndex + 1)..(sizeStartIndex + 1 + sizeLength)]) : 0;
                        string value = GetDeclareValue(declareParts[3]);
                        Parameters.Add(new SqlParameter(declareParts[0], dbType, size) { Value = value });
                    }
                    else
                    {
                        Clauses.Add(SqlClause.Parse(curClause, inputText));
                    }
                }
                curClause = keyword;
                curClauseIndex = i + curClause.Length;
                i = i + curClause.Length;
            }
            else
            {
                i++;
            }
        }
        if (!string.IsNullOrEmpty(curClause))
            Clauses.Add(SqlClause.Parse(curClause, sqlText[curClauseIndex..]));
    }
    private string GetDeclareValue(string value)
    {
        if (value.StartsWith('\''))
        {
            return value[1..^1];
        }
        else if (value.StartsWith("N'"))
        {
            return value[2..^1];
        }
        else if (value.StartsWith("CAST("))
        {
            return value[5..];
        }
        else
        {
            return value;
        }
    }
    private static string StartsWithString(string textToSearch, IEnumerable<string> valuesToFind, StringComparison stringComparison)
    {
        string value = null;
        foreach (var valueToFind in valuesToFind)
        {
            if (textToSearch.StartsWith(valueToFind, stringComparison))
            {
                value = valueToFind;
                break;
            }
        }

        return value;
    }
    private void Validate()
    {
        if (Clauses.Count == 0)
        {
            throw new Exception("You must parse a valid sql statement before you can use this function.");
        }
    }
}
