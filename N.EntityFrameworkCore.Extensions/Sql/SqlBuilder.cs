using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;

namespace N.EntityFrameworkCore.Extensions.Sql
{
    class SqlBuilder
    {
        private static IEnumerable<string> keywords = new string[] { "DECLARE", "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY" };
        public string Sql
        {
            get { return this.ToString(); }
        }
        public List<SqlClause> Clauses { get; private set; }
        public List<SqlParameter> Parameters { get; private set; }
        private SqlBuilder(string sql)
        {
            Clauses = new List<SqlClause>();
            Parameters = new List<SqlParameter>();
            Initialize(sql);
        }

        private void Initialize(string sqlText)
        {
            string curClause = string.Empty;
            int curClauseIndex = 0;
            for (int i = 0; i < sqlText.Length;)
            {
                //Find new Sql clause
                int maxLenToSearch = sqlText.Length - i >= 10 ? 10 : sqlText.Length - i;
                string keyword = StartsWithString(sqlText.Substring(i, maxLenToSearch), keywords, StringComparison.OrdinalIgnoreCase);
                bool isWordStart = i > 0 ? sqlText[i - 1] == ' ' || (i > 1 && sqlText.Substring(i-2,2) == "\r\n") : true;
                //Process Sql clause
                if (keyword != null && curClause != keyword && isWordStart)
                {
                    string inputText = sqlText.Substring(curClauseIndex, i - curClauseIndex);
                    if (!string.IsNullOrEmpty(curClause))
                    {
                        if (curClause == "DECLARE")
                        {
                            var declareParts = inputText.Substring(0, inputText.IndexOf(";")).Trim().Split(" ");
                            int sizeStartIndex = declareParts[1].IndexOf("(");
                            int sizeLength = declareParts[1].IndexOf(")") - (sizeStartIndex+1);
                            string dbTypeString = sizeStartIndex != -1 ? declareParts[1].Substring(0, sizeStartIndex) : declareParts[1];
                            SqlDbType dbType = (SqlDbType)Enum.Parse(typeof(SqlDbType), dbTypeString, true);
                            int size = sizeStartIndex != -1 ? 
                                Convert.ToInt32(declareParts[1].Substring(sizeStartIndex+1, sizeLength)) : 0;
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
                Clauses.Add(SqlClause.Parse(curClause, sqlText.Substring(curClauseIndex)));
        }

        private string GetDeclareValue(string value)
        {
            if(value.StartsWith("'"))
            {
                return value.Substring(1, value.Length - 2);
            }
            else if (value.StartsWith("N'"))
            {
                return value.Substring(2, value.Length - 3);
            }
            else
            {
                return value;
            }
        }

        public string Count()
        {
            return string.Format("SELECT COUNT(*) FROM ({0}) s", string.Join("\r\n", Clauses.Where(o => o.Name != "ORDER BY").Select(o => o.ToString())));
        }
        public override string ToString()
        {
            return string.Join("\r\n", Clauses.Select(o => o.ToString()));
        }
        private static string StartsWithString(string textToSearch, IEnumerable<string> valuesToFind, StringComparison stringComparison)
        {
            string value=null;
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
        public static SqlBuilder Parse(string sql)
        {
            return new SqlBuilder(sql);
        }
        public String GetTableAlias()
        {
            var sqlFromClause = Clauses.First(o => o.Name == "FROM");
            var startIndex = sqlFromClause.InputText.LastIndexOf(" AS ");
            return startIndex > 0 ? sqlFromClause.InputText.Substring(startIndex+4) : "";
        }
        public void ChangeToDelete()
        {
            Validate();
            var sqlClause = Clauses.FirstOrDefault();
            var sqlFromClause = Clauses.First(o => o.Name == "FROM");
            if(sqlClause != null)
            {
                sqlClause.Name = "DELETE";
                sqlClause.InputText = sqlFromClause.InputText.Substring(sqlFromClause.InputText.LastIndexOf("AS ") + 3);
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
            string insertValueExpression = string.Format("INTO {0} ({1})", tableName, columnsToInsert);
            Clauses.Insert(0, new SqlClause { Name = "INSERT", InputText = insertValueExpression });
            sqlSelectClause.InputText = columnsToInsert;
            
        }
        internal void SelectColumns(IEnumerable<string> columns)
        {
            var tableAlias = GetTableAlias();
            var sqlClause = Clauses.FirstOrDefault();
            if (sqlClause.Name == "SELECT")
            {
                sqlClause.InputText = string.Join(",", columns.Select(c => string.Format("{0}.{1}", tableAlias, c)));
            }
        }
        private void Validate()
        {
            if(Clauses.Count == 0)
            {
                throw new Exception("You must parse a valid sql statement before you can use this function.");
            }
        }
    }
}