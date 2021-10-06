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
        private static IEnumerable<string> keywords = new string[] { "DECLARE", "SELECT", "FROM", "WHERE" };
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
                int maxLenToSearch = sqlText.Length - i >= 7 ? 7 : sqlText.Length - i;
                string keyword = StartsWithString(sqlText.Substring(i, maxLenToSearch), keywords, StringComparison.OrdinalIgnoreCase);
                //Process Sql clause
                if (keyword != null && curClause != keyword)
                {
                    string inputText = sqlText.Substring(curClauseIndex, i - curClauseIndex);
                    if (!string.IsNullOrEmpty(curClause))
                    {
                        if (curClause == "DECLARE")
                        {
                            var declareParts = inputText.Substring(0, inputText.IndexOf(";")).Trim().Split(" ");
                            int sizeStartIndex = declareParts[1].IndexOf("(");
                            string dbTypeString = sizeStartIndex != -1 ? declareParts[1].Substring(0, sizeStartIndex) : declareParts[1];
                            SqlDbType dbType = (SqlDbType)Enum.Parse(typeof(SqlDbType), dbTypeString, true);
                            int size = sizeStartIndex != -1 ? 
                                Convert.ToInt32(declareParts[1].Substring(sizeStartIndex).Replace(")", "")) : 0;
                            string value = declareParts[3][0] == '\'' ? declareParts[3].Substring(1, declareParts[3].Length - 2) : declareParts[3];
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
        private void Validate()
        {
            if(Clauses.Count == 0)
            {
                throw new Exception("You must parse a valid sql statement before you can use this function.");
            }
        }
    }
}