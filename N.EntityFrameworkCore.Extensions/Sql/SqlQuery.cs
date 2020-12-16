using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace N.EntityFrameworkCore.Extensions.Sql
{
    class SqlQuery
    {
        private static IEnumerable<string> keywords = new string[] { "SELECT", "FROM", "WHERE" };
        public string Sql
        {
            get { return this.ToString(); }
        }
        public List<SqlClause> Clauses { get; private set; }
        private SqlQuery(string sql)
        {
            Clauses = new List<SqlClause>();
            Initialize(sql);
        }

        private void Initialize(string sqlText)
        {
            string curClause = string.Empty;
            int curClauseIndex = 0;
            for (int i = 0; i < sqlText.Length;)
            {
                //Find new Sql clause
                int maxLenToSearch = sqlText.Length - i >= 6 ? 6 : sqlText.Length - i;
                string keyword = StartsWithString(sqlText.Substring(i, maxLenToSearch), keywords, StringComparison.OrdinalIgnoreCase);
                //Process Sql clause
                if (keyword != null && curClause != keyword)
                {
                    if (!string.IsNullOrEmpty(curClause))
                    {
                        Clauses.Add(SqlClause.Parse(curClause, sqlText.Substring(curClauseIndex, i - curClauseIndex)));
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
        public static SqlQuery Parse(string sql)
        {
            return new SqlQuery(sql);
        }
        public void ChangeToDelete(string expression)
        {
            Validate();
            var sqlClause = Clauses.FirstOrDefault();
            if(sqlClause != null)
            {
                sqlClause.Name = "DELETE";
                sqlClause.InputText = expression;
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