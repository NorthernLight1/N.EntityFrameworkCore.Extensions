using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore.Infrastructure;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace N.EntityFrameworkCore.Extensions
{
    public class SqlQuery
    {
        private DatabaseFacade database;
        public string SqlText { get; private set; }
        public object[] Parameters { get; private set; }

        public SqlQuery(DatabaseFacade database, String sqlText, params object[] parameters)
        {
            this.database = database;
            this.SqlText = sqlText;
            this.Parameters = parameters;
        }

        public int Count()
        {
            string newSqlText = string.Format("SELECT COUNT(*) FROM ({0}) s", this.SqlText);
            return (int)database.ExecuteScalar(newSqlText, this.Parameters);
        }
        public async Task<int> CountAsync(CancellationToken cancellationToken)
        {
            string newSqlText = string.Format("SELECT COUNT(*) FROM ({0}) s", this.SqlText);
            return (int)await database.ExecuteScalarAsync(newSqlText, this.Parameters, null, cancellationToken);
        }
        public int ExecuteNonQuery()
        {
            return database.ExecuteSql(this.SqlText, this.Parameters);
        }
    }
}
