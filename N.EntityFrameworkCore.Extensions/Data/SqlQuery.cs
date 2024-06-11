using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore.Infrastructure;
using N.EntityFrameworkCore.Extensions.Sql;

namespace N.EntityFrameworkCore.Extensions;

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
            string countSqlText = SqlBuilder.Parse(this.SqlText).Count();
            return (int)database.ExecuteScalar(countSqlText, this.Parameters);
        }
    public async Task<int> CountAsync(CancellationToken cancellationToken = default)
    {
            string countSqlText = SqlBuilder.Parse(this.SqlText).Count();
            return (int)await database.ExecuteScalarAsync(countSqlText, this.Parameters, null, cancellationToken);
        }
    public int ExecuteNonQuery()
    {
            return database.ExecuteSql(this.SqlText, this.Parameters);
        }
}