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

    public SqlQuery(DatabaseFacade database, string sqlText, params object[] parameters)
    {
        this.database = database;
        SqlText = sqlText;
        Parameters = parameters;
    }

    public int Count()
    {
        string countSqlText = SqlBuilder.Parse(SqlText).Count();
        return (int)database.ExecuteScalar(countSqlText, Parameters);
    }
    public async Task<int> CountAsync(CancellationToken cancellationToken = default)
    {
        string countSqlText = SqlBuilder.Parse(SqlText).Count();
        return (int)await database.ExecuteScalarAsync(countSqlText, Parameters, null, cancellationToken);
    }
    public int ExecuteNonQuery()
    {
        return database.ExecuteSql(SqlText, Parameters);
    }
}