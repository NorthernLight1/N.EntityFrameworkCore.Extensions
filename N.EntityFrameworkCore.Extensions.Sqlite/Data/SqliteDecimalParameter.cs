using Microsoft.Data.Sqlite;

namespace N.EntityFrameworkCore.Extensions;

/// <summary>
/// A SQLite parameter for decimal values that enables numeric comparison with TEXT-stored decimal columns.
/// </summary>
public class SqliteDecimalParameter : SqliteParameter
{
    public SqliteDecimalParameter(string name, decimal value)
        : base(name, (double)value)
    {
        SqliteType = SqliteType.Real;
    }
}
