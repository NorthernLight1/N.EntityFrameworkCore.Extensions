namespace N.EntityFrameworkCore.Extensions.Util;

internal static class BulkMergeDiagnostics
{
    internal static string LastNativeUpsertSql { get; private set; }

    internal static void RecordNativeUpsertSql(string sql) => LastNativeUpsertSql = sql;

    internal static void Reset() => LastNativeUpsertSql = null;
}
