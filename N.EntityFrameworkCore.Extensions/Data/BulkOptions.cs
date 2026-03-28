using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore.Metadata;
using N.EntityFrameworkCore.Extensions.Enums;

namespace N.EntityFrameworkCore.Extensions;

public class BulkOptions
{
    public int BatchSize { get; set; }
    public SqlBulkCopyOptions BulkCopyOptions { get; internal set; }
    public SqlBulkCopyColumnOrderHintCollection ColumnOrderHints { get; internal set; }
    public bool EnableStreaming { get; internal set; }
    public int NotifyAfter { get; internal set; }
    public bool UsePermanentTable { get; set; }
    public int? CommandTimeout { get; set; }
    internal ConnectionBehavior ConnectionBehavior { get; set; }
    internal IEntityType EntityType { get; set; }

    public SqlRowsCopiedEventHandler SqlRowsCopied { get; internal set; }

    public BulkOptions()
    {
        BulkCopyOptions = SqlBulkCopyOptions.Default;
        ColumnOrderHints = new SqlBulkCopyColumnOrderHintCollection();
        ConnectionBehavior = ConnectionBehavior.Default;
    }
}