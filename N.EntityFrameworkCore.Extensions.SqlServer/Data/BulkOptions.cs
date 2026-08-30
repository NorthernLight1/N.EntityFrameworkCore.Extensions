using System;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore.Metadata;
using N.EntityFrameworkCore.Extensions.Enums;

namespace N.EntityFrameworkCore.Extensions;

public class BulkOptions
{
    public int BatchSize { get; set; }
    public SqlBulkCopyOptions BulkCopyOptions { get; set; }
    public SqlBulkCopyColumnOrderHintCollection ColumnOrderHints { get; set; }
    public bool EnableStreaming { get; set; }
    public int NotifyAfter { get; set; }
    public bool UsePermanentTable { get; set; }
    public int? CommandTimeout { get; set; }
    internal ConnectionBehavior ConnectionBehavior { get; set; }
    internal IEntityType EntityType { get; set; }

    public SqlRowsCopiedEventHandler SqlRowsCopied { get; set; }
    public Action<BulkRowsCopiedEventArgs> RowsCopied { get; set; }

    public bool FireTriggers
    {
        get => BulkCopyOptions.HasFlag(SqlBulkCopyOptions.FireTriggers);
        set => BulkCopyOptions = value
            ? BulkCopyOptions | SqlBulkCopyOptions.FireTriggers
            : BulkCopyOptions & ~SqlBulkCopyOptions.FireTriggers;
    }

    public BulkOptions()
    {
        BulkCopyOptions = SqlBulkCopyOptions.Default;
        ColumnOrderHints = new SqlBulkCopyColumnOrderHintCollection();
        ConnectionBehavior = ConnectionBehavior.Default;
    }
}