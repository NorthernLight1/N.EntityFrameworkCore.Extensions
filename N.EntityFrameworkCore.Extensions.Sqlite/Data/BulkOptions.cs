using System;
using Microsoft.EntityFrameworkCore.Metadata;
using N.EntityFrameworkCore.Extensions.Enums;

namespace N.EntityFrameworkCore.Extensions;

public class BulkOptions
{
    public int BatchSize { get; set; }
    public int NotifyAfter { get; set; }
    public bool UsePermanentTable { get; set; }
    public int? CommandTimeout { get; set; }
    public Action<BulkRowsCopiedEventArgs> RowsCopied { get; set; }
    internal ConnectionBehavior ConnectionBehavior { get; set; }
    internal IEntityType EntityType { get; set; }

    public BulkOptions()
    {
        ConnectionBehavior = ConnectionBehavior.Default;
    }
}