using Microsoft.EntityFrameworkCore.Metadata;
using N.EntityFrameworkCore.Extensions.Enums;

namespace N.EntityFrameworkCore.Extensions;

public class BulkOptions
{
    public int BatchSize { get; set; }
    public bool UsePermanentTable { get; set; }
    public int? CommandTimeout { get; set; }
    internal ConnectionBehavior ConnectionBehavior { get; set; }
    internal IEntityType EntityType { get; set; }

    public BulkOptions()
    {
        ConnectionBehavior = ConnectionBehavior.Default;
    }
}