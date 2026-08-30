using System;

namespace N.EntityFrameworkCore.Extensions;

public sealed class BulkRowsCopiedEventArgs : EventArgs
{
    public long RowsCopied { get; init; }
}
