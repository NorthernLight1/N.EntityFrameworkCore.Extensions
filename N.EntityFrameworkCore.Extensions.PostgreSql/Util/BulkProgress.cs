namespace N.EntityFrameworkCore.Extensions;

internal static class BulkProgress
{
    internal static void Notify(BulkOptions options, ref long lastNotified, long rowsCopied)
    {
        if (options.RowsCopied == null)
            return;

        if (options.NotifyAfter > 0)
        {
            while (rowsCopied - lastNotified >= options.NotifyAfter)
            {
                lastNotified += options.NotifyAfter;
                options.RowsCopied(new BulkRowsCopiedEventArgs { RowsCopied = lastNotified });
            }
        }
    }

    internal static void NotifyFinal(BulkOptions options, long rowsCopied)
    {
        if (options.RowsCopied != null && rowsCopied > 0)
            options.RowsCopied(new BulkRowsCopiedEventArgs { RowsCopied = rowsCopied });
    }
}
