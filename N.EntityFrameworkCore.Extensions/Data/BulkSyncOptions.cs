
namespace N.EntityFrameworkCore.Extensions;

public class BulkSyncOptions<T> : BulkMergeOptions<T>
{
    public BulkSyncOptions()
    {
            DeleteIfNotMatched = true;
        }
}