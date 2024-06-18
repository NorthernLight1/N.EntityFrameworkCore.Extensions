
namespace N.EntityFrameworkCore.Extensions;

public class BulkSyncOptions<T> : BulkMergeOptions<T>
{
    public BulkSyncOptions()
    {
            this.DeleteIfNotMatched = true;
        }
}