
namespace N.EntityFrameworkCore.Extensions
{
    public class BulkSyncResult<T> : BulkMergeResult<T>
    {
        public new int RowsDeleted { get; set; }
        public static BulkSyncResult<T> Map(BulkMergeResult<T> result)
        {
            return new BulkSyncResult<T>()
            {
                Output = result.Output,
                RowsAffected = result.RowsAffected,
                RowsDeleted = result.RowsDeleted,
                RowsInserted = result.RowsInserted,
                RowsUpdated = result.RowsUpdated
            };
        }
    }
}
