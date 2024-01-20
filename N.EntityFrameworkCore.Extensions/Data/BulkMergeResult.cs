using Microsoft.EntityFrameworkCore.Metadata.Internal;
using System.Collections.Generic;

namespace N.EntityFrameworkCore.Extensions
{
    public class BulkMergeResult<T>
    {
        public IEnumerable<BulkMergeOutputRow<T>> Output { get; set; }
        public int RowsAffected { get; set; }
        public int RowsDeleted { get; internal set; }
        public int RowsInserted { get; internal set; }
        public int RowsUpdated { get; internal set; }
    }
}