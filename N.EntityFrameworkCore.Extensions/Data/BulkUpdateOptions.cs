using System;
using System.Linq.Expressions;

namespace N.EntityFrameworkCore.Extensions
{
    public class BulkUpdateOptions<T> : BulkOptions
    {
        public Expression<Func<T, object>> IgnoreColumns { get; set; }
        [Obsolete("BulkUpdateOptions.IgnoreColumnsOnUpdate has been replaced by IgnoreColumns.")]
        public Expression<Func<T, object>> IgnoreColumnsOnUpdate { get { return IgnoreColumns; } set { IgnoreColumns = value; } }
        public Expression<Func<T, T, bool>> UpdateOnCondition { get; set; }
    }
}