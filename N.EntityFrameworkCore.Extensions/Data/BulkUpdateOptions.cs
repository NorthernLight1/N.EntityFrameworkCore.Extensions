using System;
using System.Linq.Expressions;

namespace N.EntityFrameworkCore.Extensions
{
    public class BulkUpdateOptions<T> : BulkOptions
    {
        public Expression<Func<T, object>> IgnoreColumnsOnUpdate { get; set; }
        public Expression<Func<T, T, bool>> UpdateOnCondition { get; set; }
    }
}