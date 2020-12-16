using System;
using System.Linq.Expressions;

namespace N.EntityFrameworkCore.Extensions
{
    public class BulkUpdateOptions<T> : BulkOptions
    {
        public Expression<Func<T, object>> IgnoreColumnsOnUpdate { get; set; }
    }
}