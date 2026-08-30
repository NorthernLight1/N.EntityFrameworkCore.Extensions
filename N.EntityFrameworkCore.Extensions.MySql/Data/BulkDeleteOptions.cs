using System;
using System.Linq.Expressions;

namespace N.EntityFrameworkCore.Extensions;

public class BulkDeleteOptions<T> : BulkOptions
{
    public Expression<Func<T, T, bool>> DeleteOnCondition { get; set; }
    /// <summary>When set, performs UPDATE instead of DELETE. Expression selects columns/values from each entity (e.g. o => new { o.IsDeleted, o.DeletedAt }).</summary>
    public Expression<Func<T, object>> SoftDeleteColumns { get; set; }
}