using System;
using System.Linq.Expressions;

namespace N.EntityFrameworkCore.Extensions;

public class BulkFetchOptions<T> : BulkOptions
{
    /// <summary>
    /// When true, fetched entities are attached to the DbContext change tracker as Unchanged.
    /// </summary>
    public bool AttachToContext { get; set; }
    public Expression<Func<T, object>> IgnoreColumns { get; set; }
    public Expression<Func<T, object>> InputColumns { get; set; }
    public Expression<Func<T, T, bool>> JoinOnCondition { get; set; }
}