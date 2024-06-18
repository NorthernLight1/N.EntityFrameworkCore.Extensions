using System;
using System.Linq.Expressions;

namespace N.EntityFrameworkCore.Extensions;

public class BulkFetchOptions<T> : BulkOptions
{
    public Expression<Func<T, object>> IgnoreColumns { get; set; }
    public Expression<Func<T, object>> InputColumns { get; set; }
    public Expression<Func<T, T, bool>> JoinOnCondition { get; set; }
    public BulkFetchOptions()
    {
        //this.ConnectionBehavior = ConnectionBehavior.New;
    }
}