using System;
using System.Linq.Expressions;

namespace N.EntityFrameworkCore.Extensions;

public class FetchOptions<T>
{
    public Expression<Func<T, object>> IgnoreColumns { get; set; }
    public Expression<Func<T, object>> InputColumns { get; set; }
    public int BatchSize { get; set; }
}