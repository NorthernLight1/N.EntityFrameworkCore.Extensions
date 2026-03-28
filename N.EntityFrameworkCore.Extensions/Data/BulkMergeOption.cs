using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace N.EntityFrameworkCore.Extensions;

public class BulkMergeOptions<T> : BulkOptions
{
    public Expression<Func<T, T, bool>> MergeOnCondition { get; set; }
    public Expression<Func<T, object>> IgnoreColumnsOnInsert { get; set; }
    public Expression<Func<T, object>> IgnoreColumnsOnUpdate { get; set; }
    public bool AutoMapOutput { get; set; }
    internal bool DeleteIfNotMatched { get; set; }

    public BulkMergeOptions()
    {
            AutoMapOutput = true;
        }
    public List<string> GetIgnoreColumnsOnInsert()
    {
            return IgnoreColumnsOnInsert == null ? new List<string>() : IgnoreColumnsOnInsert.Body.Type.GetProperties().Select(o => o.Name).ToList();
        }
    public List<string> GetIgnoreColumnsOnUpdate()
    {
            return IgnoreColumnsOnUpdate == null ? new List<string>() : IgnoreColumnsOnUpdate.Body.Type.GetProperties().Select(o => o.Name).ToList();
        }
}