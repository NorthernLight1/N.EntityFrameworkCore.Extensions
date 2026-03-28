using System;
using System.Linq;
using System.Linq.Expressions;

namespace N.EntityFrameworkCore.Extensions;

public class BulkInsertOptions<T> : BulkOptions
{
    public bool AutoMapOutput { get; set; }
    public Expression<Func<T, object>> IgnoreColumns { get; set; }
    public Expression<Func<T, object>> InputColumns { get; set; }
    public bool InsertIfNotExists { get; set; }
    public Expression<Func<T, T, bool>> InsertOnCondition { get; set; }
    public bool KeepIdentity { get; set; }

    public string[] GetInputColumns()
    {
        return InputColumns == null ? null : InputColumns.Body.Type.GetProperties().Select(o => o.Name).ToArray();
    }

    public BulkInsertOptions()
    {
        AutoMapOutput = true;
    }
    internal BulkInsertOptions(BulkOptions options)
    {
        EntityType = options.EntityType;
    }
}