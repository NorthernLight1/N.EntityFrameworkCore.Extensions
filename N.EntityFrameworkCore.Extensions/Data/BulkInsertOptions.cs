using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace N.EntityFrameworkCore.Extensions
{
    public class BulkInsertOptions<T> : BulkOptions
    {
        public Expression<Func<T, object>> InputColumns { get; set; }
        public bool AutoMapOutputIdentity { get; set; }
        public bool KeepIdentity { get; set; }
        public bool InsertIfNotExists { get; set; }
        public Expression<Func<T, T, bool>> InsertOnCondition { get; set; }

        public string[] GetInputColumns()
        {
            return this.InputColumns == null ? null : this.InputColumns.Body.Type.GetProperties().Select(o => o.Name).ToArray();
        }

        public BulkInsertOptions()
        {
            this.AutoMapOutputIdentity = true;
            this.InsertIfNotExists = false;
        }
    }
}