using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using N.EntityFrameworkCore.Extensions.Enums;

namespace N.EntityFrameworkCore.Extensions
{
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
}