using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace N.EntityFrameworkCore.Extensions
{
    public class FetchOptions<T>
    {
        public Expression<Func<T, object>> IgnoreColumns { get; set; }
        public Expression<Func<T, object>> InputColumns { get; set; }
        public int BatchSize { get; set; }
    }
}