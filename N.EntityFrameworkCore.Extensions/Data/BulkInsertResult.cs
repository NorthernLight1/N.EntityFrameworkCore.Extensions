using System;
using System.Collections.Generic;

namespace N.EntityFrameworkCore.Extensions
{
    internal class BulkInsertResult<T>
    {
        internal int RowsCopied { get; set; }
        internal Dictionary<int, T> EntityMap { get; set; }
    }
}