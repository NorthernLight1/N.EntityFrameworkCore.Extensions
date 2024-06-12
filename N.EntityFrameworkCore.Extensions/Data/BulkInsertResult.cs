using System;
using System.Collections.Generic;

namespace N.EntityFrameworkCore.Extensions;

internal class BulkInsertResult<T>
{
    internal int RowsAffected { get; set; }
    internal Dictionary<long, T> EntityMap { get; set; }
}