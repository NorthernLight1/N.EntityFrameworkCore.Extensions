using Microsoft.Data.SqlClient;
using N.EntityFrameworkCore.Extensions.Util;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace N.EntityFrameworkCore.Extensions
{
    internal static class SqlUtil
    {
        internal static string ConvertToColumnString(IEnumerable<string> columnNames)
        {
            return string.Join(",", columnNames);
        }
    }
}