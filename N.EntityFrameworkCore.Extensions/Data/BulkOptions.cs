using Microsoft.Data.SqlClient;
using N.EntityFrameworkCore.Extensions.Enums;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace N.EntityFrameworkCore.Extensions
{
    public class BulkOptions
    {
        public int BatchSize { get; set; }
        public SqlBulkCopyOptions BulkCopyOptions { get; internal set; }
        public SqlBulkCopyColumnOrderHintCollection ColumnOrderHints { get; internal set; }
        public bool EnableStreaming { get; internal set; }
        public int NotifyAfter { get; internal set; }
        public bool UsePermanentTable { get; set; }
        public int? CommandTimeout { get; set; }
        internal ConnectionBehavior ConnectionBehavior { get; set; }


        public SqlRowsCopiedEventHandler SqlRowsCopied { get; internal set; }

        public BulkOptions()
        {
            this.BulkCopyOptions = SqlBulkCopyOptions.Default;
            this.ColumnOrderHints = new SqlBulkCopyColumnOrderHintCollection();
            this.EnableStreaming = false;
            this.NotifyAfter = 0;
            this.ConnectionBehavior = ConnectionBehavior.Default;
        }
    }
}
