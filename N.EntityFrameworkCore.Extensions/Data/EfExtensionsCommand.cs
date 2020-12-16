//using System;
//using System.Collections.Generic;
//using System.Data.Common;
//using System.Data.Entity.Infrastructure.Interception;
//using System.Data.SqlClient;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;

//namespace N.EntityFrameworkCore.Extensions
//{
//    class EfExtensionsCommand
//    {
//        public EfExtensionsCommandType CommandType { get; set; }
//        public string OldValue { get; set; }
//        public string NewValue { get; set; }
//        public SqlConnection Connection { get; internal set; }

//        internal bool Execute(DbCommand command, DbCommandInterceptionContext<DbDataReader> interceptionContext)
//        {
//            if(CommandType == EfExtensionsCommandType.ChangeTableName)
//            {
//                command.CommandText = command.CommandText.Replace(OldValue, NewValue);
//            }

//            return true;
//        }
//    }
//}
