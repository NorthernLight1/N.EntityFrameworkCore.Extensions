//using System;
//using System.Collections.Concurrent;
//using System.Collections.Generic;
//using System.Data.Common;
//using System.Data.Entity.Infrastructure.Interception;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;

//namespace N.EntityFrameworkCore.Extensions
//{
//    public class EfExtensionsCommandInterceptor : IDbCommandInterceptor
//    {
//        private ConcurrentDictionary<Guid, EfExtensionsCommand> extensionCommands = new ConcurrentDictionary<Guid, EfExtensionsCommand>();
//        public void NonQueryExecuted(DbCommand command, DbCommandInterceptionContext<int> interceptionContext)
//        {

//        }

//        public void NonQueryExecuting(DbCommand command, DbCommandInterceptionContext<int> interceptionContext)
//        {

//        }

//        public void ReaderExecuted(DbCommand command, DbCommandInterceptionContext<DbDataReader> interceptionContext)
//        {

//        }

//        public void ReaderExecuting(DbCommand command, DbCommandInterceptionContext<DbDataReader> interceptionContext)
//        {
//            foreach(var extensionCommand in extensionCommands)
//            {
//                if(extensionCommand.Value.Connection == command.Connection)
//                {
//                    extensionCommand.Value.Execute(command, interceptionContext);
//                    extensionCommands.TryRemove(extensionCommand.Key, out _);
//                }
//            }
//        }

//        public void ScalarExecuted(DbCommand command, DbCommandInterceptionContext<object> interceptionContext)
//        {

//        }

//        public void ScalarExecuting(DbCommand command, DbCommandInterceptionContext<object> interceptionContext)
//        {

//        }

//        internal void AddCommand(Guid clientConnectionId, EfExtensionsCommand efExtensionsCommand)
//        {
//            extensionCommands.TryAdd(clientConnectionId, efExtensionsCommand);
//        }
//    }
//}
