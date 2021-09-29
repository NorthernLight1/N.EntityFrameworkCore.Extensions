using Microsoft.EntityFrameworkCore.Diagnostics;
using System;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace N.EntityFrameworkCore.Extensions
{
    public class EfExtensionsCommandInterceptor : DbCommandInterceptor
    {
        private ConcurrentDictionary<Guid, EfExtensionsCommand> extensionCommands = new ConcurrentDictionary<Guid, EfExtensionsCommand>();
        public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result)
        {
            foreach (var extensionCommand in extensionCommands)
            {
                if (extensionCommand.Value.Connection == command.Connection)
                {
                    extensionCommand.Value.Execute(command, eventData, result);
                    extensionCommands.TryRemove(extensionCommand.Key, out _);
                }
            }
            return result;
        }
        internal void AddCommand(Guid clientConnectionId, EfExtensionsCommand efExtensionsCommand)
        {
            extensionCommands.TryAdd(clientConnectionId, efExtensionsCommand);
        }
    }
}
