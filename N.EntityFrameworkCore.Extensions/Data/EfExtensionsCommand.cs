using System.Data.Common;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace N.EntityFrameworkCore.Extensions;

class EfExtensionsCommand
{
    public EfExtensionsCommandType CommandType { get; set; }
    public string OldValue { get; set; }
    public string NewValue { get; set; }
    public SqlConnection Connection { get; internal set; }

    internal bool Execute(DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result)
    {
            if (CommandType == EfExtensionsCommandType.ChangeTableName)
            {
                command.CommandText = command.CommandText.Replace(OldValue, NewValue);
            }

            return true;
        }
}