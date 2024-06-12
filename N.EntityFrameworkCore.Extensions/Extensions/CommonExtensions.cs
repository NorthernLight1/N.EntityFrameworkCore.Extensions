using System;

namespace N.EntityFrameworkCore.Extensions.Extensions;

internal static class CommonExtensions
{
    internal static T Build<T>(this Action<T> buildAction) where T : new()
    {
            var parameter = new T();
            buildAction(parameter);
            return parameter;
        }
}