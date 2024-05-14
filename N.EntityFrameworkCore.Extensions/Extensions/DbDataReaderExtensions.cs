using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Reflection;

namespace N.EntityFrameworkCore.Extensions.Extensions;

static class DbDataReaderExtensions
{
    internal static T MapEntity<T>(this DbDataReader reader, List<PropertyInfo> propertySetters) where T : class, new()
    {
        var entity = new T();
        for (var i = 0; i < reader.FieldCount; i++)
        {
            var value = reader.GetValue(i);
            if (value == DBNull.Value)
                value = null;

            var prop = propertySetters[i];
            if (prop == null) continue;

            if (prop.PropertyType.IsEnum)
            {
                var valueString = value?.ToString();
                prop.SetValue(entity, string.IsNullOrWhiteSpace(valueString) ? null : Enum.Parse(prop.PropertyType, valueString));
            }
            else if (prop.PropertyType.IsNullableEnum())
            {
                var valueString = value?.ToString();
                var enumType = Nullable.GetUnderlyingType(prop.PropertyType)!;
                prop.SetValue(entity, string.IsNullOrWhiteSpace(valueString) ? null : Enum.Parse(enumType, valueString));
            }
            else
            {
                prop.SetValue(entity, value);
            }
        }

        return entity;
    }

    static bool IsNullableEnum(this Type t)
    {
        var u = Nullable.GetUnderlyingType(t);
        return u is { IsEnum: true };
    }

    internal static List<PropertyInfo> GetPropertyInfos<T>(this DbDataReader reader)
    {
        var propertySetters = new List<PropertyInfo>();
        var entityType = typeof(T);

        for (var i = 0; i < reader.FieldCount; i++)
        {
            var prop = entityType.GetProperty(reader.GetName(i));
            propertySetters.Add(prop);
        }

        return propertySetters;
    }
}