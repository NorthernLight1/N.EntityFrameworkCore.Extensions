using System;
using System.Collections.Generic;
using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace N.EntityFrameworkCore.Extensions.Extensions;

static class DbDataReaderExtensions
{
    internal static T MapEntity<T>(this DbDataReader reader, DbContext dbContext, IProperty[] properties, Func<object, object>[] valuesFromProvider) where T : class, new()
    {
        var entity = new T();
        var entry = dbContext.Entry(entity);

        for (var i = 0; i < reader.FieldCount; i++)
        {
            var property = properties[i];
            var value = valuesFromProvider[i].Invoke(reader.GetValue(i));
            if (value == DBNull.Value)
                value = null;

            if (property.DeclaringType is IComplexType complexType)
            {
                var complexProperty = entry.ComplexProperty(complexType.ComplexProperty);
                if (complexProperty.CurrentValue == null)
                {
                    complexProperty.CurrentValue = Activator.CreateInstance(complexType.ClrType);
                }
                complexProperty.Property(property).CurrentValue = value;
            }
            else
            {
                entry.Property(property).CurrentValue = value;
            }
        }
        return entity;
    }
    internal static IProperty[] GetProperties(this DbDataReader reader, TableMapping tableMapping)
    {
        var properties = new List<IProperty>();

        for (var i = 0; i < reader.FieldCount; i++)
        {
            var property = tableMapping.GetPropertyFromColumnName(reader.GetName(i));
            properties.Add(property);
        }

        return properties.ToArray();
    }
}