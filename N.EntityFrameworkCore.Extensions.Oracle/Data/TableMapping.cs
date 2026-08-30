using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using N.EntityFrameworkCore.Extensions.Extensions;
using N.EntityFrameworkCore.Extensions.Util;

namespace N.EntityFrameworkCore.Extensions;

public class TableMapping
{
    public DbContext DbContext { get; private set; }
    public IEntityType EntityType { get; set; }
    public IProperty[] Properties { get; }
    public string Schema { get; }
    public string TableName { get; }
    public IEnumerable<IEntityType> EntityTypes { get; }

    public bool HasIdentityColumn => EntityType.FindPrimaryKey().Properties.Any(o => o.ValueGenerated != ValueGenerated.Never);
    public StoreObjectIdentifier StoreObjectIdentifier => StoreObjectIdentifier.Table(TableName, EntityType.GetSchema() ?? DbContext.Database.GetDefaultSchema());
    private Dictionary<string, IProperty> ColumnMap { get; set; }
    public string FullQualifedTableName => DbContext.DelimitIdentifier(TableName, Schema);

    public TableMapping(DbContext dbContext, IEntityType entityType)
    {
        DbContext = dbContext;
        EntityType = entityType;
        Properties = GetProperties(entityType);
        ColumnMap = Properties
            .GroupBy(p => GetColumnName(p), StringComparer.OrdinalIgnoreCase)
            .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);
        Schema = entityType.GetSchema() ?? dbContext.Database.GetDefaultSchema();
        TableName = entityType.GetTableName();
        EntityTypes = EntityType.GetAllBaseTypesInclusive().Where(o => !o.IsAbstract());
    }
    public IProperty GetPropertyFromColumnName(string columnName) => ColumnMap[columnName];
    private static IProperty[] GetProperties(IEntityType entityType)
    {
        var properties = entityType.GetProperties().ToList();
        properties.AddRange(entityType.GetComplexProperties().SelectMany(p => p.ComplexType.GetProperties()));
        properties.AddRange(entityType.GetNavigations()
            .Where(n => n.ForeignKey.IsOwnership)
            .SelectMany(n => n.TargetEntityType.GetProperties()
                .Where(p => !n.ForeignKey.Properties.Contains(p))));
        return properties
            .Select(p => new { Property = p, Column = FindMappedColumn(p, entityType) })
            .Where(x => x.Column != null && !string.IsNullOrEmpty(x.Column.Name))
            .DistinctBy(x => x.Column!.Name)
            .Select(x => x.Property)
            .ToArray();
    }
    private static IColumnBase FindMappedColumn(IProperty property, IEntityType rootEntityType)
    {
        IEntityType entityType = property.DeclaringType switch
        {
            IEntityType declaringEntityType => declaringEntityType,
            IComplexType complexType when complexType.ComplexProperty.DeclaringType is IEntityType complexDeclaringEntityType => complexDeclaringEntityType,
            _ => rootEntityType
        };
        if (entityType == null || entityType.IsAbstract())
            entityType = rootEntityType;
        if (entityType.IsOwned())
            entityType = entityType.FindOwnership()?.PrincipalEntityType ?? rootEntityType;
        var tableName = entityType.GetTableName();
        if (string.IsNullOrEmpty(tableName))
            return null;
        return property.FindColumn(StoreObjectIdentifier.Table(tableName, entityType.GetSchema()));
    }

    public IEnumerable<string> GetQualifiedColumnNames(IEnumerable<string> columnNames, IEntityType entityType = null)
    {
        return Properties.Where(o => entityType == null || o.GetDeclaringEntityType() == entityType)
            .Select(o => new
            {
                Column = FindColumn(o),
                Name = GetColumnName(o)
            })
            .Where(o => columnNames == null || columnNames.Contains(o.Name))
            .Select(o => $"{DbContext.DelimitIdentifier(o.Column?.Table.Name ?? TableName)}.{DbContext.DelimitIdentifier(o.Name)}").ToList();
    }
    public string GetColumnName(IProperty property) => FindColumn(property)?.Name ?? property.Name;
    private IColumnBase FindColumn(IProperty property)
    {
        IEntityType entityType = property.DeclaringType switch
        {
            IEntityType declaringEntityType => declaringEntityType,
            IComplexType complexType when complexType.ComplexProperty.DeclaringType is IEntityType complexDeclaringEntityType => complexDeclaringEntityType,
            _ => EntityType
        };
        if (entityType == null || entityType.IsAbstract())
            entityType = EntityType;
        if (entityType.IsOwned())
            entityType = entityType.FindOwnership()?.PrincipalEntityType ?? EntityType;
        var storeObjectIdentifier = StoreObjectIdentifier.Table(entityType.GetTableName(), entityType.GetSchema());
        return property.FindColumn(storeObjectIdentifier);
    }

    private string FindTableName(IEntityType declaringEntityType, IEntityType entityType) =>
        declaringEntityType != null && declaringEntityType.IsAbstract() ? declaringEntityType.GetTableName() : entityType.GetTableName();
    private static bool IsWritableDataProperty(IProperty property)
    {
        if (property.GetComputedColumnSql() != null)
            return false;
        if (property.ValueGenerated == ValueGenerated.Never)
            return true;
        if (property.ValueGenerated != ValueGenerated.OnAdd)
            return false;
        if (property.IsPrimaryKey())
        {
            Type clrType = Nullable.GetUnderlyingType(property.ClrType) ?? property.ClrType;
            return clrType == typeof(Guid) && property.GetDefaultValueSql() == null;
        }
        return property.GetDefaultValueSql() != null || property.GetDefaultValue() != null;
    }
    public IEnumerable<string> GetColumnNames(IEntityType entityType, bool primaryKeyColumns)
    {
        List<string> columns;
        if (entityType != null)
        {
            columns = entityType.GetProperties().Where(o => (o.GetDeclaringEntityType() == entityType || o.GetDeclaringEntityType().IsAbstract()
                    || o.IsForeignKeyToSelf()) && IsWritableDataProperty(o))
                .Select(GetColumnName).ToList();

            columns.AddRange(entityType.GetComplexProperties().SelectMany(o => o.ComplexType.GetProperties()
                .Where(IsWritableDataProperty)
                .Select(GetColumnName)));
            columns.AddRange(entityType.GetNavigations()
                .Where(n => n.ForeignKey.IsOwnership)
                .SelectMany(n => n.TargetEntityType.GetProperties()
                    .Where(p => !n.ForeignKey.Properties.Contains(p) && IsWritableDataProperty(p))
                    .Select(GetColumnName)));
        }
        else
        {
            columns = EntityType.GetProperties().Where(IsWritableDataProperty)
            .Select(GetColumnName).ToList();

            columns.AddRange(EntityType.GetComplexProperties().SelectMany(o => o.ComplexType.GetProperties()
                .Where(IsWritableDataProperty)
                .Select(GetColumnName)));
            columns.AddRange(EntityType.GetNavigations()
                .Where(n => n.ForeignKey.IsOwnership)
                .SelectMany(n => n.TargetEntityType.GetProperties()
                    .Where(p => !n.ForeignKey.Properties.Contains(p) && IsWritableDataProperty(p))
                    .Select(GetColumnName)));
        }
        if (primaryKeyColumns)
        {
            columns.AddRange(GetPrimaryKeyColumns());
        }
        return columns.Distinct();
    }
    public IEnumerable<string> GetColumns(bool includePrimaryKeyColumns = false)
    {
        List<string> columns = [];
        foreach (var entityType in EntityTypes)
        {
            var storeObjectIdentifier = StoreObjectIdentifier.Create(entityType, StoreObjectType.Table).GetValueOrDefault();
            columns.AddRange(entityType.GetProperties().Where(IsWritableDataProperty)
                .Select(GetColumnName));

            columns.AddRange(entityType.GetComplexProperties().SelectMany(o => o.ComplexType.GetProperties()
                .Where(IsWritableDataProperty)
                .Select(GetColumnName)));
            columns.AddRange(entityType.GetNavigations()
                .Where(n => n.ForeignKey.IsOwnership)
                .SelectMany(n => n.TargetEntityType.GetProperties()
                    .Where(p => !n.ForeignKey.Properties.Contains(p) && IsWritableDataProperty(p))
                    .Select(GetColumnName)));

            if (includePrimaryKeyColumns)
                columns.AddRange(GetPrimaryKeyColumns());
        }
        return columns.Where(o => o != null).Distinct();
    }
    public IEnumerable<string> GetPrimaryKeyColumns() =>
        EntityType.FindPrimaryKey()?.Properties.Select(GetColumnName) ?? [];

    internal IEnumerable<string> GetAutoGeneratedColumns(IEntityType entityType = null)
    {
        entityType ??= EntityType;
        return entityType.GetProperties().Where(o => o.ValueGenerated != ValueGenerated.Never)
            .Select(GetColumnName);
    }

    internal IEnumerable<IProperty> GetEntityProperties(IEntityType entityType = null, ValueGenerated? valueGenerated = null)
    {
        entityType ??= EntityType;
        return entityType.GetProperties().Where(o => valueGenerated == null || o.ValueGenerated == valueGenerated).AsEnumerable();
    }
    internal Func<object, object> GetValueFromProvider(IProperty property)
    {
        var valueConverter = property.GetTypeMapping().Converter;
        if (valueConverter != null)
            return value => value == DBNull.Value ? null : valueConverter.ConvertFromProvider(value);

        var clrType = Nullable.GetUnderlyingType(property.ClrType) ?? property.ClrType;
        return value =>
        {
            if (value == null || value == DBNull.Value)
                return null;
            if (clrType.IsInstanceOfType(value))
                return value;

            if (value is long longValue)
            {
                if (clrType == typeof(int)) return (int)longValue;
                if (clrType == typeof(short)) return (short)longValue;
                if (clrType == typeof(byte)) return (byte)longValue;
                if (clrType == typeof(decimal)) return longValue;
            }

            if (value is decimal decimalValue)
            {
                if (clrType == typeof(long)) return Convert.ToInt64(decimalValue);
                if (clrType == typeof(int)) return Convert.ToInt32(decimalValue);
                if (clrType == typeof(short)) return Convert.ToInt16(decimalValue);
                if (clrType == typeof(byte)) return Convert.ToByte(decimalValue);
            }

            if (clrType == typeof(Guid) && value is byte[] bytes)
                return new Guid(bytes);

            return Convert.ChangeType(value, clrType);
        };
    }
    internal IEnumerable<string> GetSchemaQualifiedTableNames()
    {
        return EntityTypes
            .Select(o => DbContext.DelimitIdentifier(o.GetTableName(), o.GetSchema() ?? DbContext.Database.GetDefaultSchema())).Distinct();
    }
}