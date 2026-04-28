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
        ColumnMap = Properties.Select(p => new KeyValuePair<string, IProperty>(GetColumnName(p), p)).ToDictionary();
        Schema = entityType.GetSchema() ?? dbContext.Database.GetDefaultSchema();
        TableName = entityType.GetTableName();
        EntityTypes = EntityType.GetAllBaseTypesInclusive().Where(o => !o.IsAbstract());
    }
    public IProperty GetPropertyFromColumnName(string columnName) => ColumnMap[columnName];
    private static IProperty[] GetProperties(IEntityType entityType)
    {
        var properties = entityType.GetProperties().ToList();
        properties.AddRange(entityType.GetComplexProperties().SelectMany(p => p.ComplexType.GetProperties()));
        return properties.ToArray();
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
        var entityType = property.GetDeclaringEntityType();
        if (entityType == null || entityType.IsAbstract())
            entityType = EntityType;
        var storeObjectIdentifier = StoreObjectIdentifier.Table(entityType.GetTableName(), entityType.GetSchema());
        return property.FindColumn(storeObjectIdentifier);
    }

    private string FindTableName(IEntityType declaringEntityType, IEntityType entityType) =>
        declaringEntityType != null && declaringEntityType.IsAbstract() ? declaringEntityType.GetTableName() : entityType.GetTableName();
    public IEnumerable<string> GetColumnNames(IEntityType entityType, bool primaryKeyColumns)
    {
        List<string> columns;
        if (entityType != null)
        {
            columns = entityType.GetProperties().Where(o => (o.GetDeclaringEntityType() == entityType || o.GetDeclaringEntityType().IsAbstract()
                    || o.IsForeignKeyToSelf()) && o.ValueGenerated == ValueGenerated.Never)
                .Select(GetColumnName).ToList();

            columns.AddRange(entityType.GetComplexProperties().SelectMany(o => o.ComplexType.GetProperties()
                .Select(GetColumnName)));
        }
        else
        {
            columns = EntityType.GetProperties().Where(o => o.ValueGenerated == ValueGenerated.Never)
            .Select(GetColumnName).ToList();

            columns.AddRange(EntityType.GetComplexProperties().SelectMany(o => o.ComplexType.GetProperties()
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
            columns.AddRange(entityType.GetProperties().Where(o => o.ValueGenerated == ValueGenerated.Never)
                .Select(GetColumnName));

            columns.AddRange(EntityType.GetComplexProperties().SelectMany(o => o.ComplexType.GetProperties()
                .Select(GetColumnName)));

            if (includePrimaryKeyColumns)
                columns.AddRange(GetPrimaryKeyColumns());
        }
        return columns.Where(o => o != null).Distinct();
    }
    public IEnumerable<string> GetPrimaryKeyColumns() =>
        EntityType.FindPrimaryKey().Properties.Select(GetColumnName);

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
        return valueConverter != null ? value => valueConverter.ConvertFromProvider(value) : value => value;
    }
    internal IEnumerable<string> GetSchemaQualifiedTableNames()
    {
        return EntityTypes
            .Select(o => DbContext.DelimitIdentifier(o.GetTableName(), o.GetSchema() ?? DbContext.Database.GetDefaultSchema())).Distinct();
    }
}
