using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using N.EntityFrameworkCore.Extensions.Extensions;

namespace N.EntityFrameworkCore.Extensions
{
    public class TableMapping
    {
        public DbContext DbContext { get; private set; }
        public IEntityType EntityType { get; set; }
        public IProperty[] Properties { get; }
        public string Schema { get; }
        public string TableName { get; }
        public IEnumerable<IEntityType> EntityTypes { get; }

        public bool HasIdentityColumn => EntityType.FindPrimaryKey().Properties.Any(o => o.ValueGenerated != ValueGenerated.Never);
        public StoreObjectIdentifier StoreObjectIdentifier => StoreObjectIdentifier.Table(TableName, EntityType.GetSchema());
        private Dictionary<string, IProperty> ColumnMap { get; set; }
        public string FullQualifedTableName
        {
            get { return string.Format("[{0}].[{1}]", this.Schema, this.TableName); }
        }

        public TableMapping(DbContext dbContext, IEntityType entityType)
        {
            DbContext = dbContext;
            EntityType = entityType;
            Properties = GetProperties(entityType);
            ColumnMap = Properties.Select(p => new KeyValuePair<string, IProperty>(GetColumnName(p), p)).ToDictionary();
            Schema = entityType.GetSchema() ?? "dbo";
            TableName = entityType.GetTableName();
            EntityTypes = EntityType.GetAllBaseTypesInclusive().Where(o => !o.IsAbstract());
        }
        public IProperty GetPropertyFromColumnName(string columnName)
        {
            return ColumnMap[columnName];
        }
        private static IProperty[] GetProperties(IEntityType entityType)
        {
            var properties = entityType.GetProperties().ToList();
            properties.AddRange(entityType.GetComplexProperties().SelectMany(p => p.ComplexType.GetProperties()));
            return properties.ToArray();
        }

        public IEnumerable<string> GetQualifiedColumnNames(IEnumerable<string> columnNames, IEntityType entityType = null)
        {
            return Properties.Where(o => entityType == null || o.GetDeclaringEntityType() == entityType)
                .Select(o => FindColumn(o))
                .Where(o => columnNames == null || columnNames.Contains(o.Name))
                .Select(o => $"[{o.Table.Name}].[{o.Name}]").ToList();
        }
        public string GetColumnName(IProperty property)
        {
            return FindColumn(property).Name;
        }
        private IColumnBase FindColumn(IProperty property)
        {
            var entityType = property.GetDeclaringEntityType();
            if (entityType == null || entityType.IsAbstract())
                entityType = EntityType;
            var storeObjectIdentifier = StoreObjectIdentifier.Table(entityType.GetTableName(), entityType.GetSchema());
            return property.FindColumn(storeObjectIdentifier);
        }

        private string FindTableName(IEntityType declaringEntityType, IEntityType entityType)
        {
            return declaringEntityType != null && declaringEntityType.IsAbstract() ? declaringEntityType.GetTableName() : entityType.GetTableName();
        }
        public IEnumerable<string> GetColumnNames(IEntityType entityType, bool primaryKeyColumns)
        {
            List<string> columns;
            if (entityType != null)
            {
                columns = entityType.GetProperties().Where(o => (o.GetDeclaringEntityType() == entityType || o.GetDeclaringEntityType().IsAbstract()
                        || o.IsForeignKeyToSelf()) && o.ValueGenerated == ValueGenerated.Never)
                    .Select(o => GetColumnName(o)).ToList();

                columns.AddRange(entityType.GetComplexProperties().SelectMany(o => o.ComplexType.GetProperties()
                    .Select(c => GetColumnName(c))));
            }
            else
            {
                columns = EntityType.GetProperties().Where(o => o.ValueGenerated == ValueGenerated.Never)
                .Select(o => GetColumnName(o)).ToList();

                columns.AddRange(EntityType.GetComplexProperties().SelectMany(o => o.ComplexType.GetProperties()
                    .Select(c => GetColumnName(c))));
            }
            if (primaryKeyColumns)
            {
                columns.AddRange(GetPrimaryKeyColumns());
            }
            return columns.Distinct();
        }
        public IEnumerable<string> GetColumns(bool includePrimaryKeyColumns = false)
        {
            var columns = new List<string>();
            foreach (var entityType in EntityTypes)
            {
                var storeObjectIdentifier = StoreObjectIdentifier.Create(entityType, StoreObjectType.Table).GetValueOrDefault();
                columns.AddRange(entityType.GetProperties().Where(o => o.ValueGenerated == ValueGenerated.Never)
                    .Select(o => o.GetColumnName(storeObjectIdentifier)));

                columns.AddRange(EntityType.GetComplexProperties().SelectMany(o => o.ComplexType.GetProperties()
                    .Select(c => c.GetColumnName(storeObjectIdentifier))));

                if (includePrimaryKeyColumns)
                    columns.AddRange(GetPrimaryKeyColumns());
            }
            return columns.Where(o => o != null).Distinct();
        }
        public IEnumerable<string> GetPrimaryKeyColumns()
        {
            return EntityType.FindPrimaryKey().Properties.Select(o => o.GetColumnName(this.StoreObjectIdentifier));
        }

        internal IEnumerable<string> GetAutoGeneratedColumns(IEntityType entityType = null)
        {
            entityType = entityType ?? this.EntityType;
            return entityType.GetProperties().Where(o => o.ValueGenerated != ValueGenerated.Never)
                .Select(o => o.GetColumnName(this.StoreObjectIdentifier));
        }

        internal IEnumerable<IProperty> GetEntityProperties(IEntityType entityType = null, ValueGenerated? valueGenerated = null)
        {
            entityType = entityType ?? this.EntityType;
            return entityType.GetProperties().Where(o => valueGenerated == null || o.ValueGenerated == valueGenerated).AsEnumerable();
        }
        internal Func<object, object> GetValueFromProvider(IProperty property)
        {
            var valueConverter = property.GetTypeMapping().Converter;
            if (valueConverter != null)
            {
                return value => valueConverter.ConvertFromProvider(value);
            }
            else
            {
                return value => value;
            }
        }
        internal IEnumerable<Func<object, object>> GetValuesFromProvider()
        {
            var propertyGetters = new List<Func<object, object>>();
            foreach (var property in this.Properties)
            {
                propertyGetters.Add(value => GetValueFromProvider(property));
            }
            return propertyGetters.AsEnumerable();
        }

        internal IEnumerable<string> GetSchemaQualifiedTableNames()
        {
            return EntityTypes
                .Select(o => $"[{o.GetSchema() ?? "dbo"}].[{o.GetTableName()}]").Distinct();
        }
    }
}