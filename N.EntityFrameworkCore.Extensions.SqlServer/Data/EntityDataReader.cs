using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using N.EntityFrameworkCore.Extensions.Common;

namespace N.EntityFrameworkCore.Extensions;

internal sealed class EntityDataReader<T> : IDataReader
{
    public TableMapping TableMapping { get; set; }
    public Dictionary<long, T> EntityMap { get; set; }
    private Dictionary<string, int> columnIndexes;
    private int currentId;
    private bool useInternalId;
    private int tableFieldCount;
    private IEnumerable<T> entities;
    private IEnumerator<T> enumerator;
    private Dictionary<int, Func<EntityEntry, object>> selectors;

    public EntityDataReader(TableMapping tableMapping, IEnumerable<T> entities, bool useInternalId)
    {
        this.columnIndexes = [];
        this.currentId = 0;
        this.useInternalId = useInternalId;
        this.tableFieldCount = tableMapping.Properties.Length;
        this.entities = entities;
        this.enumerator = entities.GetEnumerator();
        this.selectors = [];
        this.EntityMap = [];
        this.FieldCount = tableMapping.Properties.Length;
        this.TableMapping = tableMapping;


        int i = 0;
        foreach (var property in tableMapping.Properties)
        {
            selectors[i] = GetValueSelector(property);
            columnIndexes[tableMapping.GetColumnName(property)] = i;
            i++;
        }

        if (useInternalId)
        {
            this.FieldCount++;
            columnIndexes[Constants.InternalId_ColumnName] = i;
        }
    }
    private Func<EntityEntry, object> GetValueSelector(IProperty property)
    {
        Func<EntityEntry, object> selector;
        var valueGeneratorFactory = property.GetValueGeneratorFactory();
        if (valueGeneratorFactory != null)
        {
            var valueGenerator = valueGeneratorFactory.Invoke(property, this.TableMapping.EntityType);
            selector = entry => valueGenerator.Next(entry);
        }
        else
        {
            var typeMapping = property.GetTypeMapping();
            var valueConverter = typeMapping.Converter;
            if (typeof(Geometry).IsAssignableFrom(property.ClrType))
            {
                var isGeography = string.Equals(property.GetColumnType(), "geography", StringComparison.OrdinalIgnoreCase);
                var spatialWriter = new SqlServerBytesWriter { IsGeography = isGeography };
                selector = entry =>
                {
                    object value = entry.CurrentValues[property];
                    if (value == null && property.PropertyInfo != null)
                        value = property.PropertyInfo.GetValue(entry.Entity);
                    if (value is not Geometry geometry)
                        return null;
                    if (valueConverter != null)
                    {
                        var providerValue = valueConverter.ConvertToProvider(geometry);
                        if (providerValue is byte[] bytes)
                            return bytes;
                    }
                    return spatialWriter.Write(geometry);
                };
            }
            else if (valueConverter != null)
            {
                selector = entry =>
                {
                    object value = entry.CurrentValues[property];
                    if (value == null && property.PropertyInfo != null)
                        value = property.PropertyInfo.GetValue(entry.Entity);
                    return valueConverter.ConvertToProvider(value);
                };
            }
            else
            {
                if (property.DeclaringType is IComplexType complexType)
                {
                    selector = entry => entry.ComplexProperty(complexType.ComplexProperty).Property(property).CurrentValue;
                }
                else if (property.DeclaringType is IEntityType declaringEntityType && declaringEntityType.IsOwned())
                {
                    var ownershipNavigation = declaringEntityType.FindOwnership()?.PrincipalToDependent
                        ?? TableMapping.EntityType.GetNavigations()
                            .FirstOrDefault(n => n.ForeignKey.IsOwnership && n.TargetEntityType == declaringEntityType);
                    if (ownershipNavigation != null)
                    {
                        selector = entry =>
                        {
                            object ownedInstance = ownershipNavigation.PropertyInfo != null
                                ? ownershipNavigation.PropertyInfo.GetValue(entry.Entity)
                                : entry.Reference(ownershipNavigation.Name).CurrentValue;
                            if (ownedInstance == null)
                                return null;
                            if (property.PropertyInfo != null)
                                return property.PropertyInfo.GetValue(ownedInstance);
                            return property.GetGetter()?.GetClrValue(ownedInstance);
                        };
                    }
                    else
                    {
                        selector = entry => entry.CurrentValues[property];
                    }
                }
                else
                {
                    selector = entry => entry.CurrentValues[property];
                }
            }
        }
        return selector;
    }
    public object this[int i] => throw new NotImplementedException();

    public object this[string name] => throw new NotImplementedException();

    public int Depth { get; set; }

    public bool IsClosed => throw new NotImplementedException();

    public int RecordsAffected => throw new NotImplementedException();

    public int FieldCount { get; set; }

    public void Close()
    {
        throw new NotImplementedException();
    }

    public void Dispose()
    {
        selectors = null;
        enumerator.Dispose();
    }

    public bool GetBoolean(int i)
    {
        throw new NotImplementedException();
    }

    public byte GetByte(int i)
    {
        throw new NotImplementedException();
    }

    public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
    {
        throw new NotImplementedException();
    }

    public char GetChar(int i)
    {
        throw new NotImplementedException();
    }

    public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
    {
        throw new NotImplementedException();
    }

    public IDataReader GetData(int i)
    {
        throw new NotImplementedException();
    }

    public string GetDataTypeName(int i)
    {
        throw new NotImplementedException();
    }

    public DateTime GetDateTime(int i)
    {
        throw new NotImplementedException();
    }

    public decimal GetDecimal(int i)
    {
        throw new NotImplementedException();
    }

    public double GetDouble(int i)
    {
        throw new NotImplementedException();
    }

    public Type GetFieldType(int i)
    {
        if (i == tableFieldCount)
            return typeof(int);

        var property = TableMapping.Properties[i];
        if (typeof(Geometry).IsAssignableFrom(property.ClrType))
            return typeof(byte[]);
        return property.GetTypeMapping().Converter?.ProviderClrType ?? property.ClrType;
    }

    public float GetFloat(int i)
    {
        throw new NotImplementedException();
    }

    public Guid GetGuid(int i)
    {
        throw new NotImplementedException();
    }

    public short GetInt16(int i)
    {
        throw new NotImplementedException();
    }

    public int GetInt32(int i)
    {
        throw new NotImplementedException();
    }

    public long GetInt64(int i)
    {
        throw new NotImplementedException();
    }

    public string GetName(int i)
    {
        throw new NotImplementedException();
    }

    public int GetOrdinal(string name)
    {
        return columnIndexes[name];
    }

    public DataTable GetSchemaTable()
    {
        throw new NotImplementedException();
    }

    public string GetString(int i)
    {
        throw new NotImplementedException();
    }

    public object GetValue(int i)
    {
        if (i == tableFieldCount)
        {
            return this.currentId;
        }
        else
        {
            return selectors[i](FindEntry(enumerator.Current));
        }

    }

    private EntityEntry FindEntry(object entity)
    {
        return entity is InternalEntityEntry internalEntry ? internalEntry.ToEntityEntry() : TableMapping.DbContext.Entry(entity);
    }

    public int GetValues(object[] values)
    {
        throw new NotImplementedException();
    }

    public bool IsDBNull(int i)
    {
        var value = GetValue(i);
        return value == null || value == DBNull.Value;
    }

    public bool NextResult()
    {
        throw new NotImplementedException();
    }

    public bool Read()
    {
        bool moveNext = enumerator.MoveNext();

        if (moveNext && this.useInternalId)
        {
            this.currentId++;
            this.EntityMap.Add(this.currentId, enumerator.Current);
        }
        return moveNext;
    }
}