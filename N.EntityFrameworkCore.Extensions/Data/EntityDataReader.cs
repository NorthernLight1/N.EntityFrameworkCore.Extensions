using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.ValueGeneration;
using N.EntityFrameworkCore.Extensions.Common;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;

namespace N.EntityFrameworkCore.Extensions
{
    internal class EntityDataReader<T> : IDataReader
    {
        public TableMapping TableMapping { get; set; }
        public Dictionary<long,T> EntityMap { get; set; }
        private Dictionary<string, int> columnIndexes;
        private int currentId;
        private bool useInternalId;
        private int tableFieldCount;
        private IEnumerable<T> entities;
        private IEnumerator<T> enumerator;
        private Dictionary<int, Func<EntityEntry, object>> selectors;

        public EntityDataReader(TableMapping tableMapping, IEnumerable<T> entities, bool useInternalId)
        {
            this.columnIndexes = new Dictionary<string, int>();
            this.currentId = 0;
            this.useInternalId = useInternalId;
            this.tableFieldCount = tableMapping.Properties.Length;
            this.entities = entities;
            this.enumerator = entities.GetEnumerator();
            this.selectors = new Dictionary<int, Func<EntityEntry, object>>();
            this.EntityMap = new Dictionary<long, T>();
            this.FieldCount = tableMapping.Properties.Length;
            this.TableMapping = tableMapping;
            

            int i = 0;
            foreach (var property in tableMapping.Properties)
            {
                var valueGeneratorFactory = property.GetValueGeneratorFactory();
                if (valueGeneratorFactory != null)
                {
                    var valueGenerator = valueGeneratorFactory.Invoke(property, this.TableMapping.EntityType);
                    Func<EntityEntry, object> selector = entry => valueGenerator.Next(entry);
                    selectors[i] = selector;
                }
                else
                {
                    var valueConverter = property.GetValueConverter();
                    if (valueConverter != null)
                    {
                        selectors[i] = entry => valueConverter.ConvertToProvider(entry.CurrentValues[property]);
                    }
                    else
                    {
                        selectors[i] = entry => entry.CurrentValues[property];
                    }
                }
                columnIndexes[property.Name] = i;
                i++;
            }
            
            if(useInternalId)
            {
                this.FieldCount++;
                columnIndexes[Constants.InternalId_ColumnName] = i;
            }
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
            throw new NotImplementedException();
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
            if(i == tableFieldCount)
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
            return entity is InternalEntityEntry ? ((InternalEntityEntry)entity).ToEntityEntry() : this.TableMapping.DbContext.Entry(entity);
        }

        public int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public bool IsDBNull(int i)
        {
            throw new NotImplementedException();
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
}

