using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using System.Collections.Generic;
using System.Linq;

namespace N.EntityFrameworkCore.Extensions
{
    public class TableMapping
    {
        //public EntitySetMapping Mapping { get; set; }
        //public EntitySet EntitySet { get; set; }
        public IEntityType EntityType { get; set; }
        public IProperty[] Properties { get; }
        public List<ColumnMetaData> Columns { get; set; }
        public string Schema { get; }
        public string TableName { get; }
        public string FullQualifedTableName
        {
            get { return string.Format("[{0}].[{1}]", this.Schema, this.TableName);  }
        }

        public TableMapping(IEntityType entityType)
        {
            //var storeEntitySet = mapping.EntityTypeMappings.Single().Fragments.Single().StoreEntitySet;
            //Columns = columns;
            //EntitySet = entitySet;
            EntityType = entityType;
            //Mapping = mapping;
            //entityType.FindPrimaryKey().
            Properties = entityType.GetProperties().ToArray();
            Schema = entityType.GetSchema() ?? "dbo";
            TableName = entityType.GetTableName();
        }
        public IEnumerable<string> GetNonValueGeneratedColumns()
        {
            return EntityType.GetProperties().Where(o => o.ValueGenerated == ValueGenerated.Never).Select( o => o.Name);
        }
        public IEnumerable<string> GetPrimaryKeyColumns()
        {
            return EntityType.FindPrimaryKey().Properties.Select(o => o.Name);
        }
    }
    public class ColumnMetaData
    {
        public EFColumn2 Column { get; internal set; }
        public EFColumnProperty Property { get; internal set; }
    }

    public class EFColumn2
    {
        public bool IsStoreGeneratedIdentity { get; internal set; }
        public string Name { get; internal set; }
    }

    public class EFColumnProperty
    {
        public string Name { get; internal set; }
    }
}

