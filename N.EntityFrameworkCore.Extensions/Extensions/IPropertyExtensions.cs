using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace N.EntityFrameworkCore.Extensions.Extensions
{
    public static class IPropertyExtensions
    {
        public static IEntityType GetDeclaringEntityType(this IProperty property)
        {
            return property.DeclaringType as IEntityType;
        }
    }
}
