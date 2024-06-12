using Microsoft.EntityFrameworkCore.Metadata;

namespace N.EntityFrameworkCore.Extensions.Extensions;

public static class IPropertyExtensions
{
    public static IEntityType GetDeclaringEntityType(this IProperty property)
    {
            return property.DeclaringType as IEntityType;
        }
}