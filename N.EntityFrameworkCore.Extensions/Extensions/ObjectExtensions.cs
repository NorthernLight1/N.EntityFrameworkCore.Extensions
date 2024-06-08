using System;
using System.Linq.Expressions;
using System.Reflection;

namespace N.EntityFrameworkCore.Extensions
{
    internal static class ObjectExtensions
    {
        public static object GetPrivateFieldValue(this object obj, string propName)
        {
            if (obj == null) throw new ArgumentNullException(nameof(obj));
            Type t = obj.GetType();
            FieldInfo fieldInfo = null;
            PropertyInfo propertyInfo = null;
            while (fieldInfo == null && propertyInfo == null && t != null)
            {
                fieldInfo = t.GetField(propName, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
                if (fieldInfo == null)
                {
                    propertyInfo = t.GetProperty(propName, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
                }

                t = t.BaseType;
            }
            if (fieldInfo == null && propertyInfo == null)
                throw new ArgumentOutOfRangeException("propName", string.Format("Field {0} was not found in Type {1}", propName, obj.GetType().FullName));

            if (fieldInfo != null)
                return fieldInfo.GetValue(obj);

            return propertyInfo.GetValue(obj, null);
        }
    }
}