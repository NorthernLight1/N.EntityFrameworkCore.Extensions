using Microsoft.IdentityModel.Protocols;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Net.NetworkInformation;
using System.Reflection;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace N.EntityFrameworkCore.Extensions
{
    static class LinqExtensions
    {
        static Dictionary<ExpressionType, string> sqlExpressionTypes = new()
        {
            { ExpressionType.AndAlso, "AND" },
            { ExpressionType.Or, "OR" },
            { ExpressionType.Add, "+" },
            { ExpressionType.Subtract, "-" },
            { ExpressionType.Multiply, "*" },
            { ExpressionType.Divide, "/" },
            { ExpressionType.Modulo, "%" },
            { ExpressionType.Equal, "=" }
        };

        internal static string ToSql(this MemberBinding binding)
        {
            if(binding is MemberAssignment memberAssingment)
            {
                return GetExpressionValueAsString(memberAssingment.Expression);
            }
            else
            {
                throw new NotSupportedException();
            }
        }
        internal static string GetExpressionValueAsString(Expression expression)
        {
            if (expression.NodeType == ExpressionType.Constant)
            {
                return ConvertToSqlValue((expression as ConstantExpression).Value);
            }
            else if (expression.NodeType == ExpressionType.MemberAccess)
            {
                if (expression.GetPrivateFieldValue("Expression") is ParameterExpression parameterExpression)
                {
                    return Expression.Lambda(expression).Body.ToString();
                }
                else
                {
                    return ConvertToSqlValue(Expression.Lambda(expression).Compile().DynamicInvoke());
                }
            }
            else if (expression.NodeType == ExpressionType.Convert)
            {
                return ConvertToSqlValue(Expression.Lambda(expression).Compile().DynamicInvoke());
            }
            else if (expression.NodeType == ExpressionType.Call)
            {
                var methodCallExpression = expression as MethodCallExpression;
                List<string> argValues = new List<string>();
                foreach (var argument in methodCallExpression.Arguments)
                {
                    argValues.Add(GetExpressionValueAsString(argument));
                }
                string methodFormat;
                switch (methodCallExpression.Method.Name)
                {
                    case "ToString":
                        methodFormat = string.Format("CONVERT(VARCHAR,{0})", argValues[0]);
                        break;
                    default:
                        methodFormat = string.Format("{0}({1})", methodCallExpression.Method.Name, string.Join(",", argValues));
                        break;
                }
                return methodFormat;
            }
            else
            {
                var binaryExpression = expression as BinaryExpression;
                string leftValue = GetExpressionValueAsString(binaryExpression.Left);
                string rightValue = GetExpressionValueAsString(binaryExpression.Right);
                string joinValue = expression.NodeType.ToSql();

                return string.Format("({0} {1} {2})", leftValue, joinValue, rightValue);
            }
        }

        private static string ConvertToSqlValue(object value)
        {
            if (value == null)
                return "NULL";
            if (value is string str)
                return "'" + str.Replace("'", "''") + "'";
            if (value is Guid guid)
                return $"'{guid}'";
            if (value is bool b)
                return b ? "1" : "0";
            if (value is DateTime dt)
                return "'" + dt.ToString("yyyy-MM-ddTHH:mm:ss.fffffff") + "'"; // Convert to ISO-8601
            if (value is DateTimeOffset dto)
                return "'" + dto.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzzz") + "'"; // Convert to ISO-8601
            var valueType = value.GetType();
            if (valueType.IsEnum)
                return Convert.ToString((int)value);
            if (!valueType.IsClass)
                return Convert.ToString(value, CultureInfo.InvariantCulture);

            throw new NotImplementedException("Unhandled data type.");
        }
        public static List<string> GetObjectProperties<T>(this Expression<Func<T, object>> expression)
        {
            if (expression == null)
            {
                return new List<string>();
            }
            else if (expression.Body is MemberExpression propertyExpression)
            {
                return new List<string>() { propertyExpression.Member.Name };
            }
            else if (expression.Body is NewExpression newExpression)
            {
                return newExpression.Members.Select(o => o.Name).ToList();
            }
            else if ((expression.Body is UnaryExpression unaryExpression) && (unaryExpression.Operand.GetPrivateFieldValue("Member") is PropertyInfo propertyInfo))
            {
                return new List<string>() { propertyInfo.Name };
            }
            else
            {
                throw new InvalidOperationException("GetObjectProperties() encountered an unsupported expression type");
            }
        }
        internal static string ToSqlPredicate<T>(this Expression<T> expression, params string[] parameters)
        {
            var sql = ToSqlString(expression.Body);

            for (var i = 0; i < parameters.Length; i++)
                sql = sql.Replace($"${expression.Parameters[i].Name!}.", $"{parameters[i]}.");

            return sql;
        }
       
        static string ToSqlString(Expression expression, string sql = null)
        {
            sql ??= "";
            if (expression is not BinaryExpression b)
                return sql;

            var internalSql = "";
            if (b.Left is MemberExpression mel)
                internalSql += $"${mel} = ";
            if (b.Right is MemberExpression mer)
                internalSql += $"${mer}";

            if (b.Left is UnaryExpression ubl)
                internalSql += $"${ubl.Operand} = ";
            if (b.Right is UnaryExpression ubr)
                internalSql += $"${ubr.Operand}";

            if (!string.IsNullOrWhiteSpace(internalSql))
                return internalSql;

            var left = ToSqlString(b.Left, sql);
            if (string.IsNullOrWhiteSpace(left))
                return sql;

            var right = ToSqlString(b.Right, sql);
            return left + " AND " + right;
        }
        internal static string ToSql(this ExpressionType expressionType)
        {
            string value = string.Empty;
            sqlExpressionTypes.TryGetValue(expressionType, out value);
            return value;
        }
        internal static string ToSql(this Expression expression)
        {
            var sb = new StringBuilder();
            if (expression is BinaryExpression binaryExpression)
            {
                sb.Append(binaryExpression.Left.ToSql());
                sb.Append($" {expression.NodeType.ToSql()} ");
                sb.Append(binaryExpression.Right.ToSql());
            }
            else if (expression is MemberExpression memberExpression)
            {
                return $"{memberExpression}";
            }
            else if(expression is UnaryExpression unaryExpression)
            {
                return $"{unaryExpression.Operand}";
            }
            return sb.ToString();
        }
        internal static string ToSqlUpdateSetExpression<T>(this Expression<T> expression, string tableName)
        {
            List<string> setValues = new List<string>();
            var memberInitExpression = expression.Body as MemberInitExpression;
            foreach (var binding in memberInitExpression.Bindings)
            {
                string expValue = binding.ToSql();
                expValue = expValue.Replace(string.Format("{0}.", expression.Parameters.First().Name), "");
                setValues.Add(string.Format("[{0}]={1}", binding.Member.Name, expValue));
            }
            return string.Join(",", setValues);
        }
    }
}
