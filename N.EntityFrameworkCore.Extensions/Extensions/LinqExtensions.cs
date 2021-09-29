using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace N.EntityFrameworkCore.Extensions
{
    static class LinqExtensions
    {
        internal static string GetExpressionValueAsString(MemberBinding binding)
        {
            return GetExpressionValueAsString(binding.GetPrivateFieldValue("Expression") as Expression);
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
                var leftExpression = expression.GetPrivateFieldValue("Left") as Expression;
                var rightExpression = expression.GetPrivateFieldValue("Right") as Expression;
                string leftValue = GetExpressionValueAsString(leftExpression);
                string rightValue = GetExpressionValueAsString(rightExpression);
                string joinValue = string.Empty;
                switch (expression.NodeType)
                {
                    case ExpressionType.Add:
                        joinValue = "+";
                        break;
                    case ExpressionType.Subtract:
                        joinValue = "-";
                        break;
                    case ExpressionType.Multiply:
                        joinValue = "*";
                        break;
                    case ExpressionType.Divide:
                        joinValue = "/";
                        break;
                    case ExpressionType.Modulo:
                        joinValue = "%";
                        break;
                }
                return string.Format("({0} {1} {2})", leftValue, joinValue, rightValue);
            }
        }

        private static string ConvertToSqlValue(object value)
        {
            if (value == null)
                return "NULL";
            if (value is string str)
                return "'" + str.Replace("'", "''") + "'";
            if (value is bool b)
                return b ? "1" : "0";
            if (value is DateTime dt)
                return "'" + dt.ToString("yyyy-MM-ddTHH:mm:ss.fff") + "'"; // Convert to ISO-8601
            if (!value.GetType().IsClass)
                return Convert.ToString(value, CultureInfo.InvariantCulture);

            throw new NotImplementedException("Unhandled data type.");
        }

        public static List<string> GetObjectProperties<T>(this Expression<Func<T, object>> expression)
        {
            return expression == null ? new List<string>() : expression.Body.Type.GetProperties().Select(o => o.Name).ToList();
        }
        internal static string ToSqlPredicate<T>(this Expression<T> expression, params string[] parameters)
        {
            var stringBuilder = new StringBuilder((string)expression.Body.GetPrivateFieldValue("DebugView"));
            int i = 0;
            foreach (var expressionParam in expression.Parameters)
            {
                if (parameters.Length <= i) break;
                stringBuilder.Replace((string)expressionParam.GetPrivateFieldValue("DebugView"), parameters[i]);
                i++;
            }
            stringBuilder.Replace("&&", "AND");
            stringBuilder.Replace("==", "=");
            return stringBuilder.ToString();
        }
        internal static string ToSqlUpdateSetExpression<T>(this Expression<T> expression, string tableName)
        {
            List<string> setValues = new List<string>();
            var memberInitExpression = expression.Body as MemberInitExpression;
            foreach (var binding in memberInitExpression.Bindings)
            {
                string expValue = GetExpressionValueAsString(binding);
                expValue = expValue.Replace(string.Format("{0}.", expression.Parameters.First().Name), "");
                setValues.Add(string.Format("[{0}]={1}", binding.Member.Name, expValue));
            }
            return string.Join(",", setValues);
        }
    }
}
