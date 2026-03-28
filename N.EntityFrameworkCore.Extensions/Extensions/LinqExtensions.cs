using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace N.EntityFrameworkCore.Extensions;

internal static class LinqExtensions
{
    public static List<string> GetObjectProperties<T>(this Expression<Func<T, object>> expression)
    {
        if (expression == null)
        {
            return [];
        }
        else if (expression.Body is MemberExpression propertyExpression)
        {
            return [propertyExpression.Member.Name];
        }
        else if (expression.Body is NewExpression newExpression)
        {
            return newExpression.Members.Select(o => o.Name).ToList();
        }
        else if ((expression.Body is UnaryExpression unaryExpression) && (unaryExpression.Operand.GetPrivateFieldValue("Member") is PropertyInfo propertyInfo))
        {
            return [propertyInfo.Name];
        }
        else
        {
            throw new InvalidOperationException("GetObjectProperties() encountered an unsupported expression type");
        }
    }
    internal static string ToSql(this ExpressionType expressionType) => expressionType switch
    {
        ExpressionType.AndAlso => "AND",
        ExpressionType.Or => "OR",
        ExpressionType.Add => "+",
        ExpressionType.Subtract => "-",
        ExpressionType.Multiply => "*",
        ExpressionType.Divide => "/",
        ExpressionType.Modulo => "%",
        ExpressionType.Equal => "=",
        _ => string.Empty
    };

    internal static string ToSql(this MemberBinding binding)
    {
        if (binding is MemberAssignment memberAssingment)
        {
            return GetExpressionValueAsString(memberAssingment.Expression);
        }
        else
        {
            throw new NotSupportedException();
        }
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
        else if (expression is UnaryExpression unaryExpression)
        {
            return $"{unaryExpression.Operand}";
        }
        return sb.ToString();
    }
    internal static string GetExpressionValueAsString(Expression expression)
    {
        if (expression is ConstantExpression constantExpression)
        {
            return ConvertToSqlValue(constantExpression.Value);
        }
        else if (expression is MemberExpression memberExpression)
        {
            if (memberExpression.Expression is ParameterExpression parameterExpression)
            {
                return memberExpression.ToString();
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
            List<string> argValues = [];
            foreach (var argument in methodCallExpression.Arguments)
            {
                argValues.Add(GetExpressionValueAsString(argument));
            }
            return methodCallExpression.Method.Name switch
            {
                "ToString" => $"CONVERT(VARCHAR,{argValues[0]})",
                _ => $"{methodCallExpression.Method.Name}({string.Join(",", argValues)})"
            };
        }
        else
        {
            var binaryExpression = expression as BinaryExpression;
            string leftValue = GetExpressionValueAsString(binaryExpression.Left);
            string rightValue = GetExpressionValueAsString(binaryExpression.Right);
            string joinValue = expression.NodeType.ToSql();

            return $"({leftValue} {joinValue} {rightValue})";
        }
    }
    internal static string ToSqlPredicate2<T>(this Expression<T> expression, params string[] parameters)
    {
        var sql = ToSqlString(expression.Body);

        for (var i = 0; i < parameters.Length; i++)
            sql = sql.Replace($"${expression.Parameters[i].Name!}.", $"{parameters[i]}.");

        return sql;
    }
    internal static string ToSqlPredicate<T>(this Expression<T> expression, params string[] parameters)
    {
        var expressionBody = (string)expression.Body.GetPrivateFieldValue("DebugView");
        expressionBody = expressionBody.Replace(System.Environment.NewLine, " ");
        var stringBuilder = new StringBuilder(expressionBody);

        int i = 0;
        foreach (var expressionParam in expression.Parameters)
        {
            if (parameters.Length <= i) break;
            stringBuilder.Replace((string)expressionParam.GetPrivateFieldValue("DebugView"), parameters[i]);
            i++;
        }
        stringBuilder.Replace("== null", "IS NULL");
        stringBuilder.Replace("!= null", "IS NOT NULL");
        stringBuilder.Replace("&&", "AND");
        stringBuilder.Replace("==", "=");
        stringBuilder.Replace("||", "OR");
        stringBuilder.Replace("(System.Nullable`1[System.Int32])", "");
        stringBuilder.Replace("(System.Int32)", "");
        return stringBuilder.ToString();
    }
    internal static string ToSqlUpdateSetExpression<T>(this Expression<T> expression, string tableName)
    {
        List<string> setValues = [];
        var memberInitExpression = expression.Body as MemberInitExpression;
        foreach (var binding in memberInitExpression.Bindings)
        {
            string expValue = binding.ToSql();
            expValue = expValue.Replace($"{expression.Parameters.First().Name}.", "");
            setValues.Add($"[{binding.Member.Name}]={expValue}");
        }
        return string.Join(",", setValues);
    }
    private static string ToSqlString(Expression expression, string sql = null)
    {
        sql ??= "";
        if (expression is not BinaryExpression b)
            return sql;

        var sb = new StringBuilder();
        if (b.Left is MemberExpression mel)
            sb.Append($"${mel} = ");
        if (b.Right is MemberExpression mer)
            sb.Append($"${mer}");

        if (b.Left is UnaryExpression ubl)
            sb.Append($"${ubl.Operand} = ");
        if (b.Right is UnaryExpression ubr)
            sb.Append($"${ubr.Operand}");

        if (sb.Length > 0)
            return sb.ToString();

        var left = ToSqlString(b.Left, sql);
        if (string.IsNullOrWhiteSpace(left))
            return sql;

        var right = ToSqlString(b.Right, sql);
        return $"{left} AND {right}";
    }
    private static string ConvertToSqlValue(object value)
    {
        if (value == null)
            return "NULL";
        if (value is string str)
            return $"'{str.Replace("'", "''")}'";
        if (value is Guid guid)
            return $"'{guid}'";
        if (value is bool b)
            return b ? "1" : "0";
        if (value is DateTime dt)
            return $"'{dt:yyyy-MM-ddTHH:mm:ss.fffffff}'"; // Convert to ISO-8601
        if (value is DateTimeOffset dto)
            return $"'{dto:yyyy-MM-ddTHH:mm:ss.fffffffzzzz}'"; // Convert to ISO-8601
        var valueType = value.GetType();
        if (valueType.IsEnum)
            return Convert.ToString((int)value);
        if (!valueType.IsClass)
            return Convert.ToString(value, CultureInfo.InvariantCulture);

        throw new NotImplementedException("Unhandled data type.");
    }
}
