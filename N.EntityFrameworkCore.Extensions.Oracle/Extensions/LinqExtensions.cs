using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.EntityFrameworkCore;
using N.EntityFrameworkCore.Extensions.Util;

namespace N.EntityFrameworkCore.Extensions;

internal static class LinqExtensions
{
    internal static List<string> GetObjectProperties<T>(this Expression<Func<T, object>> expression)
    {
        if (expression == null)
            return [];

        return UnwrapConvert(expression.Body) switch
        {
            MemberExpression memberExpression => [memberExpression.Member.Name],
            NewExpression newExpression when newExpression.Members != null => newExpression.Members.Select(o => o.Name).ToList(),
            MemberInitExpression memberInitExpression => memberInitExpression.Bindings.OfType<MemberAssignment>().Select(b => b.Member.Name).ToList(),
            _ => throw new InvalidOperationException("GetObjectProperties() encountered an unsupported expression type")
        };
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

    internal static string ToSql(this MemberBinding binding) => binding switch
    {
        MemberAssignment memberAssignment => GetExpressionValueAsString(memberAssignment.Expression),
        _ => throw new NotSupportedException()
    };

    internal static string GetExpressionValueAsString(Expression expression) => expression switch
    {
        ConstantExpression constantExpression => ConvertToSqlValue(constantExpression.Value),
        MemberExpression { Expression: ParameterExpression } memberExpression => memberExpression.ToString(),
        MemberExpression => ConvertToSqlValue(Expression.Lambda(expression).Compile().DynamicInvoke()),
        { NodeType: ExpressionType.Convert } => ConvertToSqlValue(Expression.Lambda(expression).Compile().DynamicInvoke()),
        MethodCallExpression methodCallExpression => FormatMethodCallExpression(methodCallExpression),
        BinaryExpression binaryExpression => FormatBinaryExpression(binaryExpression),
        _ => throw new NotSupportedException($"Unsupported expression type: {expression.NodeType}")
    };

    internal static string ToSqlPredicate<T>(this Expression<T> expression, params string[] parameters)
    {
        var paramMap = new Dictionary<ParameterExpression, string>();
        for (int i = 0; i < expression.Parameters.Count && i < parameters.Length; i++)
            paramMap[expression.Parameters[i]] = parameters[i];

        return ExpressionToSqlPredicate(expression.Body, paramMap);
    }

    internal static string ToSqlPredicate<T>(this Expression<T> expression, DbContext dbContext, params string[] parameters)
    {
        string predicate = expression.ToSqlPredicate(parameters);
        return DelimitMemberAccess(dbContext, predicate);
    }

    internal static IEnumerable<string> GetPredicatePropertyNames<T>(this Expression<Func<T, T, bool>> expression)
    {
        var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (expression?.Body != null)
            CollectPredicatePropertyNames(expression.Body, columns);
        return columns;
    }

    private static void CollectPredicatePropertyNames(Expression expression, HashSet<string> columns)
    {
        expression = UnwrapConvert(expression);
        switch (expression)
        {
            case BinaryExpression binary:
                CollectPredicatePropertyNames(binary.Left, columns);
                CollectPredicatePropertyNames(binary.Right, columns);
                break;
            case MemberExpression member:
                columns.Add(member.Member.Name);
                break;
            case UnaryExpression { NodeType: ExpressionType.Not } unary:
                CollectPredicatePropertyNames(unary.Operand, columns);
                break;
        }
    }

    private static string ExpressionToSqlPredicate(Expression expression, IReadOnlyDictionary<ParameterExpression, string> paramMap)
    {
        expression = UnwrapConvert(expression);
        return expression switch
        {
            BinaryExpression binary => BinaryExpressionToSqlPredicate(binary, paramMap),
            MemberExpression member => MemberExpressionToSqlPredicate(member, paramMap),
            ConstantExpression { Value: null } => "NULL",
            ConstantExpression constant => ConvertToSqlValue(constant.Value),
            MethodCallExpression method when IsEqualsMethod(method) => EqualsMethodToSqlPredicate(method, paramMap),
            UnaryExpression { NodeType: ExpressionType.Not } unary =>
                $"NOT ({ExpressionToSqlPredicate(unary.Operand, paramMap)})",
            _ => throw new NotSupportedException($"Unsupported expression type in ToSqlPredicate: {expression.NodeType}")
        };
    }

    private static string MemberExpressionToSqlPredicate(MemberExpression member, IReadOnlyDictionary<ParameterExpression, string> paramMap)
    {
        var segments = new Stack<string>();
        Expression current = member;
        while (current is MemberExpression memberExpression)
        {
            segments.Push(memberExpression.Member.Name);
            current = memberExpression.Expression;
        }

        if (current is not ParameterExpression parameter)
            throw new NotSupportedException($"Unsupported member expression in ToSqlPredicate: {member}");

        return $"{paramMap[parameter]}.{string.Join('.', segments)}";
    }

    private static bool IsEqualsMethod(MethodCallExpression method) =>
        (method.Method.Name == nameof(object.Equals) && method.Method.DeclaringType == typeof(object))
        || (method.Method.Name == nameof(string.Equals) && method.Method.DeclaringType == typeof(string));

    private static string EqualsMethodToSqlPredicate(MethodCallExpression method, IReadOnlyDictionary<ParameterExpression, string> paramMap)
    {
        Expression left = method.Object ?? method.Arguments[0];
        Expression right = method.Object != null ? method.Arguments[0] : method.Arguments[1];

        if (IsNullConstant(right))
            return $"{ExpressionToSqlPredicate(left, paramMap)} IS NULL";
        if (IsNullConstant(left))
            return $"{ExpressionToSqlPredicate(right, paramMap)} IS NULL";

        string leftSql = ExpressionToSqlPredicate(left, paramMap);
        string rightSql = ExpressionToSqlPredicate(right, paramMap);
        if (IsNullableEquality(left, right))
            return $"({leftSql} = {rightSql} OR ({leftSql} IS NULL AND {rightSql} IS NULL))";
        return $"{leftSql} = {rightSql}";
    }

    private static string BinaryExpressionToSqlPredicate(BinaryExpression binary, IReadOnlyDictionary<ParameterExpression, string> paramMap)
    {
        if (binary.NodeType == ExpressionType.Equal)
        {
            if (IsNullConstant(binary.Right))
                return $"{ExpressionToSqlPredicate(binary.Left, paramMap)} IS NULL";
            if (IsNullConstant(binary.Left))
                return $"{ExpressionToSqlPredicate(binary.Right, paramMap)} IS NULL";

            string left = ExpressionToSqlPredicate(binary.Left, paramMap);
            string right = ExpressionToSqlPredicate(binary.Right, paramMap);
            if (IsNullableEquality(binary.Left, binary.Right))
                return $"({left} = {right} OR ({left} IS NULL AND {right} IS NULL))";
            return $"{left} = {right}";
        }

        if (binary.NodeType == ExpressionType.NotEqual)
        {
            if (IsNullConstant(binary.Right))
                return $"{ExpressionToSqlPredicate(binary.Left, paramMap)} IS NOT NULL";
            if (IsNullConstant(binary.Left))
                return $"{ExpressionToSqlPredicate(binary.Right, paramMap)} IS NOT NULL";
        }

        string leftSql = ExpressionToSqlPredicate(binary.Left, paramMap);
        string rightSql = ExpressionToSqlPredicate(binary.Right, paramMap);
        string op = binary.NodeType switch
        {
            ExpressionType.AndAlso => "AND",
            ExpressionType.OrElse => "OR",
            ExpressionType.NotEqual => "<>",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            _ => binary.NodeType.ToSql()
        };

        bool wrapLeft = binary.NodeType is ExpressionType.AndAlso && NeedsParenthesesForAnd(binary.Left);
        bool wrapRight = binary.NodeType is ExpressionType.AndAlso && NeedsParenthesesForAnd(binary.Right);
        if (wrapLeft) leftSql = $"({leftSql})";
        if (wrapRight) rightSql = $"({rightSql})";
        return $"{leftSql} {op} {rightSql}";
    }

    private static bool NeedsParenthesesForAnd(Expression expression)
    {
        expression = UnwrapConvert(expression);
        return expression is BinaryExpression { NodeType: ExpressionType.OrElse };
    }

    private static bool IsNullableEquality(Expression left, Expression right)
    {
        left = UnwrapConvert(left);
        right = UnwrapConvert(right);
        return IsNullableExpressionType(left) || IsNullableExpressionType(right);
    }

    private static bool IsNullableExpressionType(Expression expression)
    {
        expression = UnwrapConvert(expression);
        Type type = expression.Type;
        return !type.IsValueType || Nullable.GetUnderlyingType(type) != null;
    }

    private static bool IsNullConstant(Expression expression)
    {
        expression = UnwrapConvert(expression);
        return expression is ConstantExpression { Value: null };
    }

    private static Expression UnwrapConvert(Expression expression)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked or ExpressionType.Quote } unary)
            expression = unary.Operand;
        return expression;
    }

    internal static string ToSqlUpdateSetExpression<T>(this Expression<T> expression, string tableName)
    {
        var memberInitExpression = (MemberInitExpression)expression.Body;
        var parameterName = expression.Parameters.First().Name;
        var sb = new StringBuilder();
        foreach (var binding in memberInitExpression.Bindings)
        {
            if (sb.Length > 0)
                sb.Append(',');
            string expValue = binding.ToSql().Replace($"{parameterName}.", "");
            sb.Append('"').Append(binding.Member.Name).Append("\"=").Append(expValue);
        }
        return sb.ToString();
    }

    internal static string ToSqlUpdateSetExpression<T>(this Expression<T> expression, DbContext dbContext, string tableName)
    {
        var memberInitExpression = (MemberInitExpression)expression.Body;
        var parameterName = expression.Parameters.First().Name;
        var sb = new StringBuilder();
        foreach (var binding in memberInitExpression.Bindings)
        {
            if (sb.Length > 0)
                sb.Append(',');
            string expValue = binding.ToSql().Replace($"{parameterName}.", "");
            expValue = DelimitMemberAccess(dbContext, expValue);
            sb.Append(dbContext.DelimitIdentifier(binding.Member.Name)).Append('=').Append(expValue);
        }
        return sb.ToString();
    }

    internal static string ToSqlConstantUpdateSetExpression<T>(this Expression<Func<T, object>> expression, DbContext dbContext, string tableAlias = null)
    {
        if (expression == null)
            throw new ArgumentNullException(nameof(expression));

        string prefix = string.IsNullOrEmpty(tableAlias) ? string.Empty : $"{tableAlias}.";

        if (expression.Body is MemberInitExpression)
            return expression.ToSqlUpdateSetExpression(dbContext, tableAlias ?? "t");

        if (expression.Body is NewExpression newExpression)
        {
            var sb = new StringBuilder();
            for (int i = 0; i < newExpression.Members.Count; i++)
            {
                if (sb.Length > 0)
                    sb.Append(',');
                string colName = newExpression.Members[i].Name;
                string value = GetExpressionValueAsString(newExpression.Arguments[i]);
                sb.Append(prefix).Append(dbContext.DelimitIdentifier(colName)).Append('=').Append(value);
            }
            return sb.ToString();
        }

        throw new InvalidOperationException("SoftDeleteColumns expression must be a MemberInit or NewExpression with constant values.");
    }

    private static string FormatMethodCallExpression(MethodCallExpression methodCallExpression)
    {
        var argValues = methodCallExpression.Arguments.Select(GetExpressionValueAsString).ToArray();
        return methodCallExpression.Method.Name switch
        {
            "ToString" => $"TO_CHAR({argValues[0]})",
            _ => $"{methodCallExpression.Method.Name}({string.Join(",", argValues)})"
        };
    }

    private static string FormatBinaryExpression(BinaryExpression binaryExpression)
    {
        string leftValue = GetExpressionValueAsString(binaryExpression.Left);
        string rightValue = GetExpressionValueAsString(binaryExpression.Right);
        return $"({leftValue} {binaryExpression.NodeType.ToSql()} {rightValue})";
    }

    private static string ConvertToSqlValue(object value) => value switch
    {
        null => "NULL",
        string str => $"'{str.Replace("'", "''")}'",
        Guid guid => $"'{guid}'",
        bool b => b ? "1" : "0",
        DateTime dt => $"'{dt:yyyy-MM-ddTHH:mm:ss.fffffff}'",
        DateTimeOffset dto => $"'{dto:yyyy-MM-ddTHH:mm:ss.fffffffzzzz}'",
        _ when value.GetType().IsEnum => Convert.ToString((int)value),
        _ when !value.GetType().IsClass => Convert.ToString(value, CultureInfo.InvariantCulture),
        _ => throw new NotImplementedException("Unhandled data type.")
    };

    private static string DelimitMemberAccess(DbContext dbContext, string expression) =>
        Regex.Replace(expression, @"(?<![""\]\w])([A-Za-z_][A-Za-z0-9_]*)(?:\.([A-Za-z_][A-Za-z0-9_]*))+", match =>
        {
            var parts = match.Value.Split('.');
            return string.Join(".", parts.Select((part, index) => index == 0 ? part : dbContext.DelimitIdentifier(part)));
        });
}
