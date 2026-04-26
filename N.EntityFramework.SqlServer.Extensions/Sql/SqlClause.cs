namespace N.EntityFrameworkCore.Extensions.Sql;

internal sealed class SqlClause
{
    internal string Name { get; set; }
    internal string InputText { get; set; }
    internal string Sql => ToString();
    internal static SqlClause Parse(string name, string inputText)
    {
        string cleanText = inputText.Replace("\r\n", "").Trim();
        return new SqlClause { Name = name, InputText = cleanText };
    }
    public override string ToString() => $"{Name} {InputText}";
}