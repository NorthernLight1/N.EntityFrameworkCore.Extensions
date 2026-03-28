namespace N.EntityFrameworkCore.Extensions.Sql;

class SqlClause
{
    internal string Name { get; set; }
    internal string InputText { get; set; }
    public string Sql => ToString();
    public static SqlClause Parse(string name, string inputText)
    {
            string cleanText = inputText.Replace("\r\n", "").Trim();
            return new SqlClause { Name = name, InputText = cleanText };
        }
    public override string ToString()
    {
            return $"{Name} {InputText}";
        }
}