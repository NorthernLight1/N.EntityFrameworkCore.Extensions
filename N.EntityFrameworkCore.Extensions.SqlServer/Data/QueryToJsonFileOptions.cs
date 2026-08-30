namespace N.EntityFrameworkCore.Extensions;

public class QueryToJsonFileOptions
{
    public int? CommandTimeout { get; set; }
    public bool Indented { get; set; } = true;

    public QueryToJsonFileOptions()
    {
    }
}
