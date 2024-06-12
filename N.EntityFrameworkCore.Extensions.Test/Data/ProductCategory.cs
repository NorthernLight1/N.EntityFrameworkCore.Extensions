namespace N.EntityFrameworkCore.Extensions.Test.Data;

public class ProductCategory
{
    public int Id { get; set; }
    public string Name { get; set; }
    public bool Active { get; internal set; }
}