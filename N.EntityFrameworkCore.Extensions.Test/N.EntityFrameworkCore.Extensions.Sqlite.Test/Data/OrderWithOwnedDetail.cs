namespace N.EntityFrameworkCore.Extensions.Test.Data;

public class OrderWithOwnedDetail
{
    public long Id { get; set; }
    public decimal Price { get; set; }
    public OwnedDetail Detail { get; set; }
}

public class OwnedDetail
{
    public string City { get; set; }
    public string Region { get; set; }
}
