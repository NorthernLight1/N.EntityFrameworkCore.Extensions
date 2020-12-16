namespace N.EntityFrameworkCore.Extensions.Test.Data
{
    public class Order
    {
        public long Id { get; set; }
        public string ExternalId { get; set; }
        public decimal Price { get; set; }
        public Order()
        {

        }
    }
}