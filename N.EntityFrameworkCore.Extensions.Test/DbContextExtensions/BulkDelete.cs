using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Data;
using System.Linq;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions
{
    [TestClass]
    public class BulkDelete : DbContextExtensionsBase
    {
        [TestMethod]
        public void With_Default_Options()
        {
            var dbContext = SetupDbContext(true);
            var orders = dbContext.Orders.Where(o => o.Price == 1.25M).ToList();
            int rowsDeleted = dbContext.BulkDelete(orders);
            int newTotal = dbContext.Orders.Where(o => o.Price == 1.25M).Count();

            Assert.IsTrue(orders.Count > 0, "There must be orders in database that match this condition (Price = $1.25)");
            Assert.IsTrue(rowsDeleted == orders.Count, "The number of rows deleted must match the count of existing rows in database");
            Assert.IsTrue(newTotal == 0, "Must be 0 to indicate all records were deleted");
        }
        [TestMethod]
        public void With_Default_Options_Tph()
        {
            var dbContext = SetupDbContext(true);
            var customers = dbContext.TphPeople.OfType<TphCustomer>().ToList();
            int rowsDeleted = dbContext.BulkDelete(customers);
            var newCustomers = dbContext.TphPeople.OfType<TphCustomer>().Count();

            Assert.IsTrue(customers.Count > 0, "There must be tphCustomer records in database");
            Assert.IsTrue(rowsDeleted == customers.Count, "The number of rows deleted must match the count of existing rows in database");
            Assert.IsTrue(newCustomers == 0, "Must be 0 to indicate all records were deleted");
        }
        [TestMethod]
        public void With_Options_DeleteOnCondition()
        {
            var dbContext = SetupDbContext(true);
            int oldTotal = dbContext.Orders.Where(o => o.Price == 1.25M).Count();
            var orders = dbContext.Orders.Where(o => o.Price == 1.25M && o.ExternalId != null).ToList();
            int rowsDeleted = dbContext.BulkDelete(orders, options => { options.DeleteOnCondition = (s, t) => s.ExternalId == t.ExternalId; options.UsePermanentTable = true; });
            int newTotal = dbContext.Orders.Where(o => o.Price == 1.25M).Count();

            Assert.IsTrue(orders.Count > 0, "There must be orders in database that match this condition (Price < $2)");
            Assert.IsTrue(rowsDeleted == orders.Count, "The number of rows deleted must match the count of existing rows in database");
            Assert.IsTrue(newTotal == oldTotal - rowsDeleted, "Must be 0 to indicate all records were deleted");
        }
    }
}
