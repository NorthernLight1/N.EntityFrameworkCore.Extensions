using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Data;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions
{
    [TestClass]
    public class BulkDeleteAsync : DbContextExtensionsBase
    {
        [TestMethod]
        public async Task With_Default_Options()
        {
            var dbContext = SetupDbContext(true);
            var orders = dbContext.Orders.Where(o => o.Price == 1.25M).ToList();
            int rowsDeleted = await dbContext.BulkDeleteAsync(orders);
            int newTotal = dbContext.Orders.Where(o => o.Price == 1.25M).Count();

            Assert.IsTrue(orders.Count > 0, "There must be orders in database that match this condition (Price = $1.25)");
            Assert.IsTrue(rowsDeleted == orders.Count, "The number of rows deleted must match the count of existing rows in database");
            Assert.IsTrue(newTotal == 0, "Must be 0 to indicate all records were deleted");
        }
        [TestMethod]
        public async Task With_Inheritance_Tpc()
        {
            var dbContext = SetupDbContext(true, PopulateDataMode.Tpc);
            var customers = dbContext.TpcPeople.OfType<TpcCustomer>().ToList();
            int rowsDeleted = await dbContext.BulkDeleteAsync(customers);
            var newCustomers = dbContext.TpcPeople.OfType<TpcCustomer>().Count();

            Assert.IsTrue(customers.Count > 0, "There must be tphCustomer records in database");
            Assert.IsTrue(rowsDeleted == customers.Count, "The number of rows deleted must match the count of existing rows in database");
            Assert.IsTrue(newCustomers == 0, "Must be 0 to indicate all records were deleted");
        }
        [TestMethod]
        public async Task With_Inheritance_Tph()
        {
            var dbContext = SetupDbContext(true, PopulateDataMode.Tph);
            var customers = dbContext.TphPeople.OfType<TphCustomer>().ToList();
            int rowsDeleted = await dbContext.BulkDeleteAsync(customers);
            var newCustomers = dbContext.TphPeople.OfType<TphCustomer>().Count();

            Assert.IsTrue(customers.Count > 0, "There must be tphCustomer records in database");
            Assert.IsTrue(rowsDeleted == customers.Count, "The number of rows deleted must match the count of existing rows in database");
            Assert.IsTrue(newCustomers == 0, "Must be 0 to indicate all records were deleted");
        }
        [TestMethod]
        public async Task With_Inheritance_Tpt()
        {
            var dbContext = SetupDbContext(true, PopulateDataMode.Tpt);
            var customers = dbContext.TptCustomers.ToList();
            int rowsDeleted = await dbContext.BulkDeleteAsync(customers);
            var newCustomers = dbContext.TptCustomers.Count();

            Assert.IsTrue(customers.Count > 0, "There must be tphCustomer records in database");
            Assert.IsTrue(rowsDeleted == customers.Count, "The number of rows deleted must match the count of existing rows in database");
            Assert.IsTrue(newCustomers == 0, "Must be 0 to indicate all records were deleted");
        }
        [TestMethod]
        public async Task With_Options_DeleteOnCondition()
        {
            var dbContext = SetupDbContext(true);
            int oldTotal = dbContext.Orders.Where(o => o.Price == 1.25M).Count();
            var orders = dbContext.Orders.Where(o => o.Price == 1.25M && o.ExternalId != null).ToList();
            int rowsDeleted = await dbContext.BulkDeleteAsync(orders, options => { options.DeleteOnCondition = (s, t) => s.ExternalId == t.ExternalId; options.UsePermanentTable = true; });
            int newTotal = dbContext.Orders.Where(o => o.Price == 1.25M).Count();

            Assert.IsTrue(orders.Count > 0, "There must be orders in database that match this condition (Price < $2)");
            Assert.IsTrue(rowsDeleted == orders.Count, "The number of rows deleted must match the count of existing rows in database");
            Assert.IsTrue(newTotal == oldTotal - rowsDeleted, "Must be 0 to indicate all records were deleted");
        }
        [TestMethod]
        public async Task With_Transaction()
        {
            var dbContext = SetupDbContext(true);
            var orders = dbContext.Orders.Where(o => o.Price == 1.25M).ToList();
            int rowsDeleted, newTotal = 0;
            using (var transaction = dbContext.Database.BeginTransaction())
            {
                rowsDeleted = await dbContext.BulkDeleteAsync(orders);
                newTotal = dbContext.Orders.Where(o => o.Price == 1.25M).Count();
                transaction.Rollback();
            }
            var rollbackTotal = dbContext.Orders.Count(o => o.Price == 1.25M);

            Assert.IsTrue(orders.Count > 0, "There must be orders in database that match this condition (Price < $2)");
            Assert.IsTrue(rowsDeleted == orders.Count, "The number of rows deleted must match the count of existing rows in database");
            Assert.IsTrue(newTotal == 0, "Must be 0 to indicate all records were deleted");
            Assert.IsTrue(rollbackTotal == orders.Count, "The number of rows after the transacation has been rollbacked should match the original count");
        }
    }
}