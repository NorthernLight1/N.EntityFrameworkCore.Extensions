using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions
{
    [TestClass]
    public class DeleteFromQuery : DbContextExtensionsBase
    {
        [TestMethod]
        public void With_Boolean_Value()
        {
            var dbContext = SetupDbContext(true);
            var products = dbContext.Products.Where(p => p.OutOfStock);
            int oldTotal = products.Count(a => a.OutOfStock);
            int rowUpdated = products.DeleteFromQuery();
            int newTotal = dbContext.Products.Count(o => o.OutOfStock);

            Assert.IsTrue(oldTotal > 0, "There must be products in database that match this condition (OutOfStock == true)");
            Assert.IsTrue(rowUpdated == oldTotal, "The number of rows update must match the count of rows that match the condition (OutOfStock == false)");
            Assert.IsTrue(newTotal == 0, "The new count must be 0 to indicate all records were updated");
        }
        [TestMethod]
        public void With_Decimal_Using_IQuerable()
        {
            var dbContext = SetupDbContext(true);
            var orders = dbContext.Orders.Where(o => o.Price <= 10);
            int oldTotal = orders.Count();
            int rowsDeleted = orders.DeleteFromQuery();
            int newTotal = orders.Count();

            Assert.IsTrue(oldTotal > 0, "There must be orders in database that match this condition");
            Assert.IsTrue(rowsDeleted == oldTotal, "The number of rows deleted must match the count of existing rows in database");
            Assert.IsTrue(newTotal == 0, "Delete() Failed: must be 0 to indicate all records were delted");
        }
        [TestMethod]
        public void With_Decimal_Using_IEnumerable()
        {
            var dbContext = SetupDbContext(true);
            var orders = dbContext.Orders.Where(o => o.Price <= 10);
            int oldTotal = orders.Count();
            int rowsDeleted = orders.DeleteFromQuery();
            int newTotal = orders.Count();

            Assert.IsTrue(oldTotal > 0, "There must be orders in database that match this condition");
            Assert.IsTrue(rowsDeleted == oldTotal, "The number of rows deleted must match the count of existing rows in database");
            Assert.IsTrue(newTotal == 0, "The new count must be 0 to indicate all records were deleted");
        }
        [TestMethod]
        public void With_DateTime()
        {
            var dbContext = SetupDbContext(true);
            int oldTotal = dbContext.Orders.Count();
            DateTime dateTime = dbContext.Orders.Max(o => o.AddedDateTime).AddDays(-30);
            int rowsToDelete = dbContext.Orders.Where(o => o.ModifiedDateTime != null && o.ModifiedDateTime >= dateTime).Count();
            int rowsDeleted = dbContext.Orders.Where(o => o.ModifiedDateTime != null && o.ModifiedDateTime >= dateTime)
                .DeleteFromQuery();
            int newTotal = dbContext.Orders.Count();

            Assert.IsTrue(oldTotal > 0, "There must be orders in database that match this condition");
            Assert.IsTrue(rowsDeleted == rowsToDelete, "The number of rows deleted must match the count of the rows that matched in the database");
            Assert.IsTrue(oldTotal - newTotal == rowsDeleted, "The rows deleted must match the new count minues the old count");
        }
        [TestMethod]
        public void With_Different_Values()
        {
            var dbContext = SetupDbContext(true);
            int oldTotal = dbContext.Orders.Count();
            DateTime dateTime = dbContext.Orders.Max(o => o.AddedDateTime).AddDays(-30);
            var orders = dbContext.Orders.Where(o => o.Id == 1 && o.Active && o.ModifiedDateTime >= dateTime);
            int rowsToDelete = orders.Count();
            int rowsDeleted = orders.DeleteFromQuery();
            int newTotal = dbContext.Orders.Count();

            Assert.IsTrue(oldTotal > 0, "There must be orders in database that match this condition");
            Assert.IsTrue(rowsDeleted == rowsToDelete, "The number of rows deleted must match the count of the rows that matched in the database");
            Assert.IsTrue(oldTotal - newTotal == rowsDeleted, "The rows deleted must match the new count minues the old count");
        }
    }
}
