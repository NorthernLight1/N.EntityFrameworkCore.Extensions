using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace N.EntityFrameworkCore.Extensions.Test.DatabaseExtensions;

[TestClass]
public class TableExists : DatabaseExtensionsBase
{
    [TestMethod]
    public void With_Orders_Table()
    {
            var dbContext = SetupDbContext(true);
            int efCount = dbContext.Orders.Where(o => o.Price > 5M).Count();
            bool ordersTableExists = dbContext.Database.TableExists("Orders");
            bool orderNewTableExists = dbContext.Database.TableExists("OrdersNew");

            Assert.IsTrue(ordersTableExists, "Orders table should exist");
            Assert.IsTrue(!orderNewTableExists, "Orders_New table should not exist");
        }
}