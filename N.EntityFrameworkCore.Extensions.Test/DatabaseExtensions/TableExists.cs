using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace N.EntityFrameworkCore.Extensions.Test.DatabaseExtensions
{
    [TestClass]
    public class TableExists :DatabaseExtensionsBase
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
}
