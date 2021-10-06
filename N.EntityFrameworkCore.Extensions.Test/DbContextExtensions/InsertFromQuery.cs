using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions
{
    [TestClass]
    public class InsertFromQuery : DbContextExtensionsBase
    {
        [TestMethod]
        public void With_DateTime_Value()
        {
            var dbContext = SetupDbContext(true);
            string tableName = "OrdersLast30Days";
            DateTime dateTime = dbContext.Orders.Max(o => o.AddedDateTime).AddDays(-30);
            int oldTotal = dbContext.Orders.Count();

            var orders = dbContext.Orders.Where(o => o.AddedDateTime >= dateTime);
            int oldSourceTotal = orders.Count();
            int rowsInserted = orders.InsertFromQuery(tableName,
                o => new { o.Id, o.ExternalId, o.Price, o.AddedDateTime, o.ModifiedDateTime, o.Active });
            int newSourceTotal = orders.Count();
            int newTargetTotal = orders.UsingTable(tableName).Count();

            Assert.IsTrue(oldTotal > oldSourceTotal, "The total should be greater then the number of rows selected from the source table");
            Assert.IsTrue(oldSourceTotal > 0, "There should be existing data in the source table");
            Assert.IsTrue(oldSourceTotal == newSourceTotal, "There should not be any change in the count of rows in the source table");
            Assert.IsTrue(rowsInserted == oldSourceTotal, "The number of records inserted  must match the count of the source table");
            Assert.IsTrue(rowsInserted == newTargetTotal, "The different in count in the target table before and after the insert must match the total row inserted");
        }
        [TestMethod]
        public void With_Decimal_Value()
        {
            var dbContext = SetupDbContext(true);
            string tableName = "OrdersUnderTen";
            var orders = dbContext.Orders.Where(o => o.Price < 10M);
            int oldSourceTotal = orders.Count();
            int rowsInserted = dbContext.Orders.Where(o => o.Price < 10M).InsertFromQuery(tableName, o => new { o.Id, o.Price, o.AddedDateTime, o.Active });
            int newSourceTotal = orders.Count();
            int newTargetTotal = orders.UsingTable(tableName).Count();

            Assert.IsTrue(oldSourceTotal > 0, "There should be existing data in the source table");
            Assert.IsTrue(oldSourceTotal == newSourceTotal, "There should not be any change in the count of rows in the source table");
            Assert.IsTrue(rowsInserted == oldSourceTotal, "The number of records inserted  must match the count of the source table");
            Assert.IsTrue(rowsInserted == newTargetTotal, "The different in count in the target table before and after the insert must match the total row inserted");
        }
    }
}
