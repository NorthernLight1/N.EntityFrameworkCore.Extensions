using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions
{
    [TestClass]
    public class InsertFromQueryAsync : DbContextExtensionsBase
    {
        [TestMethod]
        public async Task With_DateTime_Value()
        {
            var dbContext = SetupDbContext(true);
            string tableName = "OrdersLast30Days";
            DateTime dateTime = dbContext.Orders.Max(o => o.AddedDateTime).AddDays(-30);
            int oldTotal = dbContext.Orders.Count();

            var orders = dbContext.Orders.Where(o => o.AddedDateTime >= dateTime);
            int oldSourceTotal = orders.Count();
            int rowsInserted = await orders.InsertFromQueryAsync(tableName,
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
        public async Task With_Decimal_Value()
        {
            var dbContext = SetupDbContext(true);
            string tableName = "OrdersUnderTen";
            var orders = dbContext.Orders.Where(o => o.Price < 10M);
            int oldSourceTotal = orders.Count();
            int rowsInserted = await dbContext.Orders.Where(o => o.Price < 10M).InsertFromQueryAsync(tableName, o => new { o.Id, o.Price, o.AddedDateTime, o.Active });
            int newSourceTotal = orders.Count();
            int newTargetTotal = orders.UsingTable(tableName).Count();

            Assert.IsTrue(oldSourceTotal > 0, "There should be existing data in the source table");
            Assert.IsTrue(oldSourceTotal == newSourceTotal, "There should not be any change in the count of rows in the source table");
            Assert.IsTrue(rowsInserted == oldSourceTotal, "The number of records inserted  must match the count of the source table");
            Assert.IsTrue(rowsInserted == newTargetTotal, "The different in count in the target table before and after the insert must match the total row inserted");
        }
        [TestMethod]
        public async Task With_Schema()
        {
            var dbContext = SetupDbContext(true, PopulateDataMode.Schema);
            string tableName = "ProductsUnderTen";
            var products = dbContext.ProductsWithCustomSchema.Where(o => o.Price < 10M);
            int oldSourceTotal = products.Count();
            int rowsInserted = await dbContext.ProductsWithCustomSchema.Where(o => o.Price < 10M).InsertFromQueryAsync(tableName, o => new { o.Id, o.Price });
            int newSourceTotal = products.Count();
            int newTargetTotal = products.UsingTable(tableName).Count();

            Assert.IsTrue(oldSourceTotal > 0, "There should be existing data in the source table");
            Assert.IsTrue(oldSourceTotal == newSourceTotal, "There should not be any change in the count of rows in the source table");
            Assert.IsTrue(rowsInserted == oldSourceTotal, "The number of records inserted  must match the count of the source table");
            Assert.IsTrue(rowsInserted == newTargetTotal, "The different in count in the target table before and after the insert must match the total row inserted");
        }
        [TestMethod]
        public async Task With_Transaction()
        {
            var dbContext = SetupDbContext(true);
            string tableName = "OrdersUnderTen";
            int rowsInserted;
            bool tableExistsBefore, tableExistsAfter;
            int oldSourceTotal = dbContext.Orders.Where(o => o.Price < 10M).Count();
            using (var transaction = dbContext.Database.BeginTransaction())
            {
                rowsInserted = await dbContext.Orders.Where(o => o.Price < 10M).InsertFromQueryAsync(tableName, o => new { o.Price, o.Id, o.AddedDateTime, o.Active });
                tableExistsBefore = dbContext.Database.TableExists(tableName);
                transaction.Rollback();
            }
            tableExistsAfter = dbContext.Database.TableExists(tableName);
            int newSourceTotal = dbContext.Orders.Where(o => o.Price < 10M).Count();

            Assert.IsTrue(oldSourceTotal > 0, "There must be orders in database that match this condition (Price < $10)");
            Assert.IsTrue(rowsInserted == oldSourceTotal, "The number of rows update must match the count of rows that match the condtion (Price < $10)");
            Assert.IsTrue(newSourceTotal == oldSourceTotal, "The new count must match the old count since the transaction was rollbacked");
            Assert.IsTrue(tableExistsBefore, string.Format("Table {0} should exist before transaction rollback", tableName));
            Assert.IsFalse(tableExistsAfter, string.Format("Table {0} should not exist after transaction rollback", tableName));
        }
    }
}