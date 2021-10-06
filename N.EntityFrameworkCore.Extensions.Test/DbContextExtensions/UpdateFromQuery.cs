using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Data;
using System;
using System.Globalization;
using System.Linq;
using System.Threading;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions
{
    [TestClass]
    public class UpdateFromQuery : DbContextExtensionsBase
    {
        [TestMethod]
        public void With_Boolean_Value()
        {
            var dbContext = SetupDbContext(true);
            int oldTotal = dbContext.Products.Count(a => a.OutOfStock);
            int rowUpdated = dbContext.Products.Where(a => a.OutOfStock).UpdateFromQuery(a => new Product { OutOfStock = false });
            int newTotal = dbContext.Products.Count(o => o.OutOfStock);

            Assert.IsTrue(oldTotal > 0, "There must be articles in database that match this condition (OutOfStock == true)");
            Assert.IsTrue(rowUpdated == oldTotal, "The number of rows update must match the count of rows that match the condition (OutOfStock == false)");
            Assert.IsTrue(newTotal == 0, "The new count must be 0 to indicate all records were updated");
        }
        [TestMethod]
        public void With_Concatenating_String()
        {
            var dbContext = SetupDbContext(true);
            var orders = dbContext.Orders.Where(o => o.ExternalId == null);
            int oldTotal = orders.Count();
            int rowUpdated = orders.UpdateFromQuery(o => new Order { ExternalId = Convert.ToString(o.Id) + "Test" });
            int newTotal = orders.Count();

            Assert.IsTrue(oldTotal > 0, "There must be orders in database that match this condition (ExternalId == null)");
            Assert.IsTrue(rowUpdated == oldTotal, "The number of rows update must match the count of rows that match the condition (ExternalId == null)");
            Assert.IsTrue(newTotal == 0, "The new count must be 0 to indicate all records were updated");
        }
        [TestMethod]
        public void With_Concatenating_String_And_Number()
        {
            var dbContext = SetupDbContext(true);
            var orders = dbContext.Orders.Where(o => o.ExternalId == null);
            int oldTotal = orders.Count();
            int rowUpdated = orders.UpdateFromQuery(o => new Order { ExternalId = Convert.ToString(o.Id) + "Test" });
            int newTotal = orders.Count();

            Assert.IsTrue(oldTotal > 0, "There must be orders in database that match this condition (ExternalId == null)");
            Assert.IsTrue(rowUpdated == oldTotal, "The number of rows update must match the count of rows that match the condition (ExternalId == null)");
            Assert.IsTrue(newTotal == 0, "The new count must be 0 to indicate all records were updated");
        }
        [TestMethod]
        public void With_DateTime_Value()
        {
            var dbContext = SetupDbContext(true);
            DateTime dateTime = dbContext.Orders.Max(o => o.AddedDateTime).AddDays(-30);
            DateTime now = DateTime.UtcNow;

            int oldTotal = dbContext.Orders.Where(o => o.AddedDateTime >= dateTime).Count();
            int rowUpdated = dbContext.Orders.Where(o => o.AddedDateTime >= dateTime).UpdateFromQuery(o => new Order { ModifiedDateTime = now });
            int newTotal = dbContext.Orders.Where(o => o.ModifiedDateTime == now).Count();

            Assert.IsTrue(oldTotal > 0, "There must be orders in database that match this condition (Orders added in last 30 days)");
            Assert.IsTrue(rowUpdated == oldTotal, "The number of rows update must match the count of rows that match the condition (Orders added in last 30 days)");
            Assert.IsTrue(newTotal == 0, "The new count must be 0 to indicate all records were updated");
        }
        [TestMethod]
        public void With_Decimal_Value()
        {
            var dbContext = SetupDbContext(true);
            var orders = dbContext.Orders.Where(o => o.Price < 10M);
            int oldTotal = orders.Count();
            int rowUpdated = orders.UpdateFromQuery(o => new Order { Price = 25.30M });
            int newTotal = orders.Count();
            int matchCount = dbContext.Orders.Where(o => o.Price == 25.30M).Count();

            Assert.IsTrue(oldTotal > 0, "There must be orders in database that match this condition (Price < $10)");
            Assert.IsTrue(rowUpdated == oldTotal, "The number of rows update must match the count of rows that match the condtion (Price < $10)");
            Assert.IsTrue(newTotal == 0, "The new count must be 0 to indicate all records were updated");
            Assert.IsTrue(matchCount == rowUpdated, "The match count must be equal the number of rows updated in the database.");
        }
        [TestMethod]
        public void With_Different_Culture()
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.GetCultureInfo("sv-SE");
            var dbContext = SetupDbContext(true);
            int oldTotal = dbContext.Orders.Where(o => o.Price < 10M).Count();
            int rowUpdated = dbContext.Orders.Where(o => o.Price < 10M).UpdateFromQuery(o => new Order { Price = 25.30M });
            int newTotal = dbContext.Orders.Where(o => o.Price < 10M).Count();
            int matchCount = dbContext.Orders.Where(o => o.Price == 25.30M).Count();

            Assert.AreEqual("25,30", Convert.ToString(25.30M));
            Assert.IsTrue(oldTotal > 0, "There must be orders in database that match this condition (Price < $10)");
            Assert.IsTrue(rowUpdated == oldTotal, "The number of rows update must match the count of rows that match the condtion (Price < $10)");
            Assert.IsTrue(newTotal == 0, "The new count must be 0 to indicate all records were updated");
            Assert.IsTrue(matchCount == rowUpdated, "The match count must be equal the number of rows updated in the database.");
        }
        [TestMethod]
        public void With_MethodCall()
        {
            var dbContext = SetupDbContext(true);

            int oldTotal = dbContext.Orders.Count(a => a.Price < 10);
            int rowUpdated = dbContext.Orders.Where(a => a.Price < 10).UpdateFromQuery(o => new Order { Price = Math.Ceiling((o.Price + 10.5M) * 3 / 1) });
            int newTotal = dbContext.Orders.Count(o => o.Price < 10);

            Assert.IsTrue(oldTotal > 0, "There must be order in database that match this condition (Price < 10)");
            Assert.IsTrue(rowUpdated == oldTotal, "The number of rows update must match the count of rows that match the condition (Price < 10)");
            Assert.IsTrue(newTotal == 0, "The new count must be 0 to indicate all records were updated");
        }
        [TestMethod]
        public void With_Null_Value()
        {
            var dbContext = SetupDbContext(true);
            var orders = dbContext.Orders.Where(o => o.ExternalId != null);
            int oldTotal = orders.Count();
            int rowUpdated = orders.UpdateFromQuery(o => new Order { ExternalId = null });
            int newTotal = orders.Count();

            Assert.IsTrue(oldTotal > 0, "There must be orders in database that match this condition (ExternalId != null)");
            Assert.IsTrue(rowUpdated == oldTotal, "The number of rows update must match the count of rows that match the condition (ExternalId != null)");
            Assert.IsTrue(newTotal == 0, "The new count must be 0 to indicate all records were updated");
        }
        [TestMethod]
        public void With_String_Containing_Apostrophe()
        {
            var dbContext = SetupDbContext(true);
            int oldTotal = dbContext.Orders.Where(o => o.ExternalId == null).Count();
            int rowUpdated = dbContext.Orders.Where(o => o.ExternalId == null).UpdateFromQuery(o => new Order { ExternalId = "inv'alid" });
            int newTotal = dbContext.Orders.Where(o => o.ExternalId == null).Count();

            Assert.IsTrue(oldTotal > 0, "There must be orders in database that match this condition (ExternalId == null)");
            Assert.IsTrue(rowUpdated == oldTotal, "The number of rows update must match the count of rows that match the condition (ExternalId == null)");
            Assert.IsTrue(newTotal == 0, "The new count must be 0 to indicate all records were updated");
        }
        [TestMethod]
        public void With_Variables()
        {
            var dbContext = SetupDbContext(true);
            decimal priceStart = 10M;
            decimal priceUpdate = 0.34M;

            int oldTotal = dbContext.Orders.Count(a => a.Price < 10);
            int rowUpdated = dbContext.Orders.Where(a => a.Price < 10).UpdateFromQuery(a => new Order { Price = priceStart + priceUpdate });
            int newTotal = dbContext.Orders.Count(o => o.Price < 10);

            Assert.IsTrue(oldTotal > 0, "There must be orders in database that match this condition (Price < 10)");
            Assert.IsTrue(rowUpdated == oldTotal, "The number of rows update must match the count of rows that match the condition (Price < 10)");
            Assert.IsTrue(newTotal == 0, "The new count must be 0 to indicate all records were updated");
        }
        [TestMethod]
        public void With_Variable_And_Decimal()
        {
            var dbContext = SetupDbContext(true);
            decimal priceStart = 10M;

            int oldTotal = dbContext.Orders.Count(a => a.Price < 10);
            int rowUpdated = dbContext.Orders.Where(a => a.Price < 10).UpdateFromQuery(a => new Order { Price = priceStart + 7M });
            int newTotal = dbContext.Orders.Count(o => o.Price < 10);

            Assert.IsTrue(oldTotal > 0, "There must be orders in database that match this condition (Price < 10)");
            Assert.IsTrue(rowUpdated == oldTotal, "The number of rows update must match the count of rows that match the condition (Price < 10)");
            Assert.IsTrue(newTotal == 0, "The new count must be 0 to indicate all records were updated");
        }
    }
}
