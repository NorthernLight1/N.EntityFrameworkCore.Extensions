using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions
{
    [TestClass]
    public class BulkFetch : DbContextExtensionsBase
    {
        [TestMethod]
        public void With_Default_Options()
        {
            var dbContext = SetupDbContext(true);
            var orders = dbContext.Orders.Where(o => o.Price == 1.25M).ToList();
            var fetchedOrders = dbContext.Orders.BulkFetch(orders);
            bool ordersAreMatched = true;

            foreach (var fetchedOrder in fetchedOrders)
            {
                var order = orders.First(o => o.Id == fetchedOrder.Id);
                if (order.ExternalId != fetchedOrder.ExternalId || order.AddedDateTime != fetchedOrder.AddedDateTime || order.ModifiedDateTime != fetchedOrder.ModifiedDateTime)
                {
                    ordersAreMatched = false;
                    break;
                }
            }

            Assert.IsTrue(orders.Count > 0, "There must be orders in database that match this condition (Price = $1.25)");
            Assert.IsTrue(orders.Count == fetchedOrders.Count(), "The number of rows deleted must match the count of existing rows in database");
            Assert.IsTrue(ordersAreMatched, "The orders from BulkFetch() should match what is retrieved from DbContext");
        }
        [TestMethod]
        public void With_Enum()
        {
            var dbContext = SetupDbContext(true);
            var products = dbContext.Products.Where(o => o.Price == 1.25m).ToList();
            var fetchedProducts = dbContext.Products.BulkFetch(products);
            bool ordersAreMatched = true;

            foreach (var fetchedOrder in fetchedProducts)
            {
                var order = products.First(o => o.Id == fetchedOrder.Id);
                if (order.Id != fetchedOrder.Id || order.Name != fetchedOrder.Name || order.StatusEnum != fetchedOrder.StatusEnum)
                {
                    ordersAreMatched = false;
                    break;
                }
            }

            Assert.IsTrue(products.Count > 0, "There must be orders in database that match this condition (Price = $1.25)");
            Assert.IsTrue(products.Count == fetchedProducts.Count(), "The number of rows deleted must match the count of existing rows in database");
            Assert.IsTrue(ordersAreMatched, "The orders from BulkFetch() should match what is retrieved from DbContext");
        }
        [TestMethod]
        public void With_IQueryable()
        {
            var dbContext = SetupDbContext(true);
            var orders = dbContext.Orders.Where(o => o.Price <= 10 && o.ExternalId != null);
            var fetchedOrders = dbContext.Orders.BulkFetch(orders, options => { options.IgnoreColumns = o => new { o.ExternalId }; }).ToList();
            int newTotal = dbContext.Orders.Where(o => o.Price <= 10 && o.ExternalId == null).Count();
            bool foundNullExternalId = fetchedOrders.Where(o => o.ExternalId != null).Any();

            Assert.IsTrue(orders.Count() > 0, "There must be orders in the database that match condition (Price <= 10 And ExternalId != null)");
            Assert.IsTrue(orders.Count() == fetchedOrders.Count(), "The number of orders must match the number of fetched orders");
            Assert.IsTrue(!foundNullExternalId, "Fetched orders should not contain any items where ExternalId is null.");
        }
        [TestMethod]
        public void With_Options_IgnoreColumns()
        {
            var dbContext = SetupDbContext(true);
            var orders = dbContext.Orders.Where(o => o.Price <= 10 && o.ExternalId != null).ToList();
            var fetchedOrders = dbContext.Orders.BulkFetch(orders, options => { options.IgnoreColumns = o => new { o.ExternalId }; }).ToList();
            int newTotal = dbContext.Orders.Where(o => o.Price <= 10 && o.ExternalId == null).Count();
            bool foundNullExternalId = fetchedOrders.Where(o => o.ExternalId != null).Any();

            Assert.IsTrue(orders.Count() > 0, "There must be orders in the database that match condition (Price <= 10 And ExternalId != null)");
            Assert.IsTrue(orders.Count() == fetchedOrders.Count(), "The number of orders must match the number of fetched orders");
            Assert.IsTrue(!foundNullExternalId, "Fetched orders should not contain any items where ExternalId is null.");
        }
    }
}
