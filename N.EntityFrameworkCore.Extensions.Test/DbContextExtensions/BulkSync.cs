using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Data;
using System;
using System.Collections.Generic;
using System.Linq;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions
{
    [TestClass]
    public class BulkSync : DbContextExtensionsBase
    {
        [TestMethod]
        public void With_Default_Options()
        {
            var dbContext = SetupDbContext(true);
            int oldTotal = dbContext.Orders.Count();
            var orders = dbContext.Orders.Where(o => o.Id <= 10000).OrderBy(o => o.Id).ToList();
            int ordersToAdd = 5000;
            int ordersToUpdate = orders.Count;
            foreach (var order in orders)
            {
                order.Price = Convert.ToDecimal(order.Id + .25);
            }
            for (int i = 0; i < ordersToAdd; i++)
            {
                orders.Add(new Order { Id = 100000 + i, Price = 3.55M });
            }
            var result = dbContext.BulkSync(orders);
            var newOrders = dbContext.Orders.OrderBy(o => o.Id).ToList();
            bool areAddedOrdersMerged = true;
            bool areUpdatedOrdersMerged = true;
            foreach (var newOrder in newOrders.Where(o => o.Id <= 10000).OrderBy(o => o.Id))
            {
                if (newOrder.Price != Convert.ToDecimal(newOrder.Id + .25))
                {
                    areUpdatedOrdersMerged = false;
                    break;
                }
            }
            foreach (var newOrder in newOrders.Where(o => o.Id >= 500000).OrderBy(o => o.Id))
            {
                if (newOrder.Price != 3.55M)
                {
                    areAddedOrdersMerged = false;
                    break;
                }
            }

            Assert.IsTrue(result.RowsAffected == oldTotal + ordersToAdd, "The number of rows inserted must match the count of order list");
            Assert.IsTrue(result.RowsUpdated == ordersToUpdate, "The number of rows updated must match");
            Assert.IsTrue(result.RowsInserted == ordersToAdd, "The number of rows added must match");
            Assert.IsTrue(result.RowsDeleted == oldTotal - orders.Count() + ordersToAdd, "The number of rows deleted must match the difference from the total existing orders to the new orders to add/update");
            Assert.IsTrue(areAddedOrdersMerged, "The orders that were added did not merge correctly");
            Assert.IsTrue(areUpdatedOrdersMerged, "The orders that were updated did not merge correctly");
        }
        [TestMethod]
        public void With_Options_AutoMapIdentity()
        {
            var dbContext = SetupDbContext(true);
            int oldTotal = dbContext.Orders.Count();
            int ordersToUpdate = 3;
            int ordersToAdd = 2;
            var orders = new List<Order>
            {
                new Order { ExternalId = "id-1", Price=7.10M },
                new Order { ExternalId = "id-2", Price=9.33M },
                new Order { ExternalId = "id-3", Price=3.25M },
                new Order { ExternalId = "id-1000001", Price=2.15M },
                new Order { ExternalId = "id-1000002", Price=5.75M },
            };
            var result = dbContext.BulkSync(orders, options => { options.MergeOnCondition = (s, t) => s.ExternalId == t.ExternalId; options.UsePermanentTable = true; });
            bool autoMapIdentityMatched = true;
            foreach (var order in orders)
            {
                if (!dbContext.Orders.Any(o => o.ExternalId == order.ExternalId && o.Price == order.Price))
                {
                    autoMapIdentityMatched = false;
                    break;
                }
            }

            Assert.IsTrue(result.RowsAffected == oldTotal + ordersToAdd, "The number of rows inserted must match the count of order list");
            Assert.IsTrue(result.RowsUpdated == ordersToUpdate, "The number of rows updated must match");
            Assert.IsTrue(result.RowsInserted == ordersToAdd, "The number of rows added must match");
            Assert.IsTrue(result.RowsDeleted == oldTotal - orders.Count() + ordersToAdd, "The number of rows deleted must match the difference from the total existing orders to the new orders to add/update");
            Assert.IsTrue(autoMapIdentityMatched, "The auto mapping of ids of entities that were merged failed to match up");
        }
        [TestMethod]
        public void With_Options_MergeOnCondition()
        {
            var dbContext = SetupDbContext(true);
            int oldTotal = dbContext.Orders.Count();
            var orders = dbContext.Orders.Where(o => o.Id <= 100 && o.ExternalId != null).OrderBy(o => o.Id).ToList();
            int ordersToAdd = 50;
            int ordersToUpdate = orders.Count;
            foreach (var order in orders)
            {
                order.Price = Convert.ToDecimal(order.Id + .25);
            }
            for (int i = 0; i < ordersToAdd; i++)
            {
                orders.Add(new Order { Id = 100000 + i, Price = 3.55M });
            }
            var result = dbContext.BulkSync(orders, new BulkSyncOptions<Order>
            {
                MergeOnCondition = (s, t) => s.ExternalId == t.ExternalId,
                BatchSize = 1000
            });
            var newOrders = dbContext.Orders.OrderBy(o => o.Id).ToList();
            bool areAddedOrdersMerged = true;
            bool areUpdatedOrdersMerged = true;
            foreach (var newOrder in newOrders.Where(o => o.Id <= 100 && o.ExternalId != null).OrderBy(o => o.Id))
            {
                if (newOrder.Price != Convert.ToDecimal(newOrder.Id + .25))
                {
                    areUpdatedOrdersMerged = false;
                    break;
                }
            }
            foreach (var newOrder in newOrders.Where(o => o.Id >= 500000).OrderBy(o => o.Id))
            {
                if (newOrder.Price != 3.55M)
                {
                    areAddedOrdersMerged = false;
                    break;
                }
            }

            Assert.IsTrue(result.RowsAffected == oldTotal + ordersToAdd, "The number of rows inserted must match the count of order list");
            Assert.IsTrue(result.RowsUpdated == ordersToUpdate, "The number of rows updated must match");
            Assert.IsTrue(result.RowsInserted == ordersToAdd, "The number of rows added must match");
            Assert.IsTrue(result.RowsDeleted == oldTotal - orders.Count() + ordersToAdd, "The number of rows deleted must match the difference from the total existing orders to the new orders to add/update");
            Assert.IsTrue(areAddedOrdersMerged, "The orders that were added did not merge correctly");
            Assert.IsTrue(areUpdatedOrdersMerged, "The orders that were updated did not merge correctly");
        }
    }
}
