using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions
{
    [TestClass]
    public class BulkMergeAsync : DbContextExtensionsBase
    {
        [TestMethod]
        public async Task With_Default_Options()
        {
            var dbContext = SetupDbContext(true);
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
            var result = await dbContext.BulkMergeAsync(orders);
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

            Assert.IsTrue(result.RowsAffected == orders.Count(), "The number of rows inserted must match the count of order list");
            Assert.IsTrue(result.RowsUpdated == ordersToUpdate, "The number of rows updated must match");
            Assert.IsTrue(result.RowsInserted == ordersToAdd, "The number of rows added must match");
            Assert.IsTrue(areAddedOrdersMerged, "The orders that were added did not merge correctly");
            Assert.IsTrue(areUpdatedOrdersMerged, "The orders that were updated did not merge correctly");
        }
        [TestMethod]
        public async Task With_Inheritance_Tpc()
        {
            var dbContext = SetupDbContext(true);
            var customers = dbContext.TpcPeople.Where(o => o.Id <= 1000).OfType<TpcCustomer>().ToList();
            int customersToAdd = 5000;
            int customersToUpdate = customers.Count;
            foreach (var customer in customers)
            {
                customer.FirstName = "BulkMerge_Tpc_Update";
            }
            for (int i = 0; i < customersToAdd; i++)
            {
                customers.Add(new TpcCustomer
                {
                    Id = 10000 + i,
                    FirstName = "BulkMerge_Tpc_Add",
                    AddedDate = DateTime.UtcNow
                });
            }
            var result = await dbContext.BulkMergeAsync(customers, options => { options.MergeOnCondition = (s, t) => s.Id == t.Id; });
            int customersAdded = dbContext.TpcPeople.Where(o => o.FirstName == "BulkMerge_Tpc_Add").OfType<TpcCustomer>().Count();
            int customersUpdated = dbContext.TpcPeople.Where(o => o.FirstName == "BulkMerge_Tpc_Update").OfType<TpcCustomer>().Count();

            Assert.IsTrue(result.RowsAffected == customers.Count, "The number of rows inserted must match the count of customer list");
            Assert.IsTrue(result.RowsUpdated == customersToUpdate, "The number of rows updated must match");
            Assert.IsTrue(result.RowsInserted == customersToAdd, "The number of rows added must match");
            Assert.IsTrue(customersToAdd == customersAdded, "The custmoers that were added did not merge correctly");
            Assert.IsTrue(customersToUpdate == customersUpdated, "The customers that were updated did not merge correctly");
        }
        [TestMethod]
        public async Task With_Inheritance_Tph()
        {
            var dbContext = SetupDbContext(true);
            var customers = dbContext.TphPeople.Where(o => o.Id <= 1000).OfType<TphCustomer>().ToList();
            int customersToAdd = 5000;
            int customersToUpdate = customers.Count;
            foreach (var customer in customers)
            {
                customer.FirstName = "BulkMerge_Tph_Update";
            }
            for (int i = 0; i < customersToAdd; i++)
            {
                customers.Add(new TphCustomer
                {
                    Id = 10000 + i,
                    FirstName = "BulkMerge_Tph_Add",
                    AddedDate = DateTime.UtcNow
                });
            }
            var result = await dbContext.BulkMergeAsync(customers);
            int customersAdded = dbContext.TphPeople.Where(o => o.FirstName == "BulkMerge_Tph_Add").OfType<TphCustomer>().Count();
            int customersUpdated = dbContext.TphPeople.Where(o => o.FirstName == "BulkMerge_Tph_Update").OfType<TphCustomer>().Count();

            Assert.IsTrue(result.RowsAffected == customers.Count(), "The number of rows inserted must match the count of customer list");
            Assert.IsTrue(result.RowsUpdated == customersToUpdate, "The number of rows updated must match");
            Assert.IsTrue(result.RowsInserted == customersToAdd, "The number of rows added must match");
            Assert.IsTrue(customersToAdd == customersAdded, "The custmoers that were added did not merge correctly");
            Assert.IsTrue(customersToUpdate == customersUpdated, "The customers that were updated did not merge correctly");
        }
        [TestMethod]
        public async Task With_Default_Options_MergeOnCondition()
        {
            var dbContext = SetupDbContext(true);
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
            var result = await dbContext.BulkMergeAsync(orders, options => { options.MergeOnCondition = (s, t) => s.ExternalId == t.ExternalId; options.BatchSize = 1000; });
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

            Assert.IsTrue(result.RowsAffected == orders.Count(), "The number of rows inserted must match the count of order list");
            Assert.IsTrue(result.RowsUpdated == ordersToUpdate, "The number of rows updated must match");
            Assert.IsTrue(result.RowsInserted == ordersToAdd, "The number of rows added must match");
            Assert.IsTrue(areAddedOrdersMerged, "The orders that were added did not merge correctly");
            Assert.IsTrue(areUpdatedOrdersMerged, "The orders that were updated did not merge correctly");
        }
        [TestMethod]
        public async Task With_Options_AutoMapIdentity()
        {
            var dbContext = SetupDbContext(true);
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
            var result = await dbContext.BulkMergeAsync(orders, new BulkMergeOptions<Order>
            {
                MergeOnCondition = (s, t) => s.ExternalId == t.ExternalId,
                UsePermanentTable = true
            });
            bool autoMapIdentityMatched = true;
            foreach (var order in orders)
            {
                if (!dbContext.Orders.Any(o => o.ExternalId == order.ExternalId && o.Id == order.Id && o.Price == order.Price))
                {
                    autoMapIdentityMatched = false;
                    break;
                }
            }

            Assert.IsTrue(result.RowsAffected == ordersToAdd + ordersToUpdate, "The number of rows inserted must match the count of order list");
            Assert.IsTrue(result.RowsUpdated == ordersToUpdate, "The number of rows updated must match");
            Assert.IsTrue(result.RowsInserted == ordersToAdd, "The number of rows added must match");
            Assert.IsTrue(autoMapIdentityMatched, "The auto mapping of ids of entities that were merged failed to match up");
        }
        [TestMethod]
        public async Task With_Key()
        {
            var dbContext = SetupDbContext(true);
            var products = dbContext.Products.Where(o => o.Price == 1.25M).OrderBy(o => o.Id).ToList();
            int productsToAdd = 5000;
            var productsToUpdate = products.ToList();
            foreach (var product in products)
            {
                product.Price = Convert.ToDecimal(product.Id) + .25M;
            }
            for (int i = 0; i < productsToAdd; i++)
            {
                products.Add(new Product { Id = (20000 + i).ToString(), Price = 3.55M });
            }
            var result = await dbContext.BulkMergeAsync(products);
            var newProducts = dbContext.Products.OrderBy(o => o.Id).ToList();
            bool areAddedOrdersMerged = true;
            bool areUpdatedOrdersMerged = true;
            foreach (var newProduct in newProducts.Where(o => productsToUpdate.Select(o => o.Id).Contains(o.Id)))
            {
                if (newProduct.Price != Convert.ToDecimal(newProduct.Id) + .25M)
                {
                    areUpdatedOrdersMerged = false;
                    break;
                }
            }
            foreach (var newProduct in newProducts.Where(o => Convert.ToInt32(o.Id) >= 20000).OrderBy(o => o.Id))
            {
                if (newProduct.Price != 3.55M)
                {
                    areAddedOrdersMerged = false;
                    break;
                }
            }

            Assert.IsTrue(result.RowsAffected == products.Count(), "The number of rows inserted must match the count of order list");
            Assert.IsTrue(result.RowsUpdated == productsToUpdate.Count, "The number of rows updated must match");
            Assert.IsTrue(result.RowsInserted == productsToAdd, "The number of rows added must match");
            Assert.IsTrue(areAddedOrdersMerged, "The orders that were added did not merge correctly");
            Assert.IsTrue(areUpdatedOrdersMerged, "The orders that were updated did not merge correctly");
        }
        [TestMethod]
        public async Task With_Transaction()
        {
            var dbContext = SetupDbContext(true);
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
            BulkMergeResult<Order> result;
            using (var transaction = dbContext.Database.BeginTransaction())
            {
                result = await dbContext.BulkMergeAsync(orders);
                transaction.Rollback();
            }
            int ordersUpdated = dbContext.Orders.Count(o => o.Id <= 10000 && o.Price == ((decimal)o.Id + .25M) && o.Price != 1.25M);
            int ordersAdded = dbContext.Orders.Count(o => o.Id >= 100000);

            Assert.IsTrue(result.RowsAffected == orders.Count(), "The number of rows inserted must match the count of order list");
            Assert.IsTrue(result.RowsUpdated == ordersToUpdate, "The number of rows updated must match");
            Assert.IsTrue(result.RowsInserted == ordersToAdd, "The number of rows added must match");
            Assert.IsTrue(ordersAdded == 0, "The number of rows added must equal 0 since transaction was rollbacked");
            Assert.IsTrue(ordersUpdated == 0, "The number of rows updated must equal 0 since transaction was rollbacked");
        }
        [TestMethod]
        public async Task With_ValueGenerated_Default()
        {
            var dbContext = SetupDbContext(false);
            var nowDateTime = DateTime.Now;
            var orders = new List<Order>();
            for (int i = 0; i < 1000; i++)
            {
                orders.Add(new Order { Id = i, Price = 1.57M });
            }
            int oldTotal = dbContext.Orders.Where(o => o.DbAddedDateTime > nowDateTime).Count();
            var mergeResult = await dbContext.BulkMergeAsync(orders);
            int newTotal = dbContext.Orders.Where(o => o.Price <= 1.57M
                && o.DbAddedDateTime > nowDateTime).Count();

            Assert.IsTrue(mergeResult.RowsInserted == orders.Count, "The number of rows inserted must match the count of order list");
            Assert.IsTrue(newTotal - oldTotal == mergeResult.RowsInserted, "The new count minus the old count should match the number of rows inserted.");
        }
        [TestMethod]
        public async Task With_ValueGenerated_Computed()
        {
            var dbContext = SetupDbContext(false);
            var nowDateTime = DateTime.Now;
            var orders = new List<Order>();
            for (int i = 0; i < 20000; i++)
            {
                orders.Add(new Order { Id = i, Price = 1.57M, DbModifiedDateTime = nowDateTime });
            }
            int oldTotal = dbContext.Orders.Where(o => o.Price <= 10).Count();
            var result = await dbContext.BulkMergeAsync(orders);
            int newTotal = dbContext.Orders.Where(o => o.Price <= 10 && o.DbModifiedDateTime > nowDateTime).Count();

            Assert.IsTrue(result.RowsInserted == orders.Count(), "The number of rows inserted must match the count of order list");
            Assert.IsTrue(newTotal - oldTotal == result.RowsInserted, "The new count minus the old count should match the number of rows inserted.");
        }
    }
}