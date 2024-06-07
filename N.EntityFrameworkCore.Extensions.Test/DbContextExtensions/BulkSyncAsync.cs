using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Data;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions
{
    [TestClass]
    public class BulkSyncAsync : DbContextExtensionsBase
    {
        [TestMethod]
        public async Task With_Default_Options()
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
            var result = await dbContext.BulkSyncAsync(orders);
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
        public async Task With_Inheritance_Tpc()
        {
            var dbContext = SetupDbContext(true, PopulateDataMode.Tpc);
            var customers = await dbContext.TpcPeople.Where(o => o.Id <= 1000).OfType<TpcCustomer>().ToListAsync();
            int customersToAdd = 5000;
            int customersToUpdate = customers.Count;
            int customersToDelete = dbContext.TpcPeople.OfType<TpcCustomer>().Count() - customersToUpdate;

            foreach (var customer in customers)
            {
                customer.FirstName = "BulkSync_Tpc_Update";
            }
            for (int i = 0; i < customersToAdd; i++)
            {
                customers.Add(new TpcCustomer
                {
                    Id = 10000 + i,
                    FirstName = "BulkSync_Tpc_Add",
                    AddedDate = DateTime.UtcNow
                });
            }
            var result = await dbContext.BulkSyncAsync(customers, options => { options.MergeOnCondition = (s, t) => s.Id == t.Id; });
            int customersAdded = dbContext.TpcPeople.Where(o => o.FirstName == "BulkSync_Tpc_Add").OfType<TpcCustomer>().Count();
            int customersUpdated = dbContext.TpcPeople.Where(o => o.FirstName == "BulkSync_Tpc_Update").OfType<TpcCustomer>().Count();
            int newCustomerTotal = dbContext.TpcPeople.OfType<TpcCustomer>().Count();

            Assert.IsTrue(result.RowsAffected == customersAdded + customersToUpdate + customersToDelete, "The number of rows affected must match the sum of customers added, updated and deleted.");
            Assert.IsTrue(result.RowsUpdated == customersToUpdate, "The number of rows updated must match");
            Assert.IsTrue(result.RowsInserted == customersToAdd, "The number of rows added must match");
            Assert.IsTrue(result.RowsDeleted == customersToDelete, "The number of rows deleted must match the difference from the total existing orders to the new orders to add/update");
            Assert.IsTrue(customersToAdd == customersAdded, "The custmoers that were added did not merge correctly");
            Assert.IsTrue(customersToUpdate == customersUpdated, "The customers that were updated did not merge correctly");
            Assert.IsTrue(newCustomerTotal == customersToAdd + customersToUpdate, "The count of customers in the database should match the sum of customers added and updated.");
        }
        [TestMethod]
        public async Task With_Inheritance_Tph()
        {
            var dbContext = SetupDbContext(true, PopulateDataMode.Tph);
            var customers = await dbContext.TphCustomers.Where(o => o.Id <= 1000).ToListAsync();
            int customersToAdd = 5000;
            int customersToUpdate = customers.Count;
            int customersToDelete = dbContext.TphPeople.Count() - customersToUpdate;

            foreach (var customer in customers)
            {
                customer.FirstName = "BulkSync_Tph_Update";
            }
            for (int i = 0; i < customersToAdd; i++)
            {
                customers.Add(new TphCustomer
                {
                    Id = 10000 + i,
                    FirstName = "BulkSync_Tph_Add",
                    AddedDate = DateTime.UtcNow
                });
            }
            var result = await dbContext.BulkSyncAsync(customers, options => { options.UsePermanentTable = true; options.MergeOnCondition = (s, t) => s.Id == t.Id; });
            int customersAdded = dbContext.TphCustomers.Where(o => o.FirstName == "BulkSync_Tph_Add").Count();
            int customersUpdated = dbContext.TphCustomers.Where(o => o.FirstName == "BulkSync_Tph_Update").Count();
            int newCustomerTotal = dbContext.TphCustomers.Count();

            Assert.IsTrue(result.RowsAffected == customersAdded + customersToUpdate + customersToDelete, "The number of rows affected must match the sum of customers added, updated and deleted.");
            Assert.IsTrue(result.RowsUpdated == customersToUpdate, "The number of rows updated must match");
            Assert.IsTrue(result.RowsInserted == customersToAdd, "The number of rows added must match");
            Assert.IsTrue(result.RowsDeleted == customersToDelete, "The number of rows deleted must match the difference from the total existing orders to the new orders to add/update");
            Assert.IsTrue(customersToAdd == customersAdded, "The customers that were added did not merge correctly");
            Assert.IsTrue(customersToUpdate == customersUpdated, "The customers that were updated did not merge correctly");
            Assert.IsTrue(newCustomerTotal == customersToAdd + customersToUpdate, "The count of customers in the database should match the sum of customers added and updated.");
        }
        [TestMethod]
        public async Task With_Inheritance_Tpt()
        {
            var dbContext = SetupDbContext(true, PopulateDataMode.Tpt);
            var customers = await dbContext.TptPeople.Where(o => o.Id <= 1000).OfType<TptCustomer>().ToListAsync();
            int customersToAdd = 5000;
            int customersToUpdate = customers.Count;
            int customersToDelete = dbContext.TptCustomers.Count() - customersToUpdate;

            foreach (var customer in customers)
            {
                customer.FirstName = "BulkSync_Tpt_Update";
            }
            for (int i = 0; i < customersToAdd; i++)
            {
                customers.Add(new TptCustomer
                {
                    Id = 10000 + i,
                    FirstName = "BulkSync_Tpt_Add",
                    AddedDate = DateTime.UtcNow
                });
            }
            var result = await dbContext.BulkSyncAsync(customers, options => { options.MergeOnCondition = (s, t) => s.Id == t.Id; });
            int customersAdded = dbContext.TptPeople.Where(o => o.FirstName == "BulkSync_Tpt_Add").OfType<TptCustomer>().Count();
            int customersUpdated = dbContext.TptPeople.Where(o => o.FirstName == "BulkSync_Tpt_Update").OfType<TptCustomer>().Count();
            int newCustomerTotal = dbContext.TptPeople.OfType<TptCustomer>().Count();

            Assert.IsTrue(result.RowsAffected == customersAdded + customersToUpdate + customersToDelete, "The number of rows affected must match the sum of customers added, updated and deleted.");
            Assert.IsTrue(result.RowsUpdated == customersToUpdate, "The number of rows updated must match");
            Assert.IsTrue(result.RowsInserted == customersToAdd, "The number of rows added must match");
            Assert.IsTrue(result.RowsDeleted == customersToDelete, "The number of rows deleted must match the difference from the total existing orders to the new orders to add/update");
            Assert.IsTrue(customersToAdd == customersAdded, "The custmoers that were added did not merge correctly");
            Assert.IsTrue(customersToUpdate == customersUpdated, "The customers that were updated did not merge correctly");
            Assert.IsTrue(newCustomerTotal == customersToAdd + customersToUpdate, "The count of customers in the database should match the sum of customers added and updated.");
        }
        [TestMethod]
        public async Task With_Options_AutoMapIdentity()
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
            var result = await dbContext.BulkSyncAsync(orders, options => { options.MergeOnCondition = (s, t) => s.ExternalId == t.ExternalId; options.UsePermanentTable = true; });
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
        public async Task With_Options_MergeOnCondition()
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
            var result = await dbContext.BulkSyncAsync(orders, new BulkSyncOptions<Order>
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