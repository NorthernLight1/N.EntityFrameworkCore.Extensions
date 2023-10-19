using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions
{
    [TestClass]
    public class BulkSaveChangesAsync : DbContextExtensionsBase
    {
        [TestMethod]
        public async Task With_All_Changes()
        {
            var dbContext = SetupDbContext(true);
            var totalCount = dbContext.Orders.Count();

            //Add new orders
            var ordersToAdd = new List<Order>();
            for (int i = 0; i < 2000; i++)
            {
                ordersToAdd.Add(new Order { Id = -i, Price = 10.57M });
            }
            dbContext.Orders.AddRange(ordersToAdd);

            //Delete orders
            var ordersToDelete = dbContext.Orders.Where(o => o.Price <= 5).ToList();
            dbContext.Orders.RemoveRange(ordersToDelete);

            //Update existing orders
            var ordersToUpdate = dbContext.Orders.Where(o => o.Price > 5 && o.Price <= 10).ToList();
            foreach(var orderToUpdate in ordersToUpdate)
            {
                orderToUpdate.Price = 99M;
            }


            int rowsAffected = await dbContext.BulkSaveChangesAsync();
            int ordersAddedCount = dbContext.Orders.Where(o => o.Price == 10.57M).Count();
            int ordersDeletedCount = dbContext.Orders.Where(o => o.Price <= 5).Count();
            int ordersUpdatedCount = dbContext.Orders.Where(o => o.Price == 99M).Count();

            Assert.IsTrue(rowsAffected == ordersToAdd.Count + ordersToDelete.Count + ordersToUpdate.Count, "The number of rows affected must equal the sum of entities added, deleted and updated");
            Assert.IsTrue(ordersAddedCount == ordersToAdd.Count(), "The number of orders to add did not match what was expected.");
            Assert.IsTrue(ordersDeletedCount == 0, "The number of orders that was deleted did not match what was expected.");
            Assert.IsTrue(ordersUpdatedCount == ordersToUpdate.Count(), "The number of orders that was updated did not match what was expected.");
        }
        [TestMethod]
        public async Task With_Add_Changes()
        {
            var dbContext = SetupDbContext(true);
            var oldTotalCount = dbContext.Orders.Where(o => o.Price == 10.57M).Count();

            var ordersToAdd = new List<Order>();
            for (int i = 0; i < 2000; i++)
            {
                ordersToAdd.Add(new Order { Id = -i, Price = 10.57M });
            }
            dbContext.Orders.AddRange(ordersToAdd);

            int rowsAffected = await dbContext.BulkSaveChangesAsync();
            int newTotalCount = dbContext.Orders.Where(o => o.Price == 10.57M).Count();

            Assert.IsTrue(rowsAffected == ordersToAdd.Count, "The number of rows affected must equal the sum of entities added, deleted and updated");
            Assert.IsTrue(oldTotalCount + ordersToAdd.Count == newTotalCount, "The number of orders to add did not match what was expected.");
        }
        [TestMethod]
        public async Task With_Delete_Changes()
        {
            var dbContext = SetupDbContext(true);
            var oldTotalCount = dbContext.Orders.Where(o => o.Price <= 5).Count();

            //Delete orders
            var ordersToDelete = dbContext.Orders.Where(o => o.Price <= 5).ToList();
            dbContext.Orders.RemoveRange(ordersToDelete);

            int rowsAffected = await dbContext.BulkSaveChangesAsync();
            int newTotalCount = dbContext.Orders.Where(o => o.Price <= 5).Count();

            Assert.IsTrue(rowsAffected == ordersToDelete.Count, "The number of rows affected must equal the sum of entities added, deleted and updated");
            Assert.IsTrue(oldTotalCount - ordersToDelete.Count == newTotalCount, "The number of orders to add did not match what was expected.");
        }
        [TestMethod]
        public async Task With_Update_Changes()
        {
            var dbContext = SetupDbContext(true);
            var oldTotalCount = dbContext.Orders.Where(o => o.Price <= 10).Count();

            //Update existing orders
            var ordersToUpdate = dbContext.Orders.Where(o => o.Price <= 10).ToList();
            foreach (var orderToUpdate in ordersToUpdate)
            {
                orderToUpdate.Price = 99M;
            }

            int rowsAffected = await dbContext.BulkSaveChangesAsync();
            int newTotalCount = dbContext.Orders.Where(o => o.Price <= 10).Count();
            int expectedCount = dbContext.Orders.Where(o => o.Price == 99M).Count();

            Assert.IsTrue(rowsAffected == ordersToUpdate.Count, "The number of rows affected must equal the sum of entities added, deleted and updated");
            Assert.IsTrue(oldTotalCount - ordersToUpdate.Count == newTotalCount, "The number of orders to add did not match what was expected.");
        }
    }
}
