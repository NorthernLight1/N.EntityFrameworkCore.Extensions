using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions
{
    [TestClass]
    public class BulkSaveChanges : DbContextExtensionsBase
    {
        [TestMethod]
        public void With_Default_Options()
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
            foreach (var orderToUpdate in ordersToUpdate)
            {
                orderToUpdate.Price = 99M;
            }


            int rowsAffected = dbContext.BulkSaveChanges();
            int ordersAddedCount = dbContext.Orders.Where(o => o.Price == 10.57M).Count();
            int ordersDeletedCount = dbContext.Orders.Where(o => o.Price <= 5).Count();
            int ordersUpdatedCount = dbContext.Orders.Where(o => o.Price == 99M).Count();

            Assert.IsTrue(rowsAffected == ordersToAdd.Count + ordersToDelete.Count + ordersToUpdate.Count, "The number of rows affected must equal the sum of entities added, deleted and updated");
            Assert.IsTrue(ordersAddedCount == ordersToAdd.Count(), "The number of orders to add did not match what was expected.");
            Assert.IsTrue(ordersDeletedCount == 0, "The number of orders that was deleted did not match what was expected.");
            Assert.IsTrue(ordersUpdatedCount == ordersToUpdate.Count(), "The number of orders that was updated did not match what was expected.");
        }
        [TestMethod]
        public void With_Add_Changes()
        {
            var dbContext = SetupDbContext(true);
            var oldTotalCount = dbContext.Orders.Where(o => o.Price == 10.57M).Count();

            var ordersToAdd = new List<Order>();
            for (int i = 0; i < 2000; i++)
            {
                ordersToAdd.Add(new Order { Id = -i, Price = 10.57M });
            }
            dbContext.Orders.AddRange(ordersToAdd);

            int rowsAffected = dbContext.BulkSaveChanges();
            int newTotalCount = dbContext.Orders.Where(o => o.Price == 10.57M).Count();

            Assert.IsTrue(rowsAffected == ordersToAdd.Count, "The number of rows affected must equal the sum of entities added, deleted and updated");
            Assert.IsTrue(oldTotalCount + ordersToAdd.Count == newTotalCount, "The number of orders to add did not match what was expected.");
        }
        [TestMethod]
        public void With_Delete_Changes()
        {
            var dbContext = SetupDbContext(true);
            var oldTotalCount = dbContext.Orders.Where(o => o.Price <= 5).Count();

            //Delete orders
            var ordersToDelete = dbContext.Orders.Where(o => o.Price <= 5).ToList();
            dbContext.Orders.RemoveRange(ordersToDelete);

            int rowsAffected = dbContext.BulkSaveChanges();
            int newTotalCount = dbContext.Orders.Where(o => o.Price <= 5).Count();

            Assert.IsTrue(rowsAffected == ordersToDelete.Count, "The number of rows affected must equal the sum of entities added, deleted and updated");
            Assert.IsTrue(oldTotalCount - ordersToDelete.Count == newTotalCount, "The number of orders to add did not match what was expected.");
        }
        [TestMethod]
        public void With_Update_Changes()
        {
            var dbContext = SetupDbContext(true);
            var oldTotalCount = dbContext.Orders.Where(o => o.Price <= 10).Count();

            //Update existing orders
            var ordersToUpdate = dbContext.Orders.Where(o => o.Price <= 10).ToList();
            foreach (var orderToUpdate in ordersToUpdate)
            {
                orderToUpdate.Price = 99M;
            }

            int rowsAffected = dbContext.BulkSaveChanges();
            int newTotalCount = dbContext.Orders.Where(o => o.Price <= 10).Count();
            int expectedCount = dbContext.Orders.Where(o => o.Price == 99M).Count();

            Assert.IsTrue(rowsAffected == ordersToUpdate.Count, "The number of rows affected must equal the sum of entities added, deleted and updated");
            Assert.IsTrue(oldTotalCount - ordersToUpdate.Count == newTotalCount, "The number of orders to add did not match what was expected.");
        }
        [TestMethod]
        public void With_Inheritance_Tpc()
        {
            var dbContext = SetupDbContext(true, PopulateDataMode.Tpc);
            //Delete Customers
            var customersToDelete = dbContext.TpcPeople.OfType<TpcCustomer>().Where(o => o.Id <= 1000);
            int expectedRowsDeleted = customersToDelete.Count();
            dbContext.TpcPeople.RemoveRange(customersToDelete);
            //Update Customers
            var customersToUpdate = dbContext.TpcPeople.OfType<TpcCustomer>().Where(o => o.Id > 1000 && o.Id <= 1500);
            int expectedRowsUpdated = customersToUpdate.Count();
            foreach (var customerToUpdate in customersToUpdate)
            {
                customerToUpdate.FirstName = "CustomerUpdated";
            }
            //Add New Customers
            long maxId = dbContext.TpcPeople.OfType<TpcCustomer>().Max(o => o.Id);
            int expectedRowsAdded = 3000;
            for (long i = maxId+1; i <= maxId+expectedRowsAdded; i++)
            {
                dbContext.TpcPeople.Add(new TpcCustomer
                {
                    Id = i,
                    FirstName = string.Format("John_{0}", i),
                    LastName = string.Format("Smith_{0}", i),
                    Email = string.Format("john.smith{0}@domain.com", i),
                    AddedDate = DateTime.UtcNow
                });
            }
            int rowsAffected = dbContext.BulkSaveChanges();
            int rowsAfterDelete = customersToDelete.Count();
            int rowsUpdated = customersToUpdate.Where(o => o.FirstName == "CustomerUpdated").Count();
            int rowsAdded = dbContext.TpcPeople.OfType<TpcCustomer>().Where(o => o.Id > maxId).Count();
            int expectedRowsAffected = expectedRowsDeleted + expectedRowsUpdated + expectedRowsAdded;

            Assert.IsTrue(rowsAfterDelete == 0, "The number of rows deleted not not match what was expected.");
            Assert.IsTrue(rowsUpdated == expectedRowsUpdated, "The number of rows updated not not match what was expected.");
            Assert.IsTrue(rowsAdded == expectedRowsAdded, "The number of rows added not not match what was expected.");
            Assert.IsTrue(rowsAffected == expectedRowsAffected, "The new count minus the old count should match the number of rows inserted.");
        }
        [TestMethod]
        public void With_Inheritance_Tph()
        {
            var dbContext = SetupDbContext(true, PopulateDataMode.Tph);
            //Delete Customers
            var customersToDelete = dbContext.TphCustomers.Where(o => o.Id <= 1000);
            int expectedRowsDeleted = customersToDelete.Count();
            dbContext.TphCustomers.RemoveRange(customersToDelete);
            //Update Customers
            var customersToUpdate = dbContext.TphCustomers.Where(o => o.Id > 1000 && o.Id <= 1500);
            int expectedRowsUpdated = customersToUpdate.Count();
            foreach (var customerToUpdate in customersToUpdate)
            {
                customerToUpdate.FirstName = "CustomerUpdated";
            }
            //Add New Customers
            long maxId = dbContext.TphPeople.Max(o => o.Id);
            int expectedRowsAdded = 3000;
            for (long i = maxId + 1; i <= maxId + expectedRowsAdded; i++)
            {
                dbContext.TphCustomers.Add(new TphCustomer
                {
                    Id = i,
                    FirstName = string.Format("John_{0}", i),
                    LastName = string.Format("Smith_{0}", i),
                    Email = string.Format("john.smith{0}@domain.com", i),
                    AddedDate = DateTime.UtcNow
                });
            }
            int rowsAffected = dbContext.BulkSaveChanges();
            int rowsAfterDelete = customersToDelete.Count();
            int rowsUpdated = customersToUpdate.Where(o => o.FirstName == "CustomerUpdated").Count();
            int rowsAdded = dbContext.TphCustomers.Where(o => o.Id > maxId).Count();
            int expectedRowsAffected = expectedRowsDeleted + expectedRowsUpdated + expectedRowsAdded;

            Assert.IsTrue(expectedRowsDeleted > 0, "The expected number of rows to delete must be greater than zero.");
            Assert.IsTrue(rowsAfterDelete == 0, "The number of rows deleted not not match what was expected.");
            Assert.IsTrue(rowsUpdated == expectedRowsUpdated, "The number of rows updated not not match what was expected.");
            Assert.IsTrue(rowsAdded == expectedRowsAdded, "The number of rows added not not match what was expected.");
            Assert.IsTrue(rowsAffected == expectedRowsAffected, "The new count minus the old count should match the number of rows inserted.");
        }
        [TestMethod]
        public void With_Inheritance_Tpt()
        {
            var dbContext = SetupDbContext(true, PopulateDataMode.Tpt);
            //Delete Customers
            var customersToDelete = dbContext.TptCustomers.Where(o => o.Id <= 1000);
            int expectedRowsDeleted = customersToDelete.Count();
            dbContext.TptCustomers.RemoveRange(customersToDelete);
            //Update Customers
            var customersToUpdate = dbContext.TptCustomers.Where(o => o.Id > 1000 && o.Id <= 1500);
            int expectedRowsUpdated = customersToUpdate.Count();
            foreach (var customerToUpdate in customersToUpdate)
            {
                customerToUpdate.FirstName = "CustomerUpdated";
            }
            //Add New Customers
            long maxId = dbContext.TptPeople.Max(o => o.Id);
            int expectedRowsAdded = 3000;
            for (long i = maxId + 1; i <= maxId + expectedRowsAdded; i++)
            {
                dbContext.TptCustomers.Add(new TptCustomer
                {
                    Id = i,
                    FirstName = string.Format("John_{0}", i),
                    LastName = string.Format("Smith_{0}", i),
                    Email = string.Format("john.smith{0}@domain.com", i),
                    AddedDate = DateTime.UtcNow
                });
            }
            int rowsAffected = dbContext.BulkSaveChanges();
            int rowsAfterDelete = customersToDelete.Count();
            int rowsUpdated = customersToUpdate.Where(o => o.FirstName == "CustomerUpdated").Count();
            int rowsAdded = dbContext.TptCustomers.Where(o => o.Id > maxId).Count();
            int expectedRowsAffected = expectedRowsDeleted + expectedRowsUpdated + expectedRowsAdded;

            Assert.IsTrue(rowsAfterDelete == 0, "The number of rows deleted not not match what was expected.");
            Assert.IsTrue(rowsUpdated == expectedRowsUpdated, "The number of rows updated not not match what was expected.");
            Assert.IsTrue(rowsAdded == expectedRowsAdded, "The number of rows added not not match what was expected.");
            Assert.IsTrue(rowsAffected == expectedRowsAffected, "The new count minus the old count should match the number of rows inserted.");
        }
    }
}
