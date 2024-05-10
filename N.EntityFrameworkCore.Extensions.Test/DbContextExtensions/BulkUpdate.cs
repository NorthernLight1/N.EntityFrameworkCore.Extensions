using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Data;
using System;
using System.Collections.Generic;
using System.Linq;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions
{
    [TestClass]
    public class BulkUpdate : DbContextExtensionsBase
    {
        [TestMethod]
        public void With_ComplexKey()
        {
            var dbContext = SetupDbContext(true);
            var products = dbContext.ProductsWithComplexKey.Where(o => o.Price == 1.25M).ToList();
            foreach (var product in products)
            {
                product.Price = 2.35M;
            }
            var oldTotal = dbContext.ProductsWithComplexKey.Where(o => o.Price == 2.35M).Count();
            int rowsUpdated = dbContext.BulkUpdate(products);
            var newTotal = dbContext.ProductsWithComplexKey.Where(o => o.Price == 2.35M).Count();

            Assert.IsTrue(products.Count > 0, "There must be orders in database that match this condition (Price = $1.25)");
            Assert.IsTrue(rowsUpdated == products.Count, "The number of rows updated must match the count of entities that were retrieved");
            Assert.IsTrue(newTotal == rowsUpdated + oldTotal, "The count of new orders must be equal the number of rows updated in the database.");
        }
        [TestMethod]
        public void With_Default_Options()
        {
            var dbContext = SetupDbContext(true);
            var orders = dbContext.Orders.Where(o => o.Price == 1.25M).OrderBy(o => o.Id).ToList();
            long maxId = 0;
            foreach (var order in orders)
            {
                order.Price = 2.35M;
                maxId = order.Id;
            }
            int rowsUpdated = dbContext.BulkUpdate(orders);
            var newOrders = dbContext.Orders.Where(o => o.Price == 2.35M).OrderBy(o => o.Id).Count();
            int entitiesWithChanges = dbContext.ChangeTracker.Entries().Where(t => t.State == EntityState.Modified).Count();

            Assert.IsTrue(orders.Count > 0, "There must be orders in database that match this condition (Price = $1.25)");
            Assert.IsTrue(rowsUpdated == orders.Count, "The number of rows updated must match the count of entities that were retrieved");
            Assert.IsTrue(newOrders == rowsUpdated, "The count of new orders must be equal the number of rows updated in the database.");
        }
        [TestMethod]
        public void With_Inheritance_Tpc()
        {
            var dbContext = SetupDbContext(true, PopulateDataMode.Tpc);
            var customers = dbContext.TpcPeople.Where(o => o.LastName != "BulkUpdate_Tpc").OfType<TpcCustomer>().ToList();
            var vendors = dbContext.TpcPeople.OfType<TpcVendor>().ToList();
            foreach (var customer in customers)
            {
                customer.FirstName = string.Format("Id={0}", customer.Id);
                customer.LastName = "BulkUpdate_Tpc";
            }
            int rowsUpdated = dbContext.BulkUpdate(customers);
            var newCustomers = dbContext.TpcPeople.Where(o => o.LastName == "BulkUpdate_Tpc").OfType<TpcCustomer>().Count();
            int entitiesWithChanges = dbContext.ChangeTracker.Entries().Where(t => t.State == EntityState.Modified).Count();

            Assert.IsTrue(vendors.Count > 0 && vendors.Count != customers.Count, "There should be vendor records in the database");
            Assert.IsTrue(customers.Count > 0, "There must be customers in database that match this condition (Price = $1.25)");
            Assert.IsTrue(rowsUpdated == customers.Count, "The number of rows updated must match the count of entities that were retrieved");
            Assert.IsTrue(newCustomers == rowsUpdated, "The count of new customers must be equal the number of rows updated in the database.");
        }
        [TestMethod]
        public void With_Inheritance_Tph()
        {
            var dbContext = SetupDbContext(true, PopulateDataMode.Tph);
            var customers = dbContext.TphPeople.Where(o => o.LastName != "BulkUpdateTest").OfType<TphCustomer>().ToList();
            var vendors = dbContext.TphPeople.OfType<TphVendor>().ToList();
            foreach (var customer in customers)
            {
                customer.FirstName = string.Format("Id={0}", customer.Id);
                customer.LastName = "BulkUpdateTest";
            }
            int rowsUpdated = dbContext.BulkUpdate(customers);
            var newCustomers = dbContext.TphPeople.Where(o => o.LastName == "BulkUpdateTest").OrderBy(o => o.Id).Count();
            int entitiesWithChanges = dbContext.ChangeTracker.Entries().Where(t => t.State == EntityState.Modified).Count();

            Assert.IsTrue(vendors.Count > 0 && vendors.Count != customers.Count, "There should be vendor records in the database");
            Assert.IsTrue(customers.Count > 0, "There must be customers in database that match this condition (Price = $1.25)");
            Assert.IsTrue(rowsUpdated == customers.Count, "The number of rows updated must match the count of entities that were retrieved");
            Assert.IsTrue(newCustomers == rowsUpdated, "The count of new customers must be equal the number of rows updated in the database.");
        }
        [TestMethod]
        public void With_Inheritance_Tpt()
        {
            var dbContext = SetupDbContext(true, PopulateDataMode.Tpt);
            var customers = dbContext.TptCustomers.Where(o => o.LastName != "BulkUpdateTest").ToList();
            foreach (var customer in customers)
            {
                customer.FirstName = string.Format("Id={0}", customer.Id);
                customer.LastName = "BulkUpdateTest";
            }
            int rowsUpdated = dbContext.BulkUpdate(customers);
            var newCustomers = dbContext.TptCustomers.Where(o => o.LastName == "BulkUpdateTest").OrderBy(o => o.Id).Count();
            //int entitiesWithChanges = dbContext.ChangeTracker.Entries().Where(t => t.State == EntityState.Modified).Count();

            Assert.IsTrue(customers.Count > 0, "There must be customers in database that match this condition (Price = $1.25)");
            Assert.IsTrue(rowsUpdated == customers.Count, "The number of rows updated must match the count of entities that were retrieved");
            Assert.IsTrue(newCustomers == rowsUpdated, "The count of new customers must be equal the number of rows updated in the database.");
        }
        [TestMethod]
        public void With_Options_InputColumns_PropertyExpression()
        {
            var dbContext = SetupDbContext(true);
            var orders = dbContext.Orders.Where(o => o.Price == 1.25M && o.ExternalId != null).OrderBy(o => o.Id).ToList();
            foreach (var order in orders)
            {
                order.Price = 2.35M;
                order.ExternalId = null;
            }
            var oldTotal = dbContext.Orders.Where(o => o.Price == 2.35M && o.ExternalId != null).Count();
            int rowsUpdated = dbContext.BulkUpdate(orders, options => { options.InputColumns = o => o.Price; });
            var newTotal1 = dbContext.Orders.Where(o => o.Price == 2.35M && o.ExternalId != null).Count();
            var newTotal2 = dbContext.Orders.Where(o => o.Price == 1.25M && o.ExternalId != null).Count();

            Assert.IsTrue(orders.Count > 0, "There must be orders in database that match this condition (Price = $1.25)");
            Assert.IsTrue(newTotal1 == rowsUpdated + oldTotal, "The count of new orders must be equal the number of rows updated in the database.");
            Assert.IsTrue(newTotal2 == 0, "There should be not records with condition (Price = $1.25)");
        }
        [TestMethod]
        public void With_Options_InputColumns_NewExpression()
        {
            var dbContext = SetupDbContext(true);
            var orders = dbContext.Orders.Where(o => o.Price == 1.25M && o.ExternalId != null).OrderBy(o => o.Id).ToList();
            foreach (var order in orders)
            {
                order.Price = 2.35M;
                order.ExternalId = null;
            }
            var oldTotal = dbContext.Orders.Where(o => o.Price == 2.35M && o.ExternalId != null).Count();
            int rowsUpdated = dbContext.BulkUpdate(orders, options => { options.InputColumns = o => new { o.Price }; });
            var newTotal1 = dbContext.Orders.Where(o => o.Price == 2.35M && o.ExternalId != null).Count();
            var newTotal2 = dbContext.Orders.Where(o => o.Price == 1.25M && o.ExternalId != null).Count();

            Assert.IsTrue(orders.Count > 0, "There must be orders in database that match this condition (Price = $1.25)");
            Assert.IsTrue(newTotal1 == rowsUpdated + oldTotal, "The count of new orders must be equal the number of rows updated in the database.");
            Assert.IsTrue(newTotal2 == 0, "There should be not records with condition (Price = $1.25)");
        }
        [TestMethod]
        public void With_Options_IgnoreColumns_PropertyExpression()
        {
            var dbContext = SetupDbContext(true);
            var orders = dbContext.Orders.Where(o => o.Price == 1.25M && o.ExternalId != null).OrderBy(o => o.Id).ToList();
            foreach (var order in orders)
            {
                order.Price = 2.35M;
                order.ExternalId = null;
            }
            var oldTotal = dbContext.Orders.Where(o => o.Price == 2.35M && o.ExternalId != null).Count();
            int rowsUpdated = dbContext.BulkUpdate(orders, options => { options.IgnoreColumns = o => o.ExternalId; });
            var newTotal1 = dbContext.Orders.Where(o => o.Price == 2.35M && o.ExternalId != null).Count();
            var newTotal2 = dbContext.Orders.Where(o => o.Price == 1.25M && o.ExternalId != null).Count();

            Assert.IsTrue(orders.Count > 0, "There must be orders in database that match this condition (Price = $1.25)");
            Assert.IsTrue(newTotal1 == rowsUpdated + oldTotal, "The count of new orders must be equal the number of rows updated in the database.");
            Assert.IsTrue(newTotal2 == 0, "There should be not records with condition (Price = $1.25)");
        }
        [TestMethod]
        public void With_Options_IgnoreColumns_NewExpression()
        {
            var dbContext = SetupDbContext(true);
            var orders = dbContext.Orders.Where(o => o.Price == 1.25M && o.ExternalId != null).OrderBy(o => o.Id).ToList();
            foreach (var order in orders)
            {
                order.Price = 2.35M;
                order.ExternalId = null;
            }
            var oldTotal = dbContext.Orders.Where(o => o.Price == 2.35M && o.ExternalId != null).Count();
            int rowsUpdated = dbContext.BulkUpdate(orders, options => { options.IgnoreColumns = o => new { o.ExternalId }; });
            var newTotal1 = dbContext.Orders.Where(o => o.Price == 2.35M && o.ExternalId != null).Count();
            var newTotal2 = dbContext.Orders.Where(o => o.Price == 1.25M && o.ExternalId != null).Count();

            Assert.IsTrue(orders.Count > 0, "There must be orders in database that match this condition (Price = $1.25)");
            Assert.IsTrue(newTotal1 == rowsUpdated + oldTotal, "The count of new orders must be equal the number of rows updated in the database.");
            Assert.IsTrue(newTotal2 == 0, "There should be not records with condition (Price = $1.25)");
        }
        [TestMethod]
        public void With_Options_UpdateOnCondition()
        {
            var dbContext = SetupDbContext(true);
            var orders = dbContext.Orders.Where(o => o.Price == 1.25M).OrderBy(o => o.Id).ToList();
            int ordersWithExternalId = orders.Where(o => o.ExternalId != null).Count();
            foreach (var order in orders)
            {
                order.Price = 2.35M;
            }
            var oldTotal = dbContext.Orders.Where(o => o.Price == 2.35M && o.ExternalId != null).Count();
            int rowsUpdated = dbContext.BulkUpdate(orders, options => { options.UpdateOnCondition = (s, t) => s.ExternalId == t.ExternalId; });
            var newTotal = dbContext.Orders.Where(o => o.Price == 2.35M && o.ExternalId != null).Count();

            Assert.IsTrue(orders.Count > 0, "There must be orders in database that match this condition (Price = $1.25)");
            Assert.IsTrue(rowsUpdated == ordersWithExternalId, "The number of rows updated must match the count of entities that were retrieved");
            Assert.IsTrue(newTotal == rowsUpdated + oldTotal, "The count of new orders must be equal the number of rows updated in the database.");
        }
        [TestMethod]
        public void With_Options_UpdateOnCondition_Enum()
        {
            var dbContext = SetupDbContext(true);
            var products = dbContext.Products.Where(o => o.Price == 1.25M).OrderBy(o => o.Id).ToList();
            foreach (var product in products)
            {
                product.Price = 2.35M;
            }
            int rowsUpdated = dbContext.BulkUpdate(products, o =>
            {
                o.UpdateOnCondition = (s, t) => s.Id == t.Id && s.StatusEnum == t.StatusEnum;
            });
            var newProducts = dbContext.Products.Where(o => o.Price == 2.35M).OrderBy(o => o.Id).Count();

            Assert.IsTrue(products.Count > 0, "There must be products in database that match this condition (Price = $1.25)");
            Assert.IsTrue(rowsUpdated == products.Count, "The number of rows updated must match the count of entities that were retrieved");
            Assert.IsTrue(newProducts == rowsUpdated, "The count of new products must be equal the number of rows updated in the database.");
        }
        [TestMethod]
        public void With_Transaction()
        {
            var dbContext = SetupDbContext(true);
            var orders = dbContext.Orders.Where(o => o.Price == 1.25M).OrderBy(o => o.Id).ToList();
            long maxId = 0;
            foreach (var order in orders)
            {
                order.Price = 2.35M;
                maxId = order.Id;
            }
            int rowsUpdated, newOrders;
            using (var transaction = dbContext.Database.BeginTransaction())
            {
                rowsUpdated = dbContext.BulkUpdate(orders);
                newOrders = dbContext.Orders.Where(o => o.Price == 2.35M).Count();
                transaction.Rollback();
            }
            int rollbackTotal = dbContext.Orders.Where(o => o.Price == 1.25M).Count();

            Assert.IsTrue(orders.Count > 0, "There must be orders in database that match this condition (Price = $1.25)");
            Assert.IsTrue(rowsUpdated == orders.Count, "The number of rows updated must match the count of entities that were retrieved");
            Assert.IsTrue(newOrders == rowsUpdated, "The count of new orders must be equal the number of rows updated in the database.");
            Assert.IsTrue(rollbackTotal == orders.Count, "The number of rows after the transacation has been rollbacked should match the original count");
        }
    }
}
