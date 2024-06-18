using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Data;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions;

[TestClass]
public class BulkInsertAsync : DbContextExtensionsBase
{
    [TestMethod]
    public async Task With_Complex_Key()
    {
        var dbContext = SetupDbContext(true);
        var products = new List<ProductWithComplexKey>();
        for (int i = 50000; i < 60000; i++)
        {
            var key = i.ToString();
            products.Add(new ProductWithComplexKey { Price = 1.57M });
        }
        int oldTotal = dbContext.ProductsWithComplexKey.Where(o => o.Price <= 10).Count();
        int rowsInserted = await dbContext.BulkInsertAsync(products);
        int newTotal = dbContext.ProductsWithComplexKey.Where(o => o.Price <= 10).Count();

        Assert.IsTrue(rowsInserted == products.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
    [TestMethod]
    public async Task With_Default_Options()
    {
        var dbContext = SetupDbContext(false);
        var orders = new List<Order>();
        for (int i = 0; i < 20000; i++)
        {
            orders.Add(new Order { Id = i, Price = 1.57M });
        }
        int oldTotal = dbContext.Orders.Where(o => o.Price <= 10).Count();
        int rowsInserted = await dbContext.BulkInsertAsync(orders);
        int newTotal = dbContext.Orders.Where(o => o.Price <= 10).Count();

        Assert.IsTrue(rowsInserted == orders.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
    //[TestMethod]
    //public async Task With_IEnumerable()
    //{
    //    var dbContext = SetupDbContext(false);
    //    var orders = dbContext.Orders.Where(o => o.Price <= 10);

    //    foreach(var order in orders)
    //    {
    //        order.Price = 15.75M;
    //    }
    //    int oldTotal = orders.Count();
    //    int rowsInserted = await dbContext.BulkInsertAsync(orders);
    //    int newTotal = orders.Count();

    //    Assert.IsTrue(rowsInserted == oldTotal, "The number of rows inserted must match the count of order list");
    //    Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
    //}
    [TestMethod]
    public async Task With_Inheritance_Tpc()
    {
        var dbContext = SetupDbContext(false);
        var customers = new List<TpcCustomer>();
        var vendors = new List<TpcVendor>();
        for (int i = 0; i < 20000; i++)
        {
            customers.Add(new TpcCustomer
            {
                Id = i,
                FirstName = string.Format("John_{0}", i),
                LastName = string.Format("Smith_{0}", i),
                Email = string.Format("john.smith{0}@domain.com", i),
                AddedDate = DateTime.UtcNow
            });
        }
        for (int i = 20000; i < 30000; i++)
        {
            vendors.Add(new TpcVendor
            {
                Id = i,
                FirstName = string.Format("Mike_{0}", i),
                LastName = string.Format("Smith_{0}", i),
                Email = string.Format("mike.smith{0}@domain.com", i),
                Url = string.Format("http://domain.com/mike.smith{0}", i)
            });
        }
        int oldTotal = dbContext.TpcPeople.Count();
        int customerRowsInserted = await dbContext.BulkInsertAsync(customers);
        int vendorRowsInserted = await dbContext.BulkInsertAsync(vendors);
        int rowsInserted = customerRowsInserted + vendorRowsInserted;
        int newTotal = dbContext.TpcPeople.Count();

        Assert.IsTrue(rowsInserted == customers.Count + vendors.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
    [TestMethod]
    public async Task With_Inheritance_Tph()
    {
        var dbContext = SetupDbContext(false);
        var customers = new List<TphCustomer>();
        var vendors = new List<TphVendor>();
        for (int i = 0; i < 20000; i++)
        {
            customers.Add(new TphCustomer
            {
                Id = i,
                FirstName = string.Format("John_{0}", i),
                LastName = string.Format("Smith_{0}", i),
                Email = string.Format("john.smith{0}@domain.com", i),
                Phone = "404-555-1111",
                AddedDate = DateTime.UtcNow
            });
        }
        for (int i = 20000; i < 30000; i++)
        {
            vendors.Add(new TphVendor
            {
                Id = i,
                FirstName = string.Format("Mike_{0}", i),
                LastName = string.Format("Smith_{0}", i),
                Phone = "404-555-2222",
                Email = string.Format("mike.smith{0}@domain.com", i),
                Url = string.Format("http://domain.com/mike.smith{0}", i)
            });
        }
        int oldTotal = dbContext.TphPeople.Count();
        int customerRowsInserted = await dbContext.BulkInsertAsync(customers);
        int vendorRowsInserted = await dbContext.BulkInsertAsync(vendors);
        int rowsInserted = customerRowsInserted + vendorRowsInserted;
        int newTotal = dbContext.TphPeople.Count();

        Assert.IsTrue(rowsInserted == customers.Count + vendors.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
    [TestMethod]
    public async Task With_Inheritance_Tpt()
    {
        var dbContext = SetupDbContext(false);
        var customers = new List<TptCustomer>();
        var vendors = new List<TptVendor>();
        for (int i = 0; i < 20000; i++)
        {
            customers.Add(new TptCustomer
            {
                Id = i,
                FirstName = string.Format("John_{0}", i),
                LastName = string.Format("Smith_{0}", i),
                Email = string.Format("john.smith{0}@domain.com", i),
                Phone = "777-555-1234",
                AddedDate = DateTime.UtcNow
            });
        }
        for (int i = 20000; i < 30000; i++)
        {
            vendors.Add(new TptVendor
            {
                Id = i,
                FirstName = string.Format("Mike_{0}", i),
                LastName = string.Format("Smith_{0}", i),
                Email = string.Format("mike.smith{0}@domain.com", i),
                Url = string.Format("http://domain.com/mike.smith{0}", i)
            });
        }
        int oldTotal = dbContext.TptPeople.Count();
        int customerRowsInserted = await dbContext.BulkInsertAsync(customers, o => o.UsePermanentTable = true);
        int vendorRowsInserted = await dbContext.BulkInsertAsync(vendors);
        int rowsInserted = customerRowsInserted + vendorRowsInserted;
        int newTotal = dbContext.TptPeople.Count();

        Assert.IsTrue(rowsInserted == customers.Count + vendors.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
    [TestMethod]
    public async Task Without_Identity_Column()
    {
        var dbContext = SetupDbContext(true);
        var products = new List<Product>();
        for (int i = 50000; i < 60000; i++)
        {
            products.Add(new Product { Id = i.ToString(), Price = 1.57M });
        }
        int oldTotal = dbContext.Products.Where(o => o.Price <= 10).Count();
        int rowsInserted = await dbContext.BulkInsertAsync(products);
        int newTotal = dbContext.Products.Where(o => o.Price <= 10).Count();

        Assert.IsTrue(rowsInserted == products.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
    [TestMethod]
    public async Task With_Options_AutoMapIdentity()
    {

        var dbContext = SetupDbContext(false);
        var orders = new List<Order>();
        for (int i = 0; i < 5000; i++)
        {
            orders.Add(new Order { ExternalId = i.ToString(), Price = ((decimal)i + 0.55M) });
        }
        int rowsAdded = await dbContext.BulkInsertAsync(orders, new BulkInsertOptions<Order>
        {
            UsePermanentTable = true
        });
        bool autoMapIdentityMatched = true;
        var ordersInDb = dbContext.Orders.ToList();
        Order order1 = null;
        Order order2 = null;
        foreach (var order in orders)
        {
            order1 = order;
            var orderinDb = ordersInDb.First(o => o.Id == order.Id);
            order2 = orderinDb;
            if (!(orderinDb.ExternalId == order.ExternalId && orderinDb.Price == order.Price))
            {
                autoMapIdentityMatched = false;
                break;
            }
        }

        Assert.IsTrue(rowsAdded == orders.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(autoMapIdentityMatched, "The auto mapping of ids of entities that were merged failed to match up");
    }
    [TestMethod]
    public async Task With_Options_IgnoreColumns()
    {
        var dbContext = SetupDbContext(false);
        var orders = new List<Order>();
        for (int i = 0; i < 20000; i++)
        {
            orders.Add(new Order { Id = i, ExternalId = i.ToString(), Price = 1.57M, Active = true });
        }
        int oldTotal = dbContext.Orders.Where(o => o.Price <= 10 && o.ExternalId == null).Count();
        int rowsInserted = await dbContext.BulkInsertAsync(orders, options => { options.UsePermanentTable = true; options.IgnoreColumns = o => new { o.ExternalId }; });
        int newTotal = dbContext.Orders.Where(o => o.Price <= 10 && o.ExternalId == null).Count();

        Assert.IsTrue(rowsInserted == orders.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
    [TestMethod]
    public async Task With_Options_InputColumns()
    {
        var dbContext = SetupDbContext(false);
        var orders = new List<Order>();
        for (int i = 0; i < 20000; i++)
        {
            orders.Add(new Order { Id = i, ExternalId = i.ToString(), Price = 1.57M, Active = true, Status = OrderStatus.Completed });
        }
        int oldTotal = dbContext.Orders.Where(o => o.Price == 1.57M && o.ExternalId == null && o.Active == true).Count();
        int rowsInserted = await dbContext.BulkInsertAsync(orders, options =>
        {
            options.UsePermanentTable = true;
            options.InputColumns = o => new { o.Price, o.Active, o.AddedDateTime, o.Status };
        });
        int newTotal = dbContext.Orders.Where(o => o.Price == 1.57M && o.ExternalId == null && o.Active == true).Count();

        Assert.IsTrue(rowsInserted == orders.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
    [TestMethod]
    public async Task With_KeepIdentity()
    {
        var dbContext = SetupDbContext(false);
        var orders = new List<Order>();
        for (int i = 0; i < 20000; i++)
        {
            orders.Add(new Order { Id = i + 1000, Price = 1.57M });
        }
        int oldTotal = dbContext.Orders.Count();
        int rowsInserted = await dbContext.BulkInsertAsync(orders, options => { options.KeepIdentity = true; options.BatchSize = 1000; });
        var oldOrders = dbContext.Orders.OrderBy(o => o.Id).ToList();
        var newOrders = dbContext.Orders.OrderBy(o => o.Id).ToList();
        bool allIdentityFieldsMatch = true;
        for (int i = 0; i < 20000; i++)
        {
            if (newOrders[i].Id != oldOrders[i].Id)
            {
                allIdentityFieldsMatch = false;
                break;
            }
        }
        try
        {
            int rowsInserted2 = await dbContext.BulkInsertAsync(orders, new BulkInsertOptions<Order>()
            {
                KeepIdentity = true,
                BatchSize = 1000,
            });
        }
        catch (Exception ex)
        {
            Assert.IsInstanceOfType(ex, typeof(SqlException));
            Assert.IsTrue(ex.Message.StartsWith("Violation of PRIMARY KEY constraint 'PK_Orders'."));
        }

        Assert.IsTrue(oldTotal == 0, "There should not be any records in the table");
        Assert.IsTrue(rowsInserted == orders.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(allIdentityFieldsMatch, "The identities between the source and the database should match.");
    }
    [TestMethod]
    public async Task With_Proxy_Type()
    {
        var dbContext = SetupDbContext(false);
        int oldTotalCount = dbContext.Products.Where(o => o.Price == 10.57M).Count();

        var products = new List<Product>();
        for (int i = 0; i < 2000; i++)
        {
            var product = dbContext.Products.CreateProxy();
            product.Id = (-i).ToString();
            product.Price = 10.57M;
            products.Add(product);
        }
        int oldTotal = dbContext.Products.Where(o => o.Price == 10.57M).Count();
        int rowsInserted = await dbContext.BulkInsertAsync(products);
        int newTotal = dbContext.Products.Where(o => o.Price == 10.57M).Count();

        Assert.IsTrue(rowsInserted == products.Count, "The number of rows inserted must match the count of products list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
    [TestMethod]
    public async Task With_Schema()
    {
        var dbContext = SetupDbContext(false);
        var products = new List<ProductWithCustomSchema>();
        for (int i = 1; i < 10000; i++)
        {
            var key = i.ToString();
            products.Add(new ProductWithCustomSchema
            {
                Id = key,
                Name = $"Product-{key}",
                Price = 1.57M
            });
        }
        int oldTotal = dbContext.ProductsWithCustomSchema.Where(o => o.Price <= 10).Count();
        int rowsInserted = await dbContext.BulkInsertAsync(products);
        int newTotal = dbContext.ProductsWithCustomSchema.Where(o => o.Price <= 10).Count();

        Assert.IsTrue(rowsInserted == products.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
    [TestMethod]
    public async Task With_Transaction()
    {
        var dbContext = SetupDbContext(false);
        var orders = new List<Order>();
        for (int i = 0; i < 20000; i++)
        {
            orders.Add(new Order { Id = i, Price = 1.57M });
        }
        int oldTotal = dbContext.Orders.Where(o => o.Price <= 10).Count();
        int rowsInserted, newTotal;
        using (var transaction = dbContext.Database.BeginTransaction())
        {
            rowsInserted = await dbContext.BulkInsertAsync(orders);
            newTotal = dbContext.Orders.Where(o => o.Price <= 10).Count();
            transaction.Rollback();
        }
        int rollbackTotal = dbContext.Orders.Where(o => o.Price <= 10).Count();

        Assert.IsTrue(rowsInserted == orders.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
        Assert.IsTrue(rollbackTotal == oldTotal, "The number of rows after the transacation has been rollbacked should match the original count");
    }
    [TestMethod]
    public async Task With_Options_InsertIfNotExists()
    {
        var dbContext = SetupDbContext(true);
        var orders = new List<Order>();
        long maxId = dbContext.Orders.Max(o => o.Id);
        long expectedRowsInserted = 1000;
        int existingRowsToAdd = 100;
        long startId = maxId - existingRowsToAdd + 1, endId = maxId + expectedRowsInserted + 1;
        for (long i = startId; i < endId; i++)
        {
            orders.Add(new Order { Id = i, Price = 1.57M });
        }

        int oldTotal = dbContext.Orders.Where(o => o.Price <= 10).Count();
        int rowsInserted = await dbContext.BulkInsertAsync(orders, new BulkInsertOptions<Order>() { InsertIfNotExists = true });
        int newTotal = dbContext.Orders.Where(o => o.Price <= 10).Count();

        Assert.IsTrue(rowsInserted == expectedRowsInserted, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == expectedRowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
    [TestMethod]
    public async Task With_ValueGenerated_Default()
    {
        var dbContext = SetupDbContext(false);
        var nowDateTime = DateTime.Now;
        var orders = new List<Order>();
        for (int i = 0; i < 20000; i++)
        {
            orders.Add(new Order { Id = i, Price = 1.57M });
        }
        int oldTotal = dbContext.Orders.Where(o => o.Price <= 10).Count();
        int rowsInserted = await dbContext.BulkInsertAsync(orders);
        int newTotal = dbContext.Orders.Where(o => o.Price <= 10 && o.DbAddedDateTime > nowDateTime).Count();

        Assert.IsTrue(rowsInserted == orders.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
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
        int rowsInserted = await dbContext.BulkInsertAsync(orders);
        int newTotal = dbContext.Orders.Where(o => o.Price <= 10 && o.DbModifiedDateTime > nowDateTime).Count();

        Assert.IsTrue(rowsInserted == orders.Count(), "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
}