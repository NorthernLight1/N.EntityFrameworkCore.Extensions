using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Common;
using N.EntityFrameworkCore.Extensions.Test.Data;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions;

[TestClass]
public class BulkInsert : DbContextExtensionsBase
{
    [TestMethod]
    public void With_Complex_Key()
    {
        var dbContext = SetupDbContext(true);
        var products = new List<ProductWithComplexKey>();
        for (int i = 50000; i < 60000; i++)
        {
            var key = i.ToString();
            products.Add(new ProductWithComplexKey { Price = 1.57M });
        }
        int oldTotal = dbContext.ProductsWithComplexKey.Where(o => o.Price <= 10).Count();
        int rowsInserted = dbContext.BulkInsert(products);
        int newTotal = dbContext.ProductsWithComplexKey.Where(o => o.Price <= 10).Count();

        Assert.IsTrue(rowsInserted == products.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
    [TestMethod]
    public void With_Complex_Type()
    {
        var dbContext = SetupDbContext(true);
        var orders = new List<OrderWithComplexType>();
        for (int i = 1; i < 1000; i++)
        {
            orders.Add(new OrderWithComplexType
            {
                Id = i,
                ShippingAddress = new Address
                {
                    Line1 = $"123 Main St, {i}",
                    City = "Atlanta",
                    Country = "USA",
                    PostCode = "30303"
                },
                BillingAddress = new Address
                {
                    Line1 = $"456 Oak St, {i}",
                    City = "Atlanta",
                    Country = "USA",
                    PostCode = "30303"
                }
            });
        }
        int oldTotal = dbContext.OrdersWithComplexType.Count();
        int rowsInserted = dbContext.BulkInsert(orders);
        int newTotal = dbContext.OrdersWithComplexType.Count();

        Assert.IsTrue(rowsInserted == orders.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");

        dbContext.ChangeTracker.Clear();
        var saved = dbContext.OrdersWithComplexType.OrderBy(o => o.Id).ToList();
        Assert.AreEqual(orders.Count, saved.Count - oldTotal);
        var first = saved.First(o => o.ShippingAddress.Line1 == "123 Main St, 1");
        Assert.AreEqual("Atlanta", first.ShippingAddress.City);
        Assert.AreEqual("456 Oak St, 1", first.BillingAddress.Line1);
        var last = saved.First(o => o.ShippingAddress.Line1 == "123 Main St, 999");
        Assert.AreEqual("Atlanta", last.ShippingAddress.City);
        Assert.AreEqual("456 Oak St, 999", last.BillingAddress.Line1);
    }
    [TestMethod]
    public void With_Default_Options()
    {
        var dbContext = SetupDbContext(false);
        var orders = new List<Order>();
        for (int i = 0; i < 20000; i++)
        {
            orders.Add(new Order { Id = i, Price = 1.57M });
        }
        int oldTotal = dbContext.Orders.Where(o => o.Price <= 10).Count();
        int rowsInserted = dbContext.BulkInsert(orders);
        int newTotal = dbContext.Orders.Where(o => o.Price <= 10).Count();

        Assert.IsTrue(rowsInserted == orders.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
    [TestMethod]
    public void With_Inheritance_Tpc()
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
        int customerRowsInserted = dbContext.BulkInsert(customers, o => o.UsePermanentTable = true);
        int vendorRowsInserted = dbContext.BulkInsert(vendors, o => o.UsePermanentTable = true);
        int rowsInserted = customerRowsInserted + vendorRowsInserted;
        int newTotal = dbContext.TpcPeople.Count();

        Assert.IsTrue(rowsInserted == customers.Count + vendors.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
    [TestMethod]
    public void With_Inheritance_Tph()
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
        int customerRowsInserted = dbContext.BulkInsert(customers);
        int vendorRowsInserted = dbContext.BulkInsert(vendors);
        int rowsInserted = customerRowsInserted + vendorRowsInserted;
        int newTotal = dbContext.TphPeople.Count();

        Assert.IsTrue(rowsInserted == customers.Count + vendors.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
    [TestMethod]
    public void With_Inheritance_Tpt()
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
        int customerRowsInserted = dbContext.BulkInsert(customers, o => o.UsePermanentTable = true);
        int vendorRowsInserted = dbContext.BulkInsert(vendors);
        int rowsInserted = customerRowsInserted + vendorRowsInserted;
        int newTotal = dbContext.TptPeople.Count();

        Assert.IsTrue(rowsInserted == customers.Count + vendors.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
    [TestMethod]
    public void Without_Identity_Column()
    {
        var dbContext = SetupDbContext(true);
        var products = new List<Product>();
        for (int i = 50000; i < 60000; i++)
        {
            products.Add(new Product { Id = i.ToString(), Price = 1.57M });
        }
        int oldTotal = dbContext.Products.Where(o => o.Price <= 10).Count();
        int rowsInserted = dbContext.BulkInsert(products);
        int newTotal = dbContext.Products.Where(o => o.Price <= 10).Count();

        Assert.IsTrue(rowsInserted == products.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
    [TestMethod]
    public void With_Options_AutoMapIdentity()
    {
        var dbContext = SetupDbContext(false);
        var orders = new List<Order>();
        for (int i = 0; i < 5000; i++)
        {
            orders.Add(new Order { ExternalId = i.ToString(), Price = ((decimal)i + 0.55M) });
        }
        int rowsAdded = dbContext.BulkInsert(orders, new BulkInsertOptions<Order>
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
    public void With_Options_IgnoreColumns()
    {
        var dbContext = SetupDbContext(false);
        var orders = new List<Order>();
        for (int i = 0; i < 20000; i++)
        {
            orders.Add(new Order { Id = i, ExternalId = i.ToString(), Price = 1.57M, Active = true });
        }
        int oldTotal = dbContext.Orders.Where(o => o.Price <= 10 && o.ExternalId == null).Count();
        int rowsInserted = dbContext.BulkInsert(orders, options => { options.UsePermanentTable = true; options.IgnoreColumns = o => new { o.ExternalId }; });
        int newTotal = dbContext.Orders.Where(o => o.Price <= 10 && o.ExternalId == null).Count();

        Assert.IsTrue(rowsInserted == orders.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
    [TestMethod]
    public void With_Options_InputColumns()
    {
        var dbContext = SetupDbContext(false);
        var orders = new List<Order>();
        for (int i = 0; i < 20000; i++)
        {
            orders.Add(new Order { Id = i, ExternalId = i.ToString(), Price = 1.57M, Active = true, Status = OrderStatus.Completed });
        }
        int oldTotal = dbContext.Orders.Where(o => o.Price == 1.57M && o.ExternalId == null && o.Active == true).Count();
        int rowsInserted = dbContext.BulkInsert(orders, options =>
        {
            options.UsePermanentTable = true;
            options.InputColumns = o => new { o.Price, o.Active, o.AddedDateTime, o.Status };
        });
        int newTotal = dbContext.Orders.Where(o => o.Price == 1.57M && o.ExternalId == null && o.Active == true).Count();

        Assert.IsTrue(rowsInserted == orders.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
    [TestMethod]
    public void With_KeepIdentity()
    {
        var dbContext = SetupDbContext(false);
        var orders = new List<Order>();
        for (int i = 0; i < 20000; i++)
        {
            orders.Add(new Order { Id = i + 1000, Price = 1.57M });
        }
        int oldTotal = dbContext.Orders.Count();
        int rowsInserted = dbContext.BulkInsert(orders, options => { options.KeepIdentity = true; options.BatchSize = 1000; });
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
            int rowsInserted2 = dbContext.BulkInsert(orders, new BulkInsertOptions<Order>()
            {
                KeepIdentity = true,
                BatchSize = 1000,
            });
        }
        catch (Exception ex)
        {
            Assert.IsInstanceOfType(ex, typeof(SqlException));
            Assert.IsTrue(Config.IsPrimaryKeyViolation(ex));
        }

        Assert.IsTrue(oldTotal == 0, "There should not be any records in the table");
        Assert.IsTrue(rowsInserted == orders.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(allIdentityFieldsMatch, "The identities between the source and the database should match.");
    }
    [TestMethod]
    public void With_Schema()
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
        int rowsInserted = dbContext.BulkInsert(products);
        int newTotal = dbContext.ProductsWithCustomSchema.Where(o => o.Price <= 10).Count();

        Assert.IsTrue(rowsInserted == products.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
    [TestMethod]
    public void With_Transaction()
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
            rowsInserted = dbContext.BulkInsert(orders);
            newTotal = dbContext.Orders.Where(o => o.Price <= 10).Count();
            transaction.Rollback();
        }
        int rollbackTotal = dbContext.Orders.Where(o => o.Price <= 10).Count();

        Assert.IsTrue(rowsInserted == orders.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
        Assert.IsTrue(rollbackTotal == oldTotal, "The number of rows after the transacation has been rollbacked should match the original count");
    }
    [TestMethod]
    public void With_Options_InsertIfNotExists()
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
        int rowsInserted = dbContext.BulkInsert(orders, new BulkInsertOptions<Order>() { InsertIfNotExists = true });
        int newTotal = dbContext.Orders.Where(o => o.Price <= 10).Count();

        Assert.IsTrue(rowsInserted == expectedRowsInserted, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == expectedRowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
    [TestMethod]
    public void With_Options_InsertOnCondition()
    {
        var dbContext = SetupDbContext(true);
        var existing = dbContext.Orders.Where(o => o.ExternalId != null).OrderBy(o => o.Id).Take(50).ToList();
        Assert.IsTrue(existing.Count > 0);

        long maxId = dbContext.Orders.Max(o => o.Id);
        var toInsert = new List<Order>();
        // Reuse existing ExternalIds (should be skipped) + new ExternalIds (should insert)
        foreach (var order in existing)
            toInsert.Add(new Order { Id = maxId + order.Id + 1, ExternalId = order.ExternalId, Price = 9.99M });
        for (int i = 0; i < 100; i++)
            toInsert.Add(new Order { Id = maxId + 100000 + i, ExternalId = $"insert-on-{i}", Price = 9.99M });

        int oldTotal = dbContext.Orders.Count(o => o.Price == 9.99M);
        int rowsInserted = dbContext.BulkInsert(toInsert, o =>
        {
            o.InsertIfNotExists = true;
            o.InsertOnCondition = (s, t) => s.ExternalId == t.ExternalId;
            o.KeepIdentity = true;
        });
        int newTotal = dbContext.Orders.Count(o => o.Price == 9.99M);

        Assert.AreEqual(100, rowsInserted, "Only rows that do not match InsertOnCondition should insert");
        Assert.AreEqual(100, newTotal - oldTotal);
    }
    [TestMethod]
    public void With_Proxy_Type()
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
        int rowsInserted = dbContext.BulkInsert(products);
        int newTotal = dbContext.Products.Where(o => o.Price == 10.57M).Count();

        Assert.IsTrue(rowsInserted == products.Count, "The number of rows inserted must match the count of products list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
    [TestMethod]
    public void With_Trigger()
    {
        var dbContext = SetupDbContext(false);
        var products = new List<ProductWithTrigger>();
        for (int i = 1; i < 1000; i++)
        {
            products.Add(new ProductWithTrigger { Id = i.ToString(), Price = 1.57M, StatusString = "InStock" });
        }

        //The return int from BulkInsert() will be off when using triggers
        dbContext.BulkInsert(products, options =>
        {
            options.AutoMapOutput = false;
            if (Config.IsSqlServer)
                options.FireTriggers = true;
        });
        var rowsInserted = dbContext.ProductsWithTrigger.Count();

        Assert.IsTrue(rowsInserted == products.Count, $"The number of rows inserted must match the count of products ({rowsInserted}!={products.Count})");
    }
    [TestMethod]
    public void With_Trigger_And_AutoMapOutput()
    {
        var dbContext = SetupDbContext(false);
        var products = new List<ProductWithTrigger>();
        for (int i = 1; i <= 50; i++)
            products.Add(new ProductWithTrigger { Id = $"trig-{i}", Price = 2.25M, StatusString = "InStock" });

        int rowsInserted = dbContext.BulkInsert(products, options =>
        {
            options.AutoMapOutput = true;
            options.FireTriggers = true;
        });

        Assert.AreEqual(50, dbContext.ProductsWithTrigger.Count());
        Assert.IsTrue(rowsInserted >= 50 || dbContext.ProductsWithTrigger.Count() == 50,
            "FireTriggers with AutoMapOutput must succeed without MERGE OUTPUT errors (#51/#88)");
    }
    [TestMethod]
    public void With_Client_Guid_PrimaryKey()
    {
        var dbContext = SetupDbContext(false);
        var id1 = Guid.NewGuid();
        var id2 = Guid.NewGuid();
        var entities = new List<GuidKeyEntity>
        {
            new GuidKeyEntity { Id = id1, Name = "One" },
            new GuidKeyEntity { Id = id2, Name = "Two" }
        };

        // No KeepIdentity — client Guid PKs without SQL defaults must still be inserted (#82/#65)
        int rowsInserted = dbContext.BulkInsert(entities);
        dbContext.ChangeTracker.Clear();
        var saved = dbContext.GuidKeyEntities.OrderBy(e => e.Name).ToList();

        Assert.AreEqual(2, rowsInserted);
        Assert.AreEqual(2, saved.Count);
        Assert.AreEqual(id1, saved[0].Id);
        Assert.AreEqual(id2, saved[1].Id);
    }
    [TestMethod]
    public void With_ValueGenerated_Default()
    {
        var dbContext = SetupDbContext(false);
        var nowDateTime = DateTime.Now;
        var orders = new List<Order>();
        for (int i = 0; i < 20000; i++)
        {
            orders.Add(new Order { Id = i, Price = 1.57M, DbActive = true, DbAddedDateTime = nowDateTime.AddSeconds(1) });
        }
        int oldTotal = dbContext.Orders.Where(o => o.Price <= 10).Count();
        int rowsInserted = dbContext.BulkInsert(orders);
        int newTotal = dbContext.Orders.Where(o => o.Price <= 10 && o.DbAddedDateTime > nowDateTime && o.DbActive).Count();

        Assert.IsTrue(rowsInserted == orders.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
    [TestMethod]
    public void With_DefaultValue_Columns_Explicit_Values()
    {
        var dbContext = SetupDbContext(false);
        var explicitAdded = new DateTime(2022, 1, 1, 8, 30, 0);
        var orders = new List<Order>
        {
            new Order { Id = 1, Price = 3.25M, DbActive = false, DbAddedDateTime = explicitAdded },
            new Order { Id = 2, Price = 4.50M, DbActive = true, DbAddedDateTime = explicitAdded }
        };

        int rowsInserted = dbContext.BulkInsert(orders);
        dbContext.ChangeTracker.Clear();
        var saved = dbContext.Orders.OrderBy(o => o.Id).ToList();

        Assert.AreEqual(2, rowsInserted);
        Assert.AreEqual(2, saved.Count);
        Assert.IsFalse(saved[0].DbActive, "Explicit false must be inserted even when the column has a SQL default of 1 (#86)");
        Assert.IsTrue(saved[1].DbActive);
        Assert.AreEqual(explicitAdded, saved[0].DbAddedDateTime, "Explicit DateTime must be inserted even with getdate() default (#83)");
        Assert.AreEqual(explicitAdded, saved[1].DbAddedDateTime);
    }
    [TestMethod]
    public void With_ValueGenerated_Computed()
    {
        var dbContext = SetupDbContext(false);
        var nowDateTime = DateTime.Now;
        var orders = new List<Order>();
        for (int i = 0; i < 20000; i++)
        {
            orders.Add(new Order { Id = i, Price = 1.57M, DbModifiedDateTime = nowDateTime });
        }
        int oldTotal = dbContext.Orders.Where(o => o.Price <= 10).Count();
        int rowsInserted = dbContext.BulkInsert(orders);
        int newTotal = dbContext.Orders.Where(o => o.Price <= 10 && o.DbModifiedDateTime > nowDateTime).Count();

        Assert.IsTrue(rowsInserted == orders.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
    [TestMethod]
    public void With_UsePermanentTable_False_And_No_Create_Table_Permission()
    {
        // Ensures BulkInsert uses a temporary (#) table when UsePermanentTable = false,
        // so users without CREATE TABLE permission can still perform bulk inserts.
        var adminDbContext = SetupDbContext(false);
        TestDatabaseInitializer.CreateRestrictedUser(adminDbContext);

        var restrictedConnectionString = Config.GetRestrictedTestDatabaseConnectionString();
        var dbContext = new TestDbContext(restrictedConnectionString);

        var orders = new List<Order>();
        for (int i = 0; i < 1000; i++)
        {
            orders.Add(new Order { Id = i, Price = 1.57M });
        }
        int oldTotal = adminDbContext.Orders.Count();
        int rowsInserted = dbContext.BulkInsert(orders, options => options.UsePermanentTable = false);
        int newTotal = adminDbContext.Orders.Count();

        Assert.IsTrue(rowsInserted == orders.Count, "The number of rows inserted must match the count of order list");
        Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
    }
    [TestMethod]
    public void With_IncludeGraph()
    {
        var dbContext = SetupDbContext(false);
        var blogs = new List<Blog>
        {
            new Blog
            {
                Title = "Graph Blog 1",
                Posts =
                [
                    new BlogPost { Title = "Post A" },
                    new BlogPost { Title = "Post B" }
                ]
            },
            new Blog
            {
                Title = "Graph Blog 2",
                Posts =
                [
                    new BlogPost { Title = "Post C" }
                ]
            }
        };

        int rowsInserted = dbContext.BulkInsert(blogs, options =>
        {
            options.IncludeGraph = true;
            options.AutoMapOutput = true;
        });

        var savedBlogs = dbContext.Blogs.Include(b => b.Posts).OrderBy(b => b.Id).ToList();
        Assert.AreEqual(2, savedBlogs.Count, "Both blogs should be inserted");
        Assert.AreEqual(3, dbContext.BlogPosts.Count(), "All related posts should be inserted");
        Assert.IsTrue(savedBlogs.All(b => b.Id > 0), "Blog identity values should be mapped back");
        Assert.IsTrue(savedBlogs.SelectMany(b => b.Posts).All(p => p.BlogId > 0 && p.Id > 0),
            "Posts should receive foreign keys from inserted blogs and have their own identity values");
        Assert.AreEqual(2, savedBlogs[0].Posts.Count);
        Assert.AreEqual(1, savedBlogs[1].Posts.Count);
        Assert.IsTrue(rowsInserted >= 5, "Rows affected should include blogs and posts");
    }

    [TestMethod]
    public void With_OwnedType()
    {
        var dbContext = SetupDbContext(false);
        var orders = new List<OrderWithOwnedDetail>
        {
            new() { Price = 9.99M, Detail = new OwnedDetail { City = "Seattle", Region = "WA" } }
        };
        int rows = dbContext.BulkInsert(orders, o => o.AutoMapOutput = true);
        Assert.AreEqual(1, rows);
        var saved = dbContext.OrdersWithOwnedDetail.First();
        Assert.AreEqual("Seattle", saved.Detail.City);
    }

    [TestMethod]
    public void With_TemporalTable()
    {
        var dbContext = SetupDbContext(false);
        var orders = new List<TemporalOrder> { new() { Id = 1, Price = 9.99M }, new() { Id = 2, Price = 19.99M } };
        int rows = dbContext.BulkInsert(orders);
        Assert.AreEqual(2, rows);
        Assert.AreEqual(2, dbContext.TemporalOrders.Count());
    }

    [TestMethod]
    public void With_HierarchyId()
    {
        var dbContext = SetupDbContext(false);
        var nodes = new List<OrgNode>
        {
            new() { Id = 1, Name = "Root", NodePath = HierarchyId.GetRoot() },
            new() { Id = 2, Name = "Child", NodePath = HierarchyId.Parse("/1/") }
        };
        int rows = dbContext.BulkInsert(nodes);
        Assert.AreEqual(2, rows);
        var saved = dbContext.OrgNodes.OrderBy(n => n.Id).ToList();
        Assert.AreEqual(2, saved.Count);
        Assert.IsNotNull(saved[0].NodePath);
        Assert.IsNotNull(saved[1].NodePath);
        Assert.AreEqual("/1/", saved[1].NodePath.ToString());
    }

    [TestMethod]
    public void With_Spatial()
    {
        var dbContext = SetupDbContext(false);
        var locations = new List<LocationWithSpatial>
        {
            new() { Id = 1, Name = "HQ", Location = new NetTopologySuite.Geometries.Point(-122.33, 47.62) { SRID = 4326 } }
        };
        int rows = dbContext.BulkInsert(locations);
        Assert.AreEqual(1, rows);
        var saved = dbContext.LocationsWithSpatial.First();
        Assert.IsNotNull(saved.Location);
        Assert.AreEqual(4326, saved.Location.SRID);
    }

    [TestMethod]
    public void With_EnableStreaming()
    {
        var dbContext = SetupDbContext(false);
        var orders = new List<Order> { new() { Id = 900001, Price = 7.77M } };
        int rows = dbContext.BulkInsert(orders, options => { options.EnableStreaming = true; options.KeepIdentity = true; });
        Assert.AreEqual(1, rows);
        Assert.IsTrue(dbContext.Orders.Any(o => o.Id == 900001));
    }

    [TestMethod]
    public void With_ColumnOrderHints()
    {
        var dbContext = SetupDbContext(false);
        var orders = new List<Order> { new() { Id = 900002, Price = 8.88M } };
        int rows = dbContext.BulkInsert(orders, options =>
        {
            options.KeepIdentity = true;
            options.ColumnOrderHints.Add(new SqlBulkCopyColumnOrderHint("Id", SortOrder.Ascending));
        });
        Assert.AreEqual(1, rows);
        Assert.IsTrue(dbContext.Orders.Any(o => o.Id == 900002));
    }

    [TestMethod]
    public void With_NotifyAfter_And_RowsCopied()
    {
        var dbContext = SetupDbContext(false);
        var orders = new List<Order>();
        for (int i = 0; i < 100; i++)
            orders.Add(new Order { Id = 900100 + i, Price = 1.11M });

        int callbackCount = 0;
        long lastRowsCopied = 0;
        dbContext.BulkInsert(orders, options =>
        {
            options.KeepIdentity = true;
            options.NotifyAfter = 50;
            options.RowsCopied += e =>
            {
                callbackCount++;
                lastRowsCopied = e.RowsCopied;
            };
        });

        Assert.AreEqual(100, dbContext.Orders.Count(o => o.Price == 1.11M));
        Assert.IsTrue(callbackCount >= 1, "RowsCopied should fire when NotifyAfter is set");
        Assert.IsTrue(lastRowsCopied >= 50);
    }

    [TestMethod]
    public void With_BulkCopyOptions_TableLock()
    {
        var dbContext = SetupDbContext(false);
        var orders = new List<Order> { new() { Id = 900003, Price = 6.66M } };
        int rows = dbContext.BulkInsert(orders, options => { options.BulkCopyOptions = SqlBulkCopyOptions.TableLock; options.KeepIdentity = true; });
        Assert.AreEqual(1, rows);
        Assert.IsTrue(dbContext.Orders.Any(o => o.Id == 900003));
    }
}