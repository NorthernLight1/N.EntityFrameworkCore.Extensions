using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Data;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions
{
    public enum PopulateDataMode
    {
        Normal,
        Tph
    }
    [TestClass]
    public class DbContextExtensionsBase
    {
        [TestInitialize]
        public void Init()
        {
            TestDbContext dbContext = new TestDbContext();
            dbContext.Database.EnsureCreated();
        }
        protected TestDbContext SetupDbContext(bool populateData, PopulateDataMode mode = PopulateDataMode.Normal)
        {
            TestDbContext dbContext = new TestDbContext();
            dbContext.Orders.Truncate();
            dbContext.Products.Truncate();
            dbContext.Database.ClearTable("TphPeople");
            dbContext.Database.DropTable("OrdersUnderTen", true);
            dbContext.Database.DropTable("OrdersLast30Days", true);
            if (populateData)
            {
                if (mode == PopulateDataMode.Normal)
                {
                    var orders = new List<Order>();
                    int id = 1;
                    for (int i = 0; i < 2050; i++)
                    {
                        DateTime addedDateTime = DateTime.UtcNow.AddDays(-id);
                        orders.Add(new Order
                        {
                            Id = id,
                            ExternalId = string.Format("id-{0}", i),
                            Price = 1.25M,
                            AddedDateTime = addedDateTime,
                            ModifiedDateTime = addedDateTime.AddHours(3)
                        });
                        id++;
                    }
                    for (int i = 0; i < 1050; i++)
                    {
                        orders.Add(new Order { Id = id, Price = 5.35M });
                        id++;
                    }
                    for (int i = 0; i < 2050; i++)
                    {
                        orders.Add(new Order { Id = id, Price = 1.25M });
                        id++;
                    }
                    for (int i = 0; i < 6000; i++)
                    {
                        orders.Add(new Order { Id = id, Price = 15.35M });
                        id++;
                    }
                    for (int i = 0; i < 6000; i++)
                    {
                        orders.Add(new Order { Id = id, Price = 15.35M });
                        id++;
                    }

                    Debug.WriteLine("Last Id for Order is {0}", id);
                    dbContext.BulkInsert(orders, new BulkInsertOptions<Order>() { KeepIdentity = true });
                    var products = new List<Product>();
                    id = 1;
                    for (int i = 0; i < 2050; i++)
                    {
                        products.Add(new Product { Id = i.ToString(), Price = 1.25M, OutOfStock = false });
                        id++;
                    }
                    for (int i = 2050; i < 7000; i++)
                    {
                        products.Add(new Product { Id = i.ToString(), Price = 1.25M, OutOfStock = true });
                        id++;
                    }

                    Debug.WriteLine("Last Id for Product is {0}", id);
                    dbContext.BulkInsert(products, new BulkInsertOptions<Product>() { KeepIdentity = false, AutoMapOutputIdentity = false });
                }
                else if (mode == PopulateDataMode.Tph)
                {
                    //TPH Customers & Vendors
                    var tphCustomers = new List<TphCustomer>();
                    var tphVendors = new List<TphVendor>();
                    for (int i = 0; i < 2000; i++)
                    {
                        tphCustomers.Add(new TphCustomer
                        {
                            Id = i,
                            FirstName = string.Format("John_{0}", i),
                            LastName = string.Format("Smith_{0}", i),
                            Email = string.Format("john.smith{0}@domain.com", i),
                            Phone = "404-555-1111",
                            AddedDate = DateTime.UtcNow
                        });
                    }
                    for (int i = 2000; i < 3000; i++)
                    {
                        tphVendors.Add(new TphVendor
                        {
                            Id = i,
                            FirstName = string.Format("Mike_{0}", i),
                            LastName = string.Format("Smith_{0}", i),
                            Phone = "404-555-2222",
                            Email = string.Format("mike.smith{0}@domain.com", i),
                            Url = string.Format("http://domain.com/mike.smith{0}", i)
                        });
                    }
                    dbContext.BulkInsert(tphCustomers, new BulkInsertOptions<TphCustomer>() { KeepIdentity = true });
                    dbContext.BulkInsert(tphVendors, new BulkInsertOptions<TphVendor>() { KeepIdentity = true });
                }
            }
            return dbContext;
        }
    }
}
