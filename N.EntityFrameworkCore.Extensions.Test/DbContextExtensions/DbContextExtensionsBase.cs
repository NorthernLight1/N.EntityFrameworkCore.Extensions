using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Data;
using N.EntityFrameworkCore.Extensions.Test.Data.Enums;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions
{
    public enum PopulateDataMode
    {
        Normal,
        Tpc,
        Tph,
        Tpt,
        Schema
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
            dbContext.ProductCategories.Clear();
            dbContext.ProductsWithCustomSchema.Truncate();
            dbContext.Database.ClearTable("TpcCustomer");
            dbContext.Database.ClearTable("TpcVendor");
            dbContext.TphPeople.Truncate();
            dbContext.Database.ClearTable("TptPeople");
            dbContext.Database.ClearTable("TptCustomer");
            dbContext.Database.ClearTable("TptVendor");
            dbContext.Database.DropTable("ProductsUnderTen", true);
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
                            ModifiedDateTime = addedDateTime.AddHours(3),
                            Status = OrderStatus.Completed
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

                    var productCategories = new List<ProductCategory>()
                    {
                        new ProductCategory { Id=1, Name="Category-1", Active=true},
                        new ProductCategory { Id=2, Name="Category-2", Active=true},
                        new ProductCategory { Id=3, Name="Category-3", Active=true},
                        new ProductCategory { Id=4, Name="Category-4", Active=false},
                    };
                    dbContext.BulkInsert(productCategories, o => { o.KeepIdentity = true; o.UsePermanentTable = true; });
                    var products = new List<Product>();
                    id = 1;
                    for (int i = 0; i < 2050; i++)
                    {
                        products.Add(new Product { Id = i.ToString(), Price = 1.25M, OutOfStock = false, ProductCategoryId = 4 });
                        id++;
                    }
                    for (int i = 2050; i < 7000; i++)
                    {
                        products.Add(new Product { Id = i.ToString(), Price = 1.25M, OutOfStock = true });
                        id++;
                    }

                    Debug.WriteLine("Last Id for Product is {0}", id);
                    dbContext.BulkInsert(products, new BulkInsertOptions<Product>() { KeepIdentity = false, AutoMapOutput = false });

                    //ProductWithComplexKey
                    var productsWithComplexKey = new List<ProductWithComplexKey>();
                    id = 1;

                    for (int i = 0; i < 2050; i++)
                    {
                        productsWithComplexKey.Add(new ProductWithComplexKey { Price = 1.25M });
                        id++;
                    }

                    Debug.WriteLine("Last Id for ProductsWithComplexKey is {0}", id);
                    dbContext.BulkInsert(productsWithComplexKey, new BulkInsertOptions<ProductWithComplexKey>() { KeepIdentity = false, AutoMapOutput = false });
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
                else if (mode == PopulateDataMode.Tpc)
                {
                    //TPC Customers & Vendors
                    var tpcCustomers = new List<TpcCustomer>();
                    var tpcVendors = new List<TpcVendor>();
                    for (int i = 1; i <= 2000; i++)
                    {
                        tpcCustomers.Add(new TpcCustomer
                        {
                            Id = i,
                            FirstName = string.Format("John_{0}", i),
                            LastName = string.Format("Smith_{0}", i),
                            Email = string.Format("john.smith{0}@domain.com", i),
                            Phone = "404-555-1111",
                            AddedDate = DateTime.UtcNow
                        });
                    }
                    for (int i = 2001; i <= 3000; i++)
                    {
                        tpcVendors.Add(new TpcVendor
                        {
                            Id = i,
                            FirstName = string.Format("Mike_{0}", i),
                            LastName = string.Format("Smith_{0}", i),
                            Phone = "404-555-2222",
                            Email = string.Format("mike.smith{0}@domain.com", i),
                            Url = string.Format("http://domain.com/mike.smith{0}", i)
                        });
                    }
                    dbContext.BulkInsert(tpcCustomers, new BulkInsertOptions<TpcCustomer>() { KeepIdentity = true });
                    dbContext.BulkInsert(tpcVendors, new BulkInsertOptions<TpcVendor>() { KeepIdentity = true });
                }
                else if (mode == PopulateDataMode.Tpt)
                {
                    //Customers & Vendors
                    var tptCustomers = new List<TptCustomer>();
                    var tptVendors = new List<TptVendor>();
                    for (int i = 1; i <= 2000; i++)
                    {
                        tptCustomers.Add(new TptCustomer
                        {
                            Id = i,
                            FirstName = string.Format("John_{0}", i),
                            LastName = string.Format("Smith_{0}", i),
                            Email = string.Format("john.smith{0}@domain.com", i),
                            Phone = "404-555-1111",
                            AddedDate = DateTime.UtcNow
                        });
                    }
                    for (int i = 2001; i < 3000; i++)
                    {
                        tptVendors.Add(new TptVendor
                        {
                            Id = i,
                            FirstName = string.Format("Mike_{0}", i),
                            LastName = string.Format("Smith_{0}", i),
                            Phone = "404-555-2222",
                            Email = string.Format("mike.smith{0}@domain.com", i),
                            Url = string.Format("http://domain.com/mike.smith{0}", i)
                        });
                    }
                    dbContext.BulkInsert(tptCustomers, new BulkInsertOptions<TptCustomer>() { KeepIdentity = true });
                    dbContext.BulkInsert(tptVendors, new BulkInsertOptions<TptVendor>() { KeepIdentity = true });
                }
                else if (mode == PopulateDataMode.Schema)
                {
                    //ProductWithCustomSchema
                    var productsWithCustomSchema = new List<ProductWithCustomSchema>();
                    int id = 1;

                    for (int i = 0; i < 2050; i++)
                    {
                        productsWithCustomSchema.Add(new ProductWithCustomSchema { Id = id.ToString(), Price = 1.25M });
                        id++;
                    }
                    for (int i = 2050; i < 5000; i++)
                    {
                        productsWithCustomSchema.Add(new ProductWithCustomSchema { Id = id.ToString(), Price = 6.75M });
                        id++;
                    }

                    dbContext.BulkInsert(productsWithCustomSchema);
                }
            }
            return dbContext;
        }
    }
}
