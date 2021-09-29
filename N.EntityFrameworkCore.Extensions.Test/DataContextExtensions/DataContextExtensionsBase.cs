using Microsoft.EntityFrameworkCore;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Data;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace N.EntityFrameworkCore.Extensions.Test.DataContextExtensions
{
    [TestClass]
    public class DataContextExtensionsBase
    {
        [TestInitialize]
        public void Init()
        {
            TestDbContext testDbContext = new TestDbContext();
            testDbContext.Database.EnsureCreated();
        }
        protected TestDbContext SetupDbContext(bool populateData)
        {
            TestDbContext dbContext = new TestDbContext();
            dbContext.Orders.DeleteFromQuery();
            dbContext.Products.DeleteFromQuery();
            dbContext.Database.ClearTable("TphPeople");
            ClearTableIfExists(dbContext, "OrdersUnderTen");
            ClearTableIfExists(dbContext, "OrdersLast30Days");
            if (populateData)
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
                //var order = dbContext.Orders.Where(o => o.Id == 1).FirstOrDefault();
                //order.Price = 5.22M;
                //dbContext.SaveChanges();
                var articles = new List<Product>();
                id = 1;
                for (int i = 0; i < 2050; i++)
                {
                    articles.Add(new Product { Id = i, Price = 1.25M, OutOfStock = false });
                    id++;
                }
                for (int i = 0; i < 2050; i++)
                {
                    articles.Add(new Product { Id = i, Price = 1.25M, OutOfStock = true });
                    id++;
                }

                Debug.WriteLine("Last Id for Article is {0}", id);
                dbContext.BulkInsert(articles, new BulkInsertOptions<Product>() { KeepIdentity = false, AutoMapOutputIdentity = false });

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
                        Phone = "404-555-1111"
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
            return dbContext;
        }

        private void ClearTableIfExists(DbContext dbContext, string tableName)
        {
            if (dbContext.Database.TableExists(tableName))
            {
                dbContext.Database.ClearTable(tableName);
            }
        }
    }
}
