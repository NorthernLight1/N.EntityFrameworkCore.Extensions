using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions;
using N.EntityFrameworkCore.Extensions.Test.Data;

namespace N.EntityFrameworkCore.Extensions.Test.DataContextExtensions
{
    [TestClass]
    public class DataContextExtensionsTest : DataContextExtensionsBase
    {
        //[TestMethod]
        //public void TestBulkInsert_EF_CustomTable()
        //{
        //    TestDbContext dbContext = new TestDbContext();
        //    var orders = new List<Order>();
        //    for (int i = 0; i < 20000; i++)
        //    {
        //        orders.Add(new Order { Id=i, Price = 1.57M });
        //    }
        //    int oldTotal = dbContext.Orders.Where(o => o.Price <= 10).Count();
        //    int rowsInserted = dbContext.BulkInsert(orders, new BulkInsertOptions<Order> { 
        //        TableName = "[dbo].[Orders3]",
        //        KeepIdentity = true,
        //        InputColumns = (o) => new { o.Id }
        //    });
        //    int newTotal = dbContext.Orders.Where(o => o.Price <= 10).Count();

        //    Assert.IsTrue(rowsInserted == orders.Count, "The number of rows inserted must match the count of order list");
        //    Assert.IsTrue(newTotal - oldTotal == rowsInserted, "The new count minus the old count should match the number of rows inserted.");
        //}
        [TestMethod]
        public void BulkDelete()
        {
            var dbContext = SetupDbContext(true);
            var orders = dbContext.Orders.Where(o => o.Price <= 2).ToList();
            int rowsDeleted = dbContext.BulkDelete(orders);
            int newTotal = dbContext.Orders.Where(o => o.Price <= 2).Count();

            Assert.IsTrue(orders.Count > 0, "There must be orders in database that match this condition (Price < $2)");
            Assert.IsTrue(rowsDeleted == orders.Count, "The number of rows deleted must match the count of existing rows in database");
            Assert.IsTrue(newTotal == 0, "Must be 0 to indicate all records were deleted");
        }
        [TestMethod]
        public void BulkMerge_Options_Default()
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
            var result = dbContext.BulkMerge(orders, new BulkMergeOptions<Order>
            {
                MergeOnCondition = (s, t) => s.Id == t.Id,
                BatchSize = 1000
            });
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
        public void BulkMerge_Options_AutoMapIdentity()
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
            var result = dbContext.BulkMerge(orders, new BulkMergeOptions<Order>
            {
                MergeOnCondition = (s, t) => s.ExternalId == t.ExternalId,
                UsePermanentTable = true
            }) ;
            bool autoMapIdentityMatched = true;
            foreach(var order in orders)
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
        public void BulkUpdate()
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
            //Assert.IsTrue(entitiesWithChanges == 0, "There should be no pending Order entities with changes after BulkInsert completes");
        }
        [TestMethod]
        public void QueryToCsvFile()
        {
            var dbContext = SetupDbContext(true);
            var query = dbContext.Orders.Where(o => o.Price < 10M);
            int count = query.Count();
            var queryToCsvFileResult = query.QueryToCsvFile("QueryToCsvFile-Test.csv");

            Assert.IsTrue(count > 0, "There should be existing data in the source table");
            Assert.IsTrue(queryToCsvFileResult.DataRowCount == count, "The number of data rows written to the file should match the count from the database");
            Assert.IsTrue(queryToCsvFileResult.TotalRowCount == count + 1, "The total number of rows written to the file should match the count from the database plus the header row");
        }
        [TestMethod]
        public void QueryToCsvFile_Options_ColumnDelimiter_TextQualifer_HeaderRow()
        {
            var dbContext = SetupDbContext(true);
            var query = dbContext.Orders.Where(o => o.Price < 10M);
            int count = query.Count();
            var queryToCsvFileResult = query.QueryToCsvFile("QueryToCsvFile_Options_ColumnDelimiter_TextQualifer_HeaderRow-Test.csv", options => { options.ColumnDelimiter = "|"; options.TextQualifer = "\""; options.IncludeHeaderRow = false; });

            Assert.IsTrue(count > 0, "There should be existing data in the source table");
            Assert.IsTrue(queryToCsvFileResult.DataRowCount == count, "The number of data rows written to the file should match the count from the database");
            Assert.IsTrue(queryToCsvFileResult.TotalRowCount == count, "The total number of rows written to the file should match the count from the database without any header row");
        }
        [TestMethod]
        public void QueryToCsvFile_FileStream()
        {
            var dbContext = SetupDbContext(true);
            var query = dbContext.Orders.Where(o => o.Price < 10M);
            int count = query.Count();
            var fileStream = File.Create("QueryToCsvFile_Stream-Test.csv");
            var queryToCsvFileResult = query.QueryToCsvFile(fileStream);

            Assert.IsTrue(count > 0, "There should be existing data in the source table");
            Assert.IsTrue(queryToCsvFileResult.DataRowCount == count, "The number of data rows written to the file should match the count from the database");
            Assert.IsTrue(queryToCsvFileResult.TotalRowCount == count + 1, "The total number of rows written to the file should match the count from the database plus the header row");
        }
        [TestMethod]
        public void SqlQueryToCsvFile()
        {
            var dbContext = SetupDbContext(true);
            int count = dbContext.Orders.Where(o => o.Price > 5M).Count();
            var queryToCsvFileResult = dbContext.Database.SqlQueryToCsvFile("SqlQueryToCsvFile-Test.csv", "SELECT * FROM Orders WHERE Price > @Price", new SqlParameter("@Price", 5M));

            Assert.IsTrue(count > 0, "There should be existing data in the source table");
            Assert.IsTrue(queryToCsvFileResult.DataRowCount == count, "The number of data rows written to the file should match the count from the database");
            Assert.IsTrue(queryToCsvFileResult.TotalRowCount == count + 1, "The total number of rows written to the file should match the count from the database plus the header row");
        }
        [TestMethod]
        public void SqlQueryToCsvFile_Options_ColumnDelimiter_TextQualifer()
        {
            var dbContext = SetupDbContext(true);
            string filePath = "SqlQueryToCsvFile_Options_ColumnDelimiter_TextQualifer-Test.csv";
            int count = dbContext.Orders.Where(o => o.Price > 5M).Count();
            var queryToCsvFileResult = dbContext.Database.SqlQueryToCsvFile(filePath, options => { options.ColumnDelimiter = "|"; options.TextQualifer = "\""; },
                "SELECT * FROM Orders WHERE Price > @Price", new SqlParameter("@Price", 5M));

            Assert.IsTrue(count > 0, "There should be existing data in the source table");
            Assert.IsTrue(queryToCsvFileResult.DataRowCount == count, "The number of data rows written to the file should match the count from the database");
            Assert.IsTrue(queryToCsvFileResult.TotalRowCount == count + 1, "The total number of rows written to the file should match the count from the database plus the header row");
        }
        [TestMethod]
        public void Sql_SqlQuery_Count()
        {
            var dbContext = SetupDbContext(true);
            int efCount = dbContext.Orders.Where(o => o.Price > 5M).Count();
            var sqlCount = dbContext.Database.FromSqlQuery("SELECT * FROM Orders WHERE Price > @Price", new SqlParameter("@Price", 5M)).Count();

            Assert.IsTrue(efCount > 0, "Count from EF should be greater than zero");
            Assert.IsTrue(efCount > 0, "Count from SQL should be greater than zero");
            Assert.IsTrue(efCount == sqlCount, "Count from EF should match the count from the SqlQuery");
        }
        [TestMethod]
        public void Sql_TableExists()
        {
            var dbContext = SetupDbContext(true);
            int efCount = dbContext.Orders.Where(o => o.Price > 5M).Count();
            bool ordersTableExists = dbContext.Database.TableExists("Orders");
            bool orderNewTableExists = dbContext.Database.TableExists("OrdersNew");

            Assert.IsTrue(ordersTableExists, "Orders table should exist");
            Assert.IsTrue(!orderNewTableExists, "Orders_New table should not exist");
        }
    }
}
