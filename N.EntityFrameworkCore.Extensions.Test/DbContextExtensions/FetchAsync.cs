using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions
{
    [TestClass]
    public class FetchAsync : DbContextExtensionsBase
    {
        [TestMethod]
        public async Task With_BulkInsert()
        {
            var dbContext = SetupDbContext(true);
            DateTime dateTime = dbContext.Orders.Max(o => o.AddedDateTime).AddDays(-30);
            var orders = dbContext.Orders.Where(o => o.AddedDateTime <= dateTime);
            int totalOrdersToFetch = orders.Count();
            int totalOrdersFetched = 0;
            int batchSize = 5000;
            await orders.FetchAsync(async result =>
            {
                totalOrdersFetched += result.Results.Count;
                var ordersFetched = result.Results;
                foreach (var orderFetched in ordersFetched)
                {
                    orderFetched.Price = 75;
                }
                await dbContext.BulkInsertAsync(ordersFetched);
            }, options => { options.BatchSize = batchSize; });

            int totalOrder = orders.Count();
            int totalOrderInserted = orders.Where(o => o.Price == 75).Count();
            Assert.IsTrue(totalOrdersToFetch == totalOrdersFetched, "The total number of rows fetched must match the number of rows to fetch");
            Assert.IsTrue(totalOrderInserted == totalOrdersFetched, "The total number of rows updated must match the number of rows that were fetched");
            Assert.IsTrue(totalOrder - totalOrdersToFetch == totalOrderInserted, "The total number of rows must match the number of rows that were updated");
        }
        [TestMethod]
        public async Task With_BulkUpdate()
        {
            var dbContext = SetupDbContext(true);
            DateTime dateTime = dbContext.Orders.Max(o => o.AddedDateTime).AddDays(-30);
            var orders = dbContext.Orders.Where(o => o.AddedDateTime <= dateTime);
            int totalOrdersToFetch = orders.Count();
            int totalOrdersFetched = 0;
            int batchSize = 5000;
            await orders.FetchAsync(async result =>
            {
                totalOrdersFetched += result.Results.Count;
                var ordersFetched = result.Results;
                foreach (var orderFetched in ordersFetched)
                {
                    orderFetched.Price = 75;
                }
                await dbContext.BulkUpdateAsync(ordersFetched);
            }, options => { options.BatchSize = batchSize; });

            int totalOrder = orders.Count();
            int totalOrderUpdated = orders.Where(o => o.Price == 75).Count();
            Assert.IsTrue(totalOrdersToFetch == totalOrdersFetched, "The total number of rows fetched must match the number of rows to fetch");
            Assert.IsTrue(totalOrderUpdated == totalOrdersFetched, "The total number of rows updated must match the number of rows that were fetched");
            Assert.IsTrue(totalOrder == totalOrderUpdated, "The total number of rows must match the number of rows that were updated");
        }
        [TestMethod]
        public async Task With_DateTime()
        {
            var dbContext = SetupDbContext(true);
            int batchSize = 1000;
            int batchCount = 0;
            int totalCount = 0;
            DateTime dateTime = dbContext.Orders.Max(o => o.AddedDateTime).AddDays(-30);
            var orders = dbContext.Orders.Where(o => o.AddedDateTime <= dateTime);
            int expectedTotalCount = orders.Count();
            int expectedBatchCount = (int)Math.Ceiling(expectedTotalCount / (decimal)batchSize);

            await orders.FetchAsync(async result =>
            {
                await Task.Run(() =>
                {
                    batchCount++;
                    totalCount += result.Results.Count;
                    Assert.IsTrue(result.Results.Count <= batchSize, "The count of results in each batch callback should less than or equal to the batchSize");
                });
            }, options => { options.BatchSize = batchSize; });

            Assert.IsTrue(expectedTotalCount > 0, "There must be orders in database that match this condition");
            Assert.IsTrue(expectedTotalCount == totalCount, "The total number of rows fetched must match the count of existing rows in database");
            Assert.IsTrue(expectedBatchCount == batchCount, "The total number of batches fetched must match what is expected");
        }
        [TestMethod]
        public async Task With_Decimal()
        {
            var dbContext = SetupDbContext(true);
            int batchSize = 1000;
            int batchCount = 0;
            int totalCount = 0;
            var orders = dbContext.Orders.Where(o => o.Price < 10M);
            int expectedTotalCount = orders.Count();
            int expectedBatchCount = (int)Math.Ceiling(expectedTotalCount / (decimal)batchSize);

            await orders.FetchAsync(async result =>
            {
                await Task.Run(() =>
                {
                    batchCount++;
                    totalCount += result.Results.Count();
                    Assert.IsTrue(result.Results.Count <= batchSize, "The count of results in each batch callback should less than or equal to the batchSize");
                });
            }, options => { options.BatchSize = batchSize; });

            Assert.IsTrue(expectedTotalCount > 0, "There must be orders in database that match this condition");
            Assert.IsTrue(expectedTotalCount == totalCount, "The total number of rows fetched must match the count of existing rows in database");
            Assert.IsTrue(expectedBatchCount == batchCount, "The total number of batches fetched must match what is expected");
        }
        [TestMethod]
        public async Task With_Enum()
        {
            var dbContext = SetupDbContext(true);
            int batchSize = 1000;
            int batchCount = 0;
            int totalCount = 0;
            var products = dbContext.Products.Where(o => o.Price < 10M);
            int expectedTotalCount = products.Count();
            int expectedBatchCount = (int)Math.Ceiling(expectedTotalCount / (decimal)batchSize);

            await products.FetchAsync(async result =>
            {
                await Task.Run(() =>
                {
                    batchCount++;
                    totalCount += result.Results.Count();
                    Assert.IsTrue(result.Results.Count <= batchSize, "The count of results in each batch callback should less than or equal to the batchSize");
                });
            }, options => { options.BatchSize = batchSize; });

            Assert.IsTrue(expectedTotalCount > 0, "There must be products in database that match this condition");
            Assert.IsTrue(expectedTotalCount == totalCount, "The total number of rows fetched must match the count of existing rows in database");
            Assert.IsTrue(expectedBatchCount == batchCount, "The total number of batches fetched must match what is expected");
        }
        [TestMethod]
        public async Task With_Options_IgnoreColumns()
        {
            var dbContext = SetupDbContext(true);
            int batchSize = 1000;
            int batchCount = 0;
            int totalCount = 0;
            var orders = dbContext.Orders.Where(o => o.Price < 10M);
            int expectedTotalCount = orders.Count();
            int expectedBatchCount = (int)Math.Ceiling(expectedTotalCount / (decimal)batchSize);

            await orders.FetchAsync(async result =>
            {
                await Task.Run(() =>
                {
                    batchCount++;
                    totalCount += result.Results.Count;
                    bool isAllExternalIdNull = !result.Results.Any(o => o.ExternalId != null);
                    Assert.IsTrue(isAllExternalIdNull, "All records should have ExternalId equal to NULL since it was not loaded.");
                    Assert.IsTrue(result.Results.Count <= batchSize, "The count of results in each batch callback should less than or equal to the batchSize");
                });
            }, options => { options.BatchSize = batchSize; options.IgnoreColumns = s => new { s.ExternalId }; });

            Assert.IsTrue(expectedTotalCount > 0, "There must be orders in database that match this condition");
            Assert.IsTrue(expectedTotalCount == totalCount, "The total number of rows fetched must match the count of existing rows in database");
            Assert.IsTrue(expectedBatchCount == batchCount, "The total number of batches fetched must match what is expected");
        }
        [TestMethod]
        public async Task With_Options_InputColumns()
        {
            var dbContext = SetupDbContext(true);
            int batchSize = 1000;
            int batchCount = 0;
            int totalCount = 0;
            var orders = dbContext.Orders.Where(o => o.Price < 10M);
            int expectedTotalCount = orders.Count();
            int expectedBatchCount = (int)Math.Ceiling(expectedTotalCount / (decimal)batchSize);

            await orders.FetchAsync(async result =>
            {
                await Task.Run(() =>
                {
                    batchCount++;
                    totalCount += result.Results.Count();
                    bool isAllExternalIdNull = !result.Results.Any(o => o.ExternalId != null);
                    Assert.IsTrue(isAllExternalIdNull, "All records should have ExternalId equal to NULL since it was not loaded.");
                    Assert.IsTrue(result.Results.Count <= batchSize, "The count of results in each batch callback should less than or equal to the batchSize");
                });
            }, options => { options.BatchSize = batchSize; options.InputColumns = s => new { s.Id, s.Price }; });

            Assert.IsTrue(expectedTotalCount > 0, "There must be orders in database that match this condition");
            Assert.IsTrue(expectedTotalCount == totalCount, "The total number of rows fetched must match the count of existing rows in database");
            Assert.IsTrue(expectedBatchCount == batchCount, "The total number of batches fetched must match what is expected");
        }
    }
}