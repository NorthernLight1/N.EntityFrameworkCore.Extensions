using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace N.EntityFrameworkCore.Extensions.Test.DataContextExtensions
{
    [TestClass]
    public class Fetch : DataContextExtensionsBase
    {
        [TestMethod]
        public void With_DateTime()
        {
            var dbContext = SetupDbContext(true);
            int batchSize = 1000;
            int batchCount = 0;
            int totalCount = 0;
            DateTime dateTime = dbContext.Orders.Max(o => o.AddedDateTime).AddDays(-30);
            var orders = dbContext.Orders.Where(o => o.AddedDateTime <= dateTime);
            int expectedTotalCount = orders.Count();
            int expectedBatchCount = (int)Math.Ceiling(expectedTotalCount / (decimal)batchSize);

            orders.Fetch(result =>
            {
                batchCount++;
                totalCount += result.Results.Count();
                Assert.IsTrue(result.Results.Count <= batchSize, "The count of results in each batch callback should less than or equal to the batchSize");
            }, options => { options.BatchSize = batchSize; });

            Assert.IsTrue(expectedTotalCount > 0, "There must be orders in database that match this condition");
            Assert.IsTrue(expectedTotalCount == totalCount, "The total number of rows fetched must match the count of existing rows in database");
            Assert.IsTrue(expectedBatchCount == batchCount, "The total number of batches fetched must match what is expected");
        }
        [TestMethod]
        public void With_Decimal()
        {
            var dbContext = SetupDbContext(true);
            int batchSize = 1000;
            int batchCount = 0;
            int totalCount = 0;
            var orders = dbContext.Orders.Where(o => o.Price < 10M);
            int expectedTotalCount = orders.Count();
            int expectedBatchCount = (int)Math.Ceiling(expectedTotalCount / (decimal)batchSize);

            orders.Fetch(result =>
            {
                batchCount++;
                totalCount += result.Results.Count();
                Assert.IsTrue(result.Results.Count <= batchSize, "The count of results in each batch callback should less than or equal to the batchSize");
            }, options => { options.BatchSize = batchSize; });

            Assert.IsTrue(expectedTotalCount > 0, "There must be orders in database that match this condition");
            Assert.IsTrue(expectedTotalCount == totalCount, "The total number of rows fetched must match the count of existing rows in database");
            Assert.IsTrue(expectedBatchCount == batchCount, "The total number of batches fetched must match what is expected");
        }
    }
}
