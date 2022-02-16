using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions
{
    [TestClass]
    public class QueryToCsvFileAsync : DbContextExtensionsBase
    {
        [TestMethod]
        public async Task With_Default_Options()
        {
            var dbContext = SetupDbContext(true);
            var query = dbContext.Orders.Where(o => o.Price < 10M);
            int count = query.Count();
            var queryToCsvFileResult = await query.QueryToCsvFileAsync("QueryToCsvFile-Test.csv");

            Assert.IsTrue(count > 0, "There should be existing data in the source table");
            Assert.IsTrue(queryToCsvFileResult.DataRowCount == count, "The number of data rows written to the file should match the count from the database");
            Assert.IsTrue(queryToCsvFileResult.TotalRowCount == count + 1, "The total number of rows written to the file should match the count from the database plus the header row");
        }
        [TestMethod]
        public async Task With_Options_ColumnDelimiter_TextQualifer_HeaderRow()
        {
            var dbContext = SetupDbContext(true);
            var query = dbContext.Orders.Where(o => o.Price < 10M);
            int count = query.Count();
            var queryToCsvFileResult = await query.QueryToCsvFileAsync("QueryToCsvFile_Options_ColumnDelimiter_TextQualifer_HeaderRow-Test.csv", options => { options.ColumnDelimiter = "|"; options.TextQualifer = "\""; options.IncludeHeaderRow = false; });

            Assert.IsTrue(count > 0, "There should be existing data in the source table");
            Assert.IsTrue(queryToCsvFileResult.DataRowCount == count, "The number of data rows written to the file should match the count from the database");
            Assert.IsTrue(queryToCsvFileResult.TotalRowCount == count, "The total number of rows written to the file should match the count from the database without any header row");
        }
        [TestMethod]
        public async Task Using_FileStream()
        {
            var dbContext = SetupDbContext(true);
            var query = dbContext.Orders.Where(o => o.Price < 10M);
            int count = query.Count();
            var fileStream = File.Create("QueryToCsvFile_Stream-Test.csv");
            var queryToCsvFileResult = await query.QueryToCsvFileAsync(fileStream);

            Assert.IsTrue(count > 0, "There should be existing data in the source table");
            Assert.IsTrue(queryToCsvFileResult.DataRowCount == count, "The number of data rows written to the file should match the count from the database");
            Assert.IsTrue(queryToCsvFileResult.TotalRowCount == count + 1, "The total number of rows written to the file should match the count from the database plus the header row");
        }
    }
}
