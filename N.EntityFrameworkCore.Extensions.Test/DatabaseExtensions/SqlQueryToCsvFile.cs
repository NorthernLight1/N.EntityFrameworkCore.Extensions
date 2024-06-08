using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace N.EntityFrameworkCore.Extensions.Test.DatabaseExtensions
{
    [TestClass]
    public class SqlQueryToCsvFile : DatabaseExtensionsBase
    {
        [TestMethod]
        public void With_Default_Options()
        {
            var dbContext = SetupDbContext(true);
            int count = dbContext.Orders.Where(o => o.Price > 5M).Count();
            var queryToCsvFileResult = dbContext.Database.SqlQueryToCsvFile("SqlQueryToCsvFile-Test.csv", "SELECT * FROM Orders WHERE Price > @Price", new SqlParameter("@Price", 5M));

            Assert.IsTrue(count > 0, "There should be existing data in the source table");
            Assert.IsTrue(queryToCsvFileResult.DataRowCount == count, "The number of data rows written to the file should match the count from the database");
            Assert.IsTrue(queryToCsvFileResult.TotalRowCount == count + 1, "The total number of rows written to the file should match the count from the database plus the header row");
        }
        [TestMethod]
        public void With_Options_ColumnDelimiter_TextQualifer()
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
    }
}