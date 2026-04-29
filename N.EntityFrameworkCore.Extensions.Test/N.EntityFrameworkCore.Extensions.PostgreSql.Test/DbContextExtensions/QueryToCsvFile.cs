using System.IO;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions;

[TestClass]
public class QueryToCsvFile : DbContextExtensionsBase
{
    [TestMethod]
    public void With_Default_Options()
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
    public void With_Options_ColumnDelimiter_TextQualifer_HeaderRow()
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
    public void Using_FileStream()
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
}