using System.IO;
using System.Linq;
using System.Text.Json;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions;

[TestClass]
public class QueryToJsonFile : DbContextExtensionsBase
{
    [TestMethod]
    public void With_Default_Options()
    {
        var dbContext = SetupDbContext(true);
        var query = dbContext.Orders.Where(o => o.Price < 10M);
        int count = query.Count();
        var result = query.QueryToJsonFile("QueryToJsonFile-Test.json");

        Assert.IsTrue(count > 0, "There should be existing data in the source table");
        Assert.AreEqual(count, result.DataRowCount);
        Assert.AreEqual(count, result.TotalRowCount);

        using var document = JsonDocument.Parse(File.ReadAllText("QueryToJsonFile-Test.json"));
        Assert.AreEqual(JsonValueKind.Array, document.RootElement.ValueKind);
        Assert.AreEqual(count, document.RootElement.GetArrayLength());
    }

    [TestMethod]
    public void Using_FileStream()
    {
        var dbContext = SetupDbContext(true);
        var query = dbContext.Orders.Where(o => o.Price < 10M);
        int count = query.Count();
        using var fileStream = File.Create("QueryToJsonFile_Stream-Test.json");
        var result = query.QueryToJsonFile(fileStream, options => options.Indented = false);

        Assert.AreEqual(count, result.DataRowCount);
        Assert.AreEqual(count, result.TotalRowCount);
    }
}
