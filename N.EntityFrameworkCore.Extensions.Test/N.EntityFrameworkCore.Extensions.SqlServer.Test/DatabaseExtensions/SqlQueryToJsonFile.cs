using System.IO;
using System.Linq;
using System.Text.Json;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Common;

namespace N.EntityFrameworkCore.Extensions.Test.DatabaseExtensions;

[TestClass]
public class SqlQueryToJsonFile : DatabaseExtensionsBase
{
    [TestMethod]
    public void With_Default_Options()
    {
        var dbContext = SetupDbContext(true);
        string sql = "SELECT * FROM Orders WHERE Price < @Price";
        int count = dbContext.Orders.Count(o => o.Price < 5M);
        var result = dbContext.Database.SqlQueryToJsonFile("SqlQueryToJsonFile-Test.json", sql, Config.CreateParameter("@Price", 5M));

        Assert.IsTrue(count > 0, "There should be existing data in the source table");
        Assert.AreEqual(count, result.DataRowCount);

        using var document = JsonDocument.Parse(File.ReadAllText("SqlQueryToJsonFile-Test.json"));
        Assert.AreEqual(count, document.RootElement.GetArrayLength());
    }
}
