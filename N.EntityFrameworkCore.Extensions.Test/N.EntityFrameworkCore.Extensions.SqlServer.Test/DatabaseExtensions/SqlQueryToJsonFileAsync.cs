using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace N.EntityFrameworkCore.Extensions.Test.DatabaseExtensions;

[TestClass]
public class SqlQueryToJsonFileAsync : DatabaseExtensionsBase
{
    [TestMethod]
    public async Task With_Default_Options()
    {
        var dbContext = SetupDbContext(true);
        string sql = "SELECT * FROM Orders WHERE Price < @Price";
        int count = dbContext.Orders.Count(o => o.Price < 5M);
        var result = await dbContext.Database.SqlQueryToJsonFileAsync("SqlQueryToJsonFileAsync-Test.json", sql, new object[] { new Microsoft.Data.SqlClient.SqlParameter("@Price", 5M) });

        Assert.IsTrue(count > 0, "There should be existing data in the source table");
        Assert.AreEqual(count, result.DataRowCount);

        using var document = JsonDocument.Parse(await File.ReadAllTextAsync("SqlQueryToJsonFileAsync-Test.json"));
        Assert.AreEqual(JsonValueKind.Array, document.RootElement.ValueKind);
        Assert.AreEqual(count, document.RootElement.GetArrayLength());
    }
}
