using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions;

[TestClass]
public class QueryToJsonFileAsync : DbContextExtensionsBase
{
    [TestMethod]
    public async Task With_Default_Options()
    {
        var dbContext = SetupDbContext(true);
        var query = dbContext.Orders.Where(o => o.Price < 10M);
        int count = query.Count();
        var result = await query.QueryToJsonFileAsync("QueryToJsonFileAsync-Test.json");

        Assert.IsTrue(count > 0, "There should be existing data in the source table");
        Assert.AreEqual(count, result.DataRowCount);
        Assert.AreEqual(count, result.TotalRowCount);

        using var document = JsonDocument.Parse(await File.ReadAllTextAsync("QueryToJsonFileAsync-Test.json"));
        Assert.AreEqual(JsonValueKind.Array, document.RootElement.ValueKind);
        Assert.AreEqual(count, document.RootElement.GetArrayLength());
    }
}
