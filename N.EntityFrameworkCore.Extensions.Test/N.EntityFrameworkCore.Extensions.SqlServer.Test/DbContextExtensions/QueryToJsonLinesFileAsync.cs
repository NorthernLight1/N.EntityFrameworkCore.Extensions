using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions;

[TestClass]
public class QueryToJsonLinesFileAsync : DbContextExtensionsBase
{
    [TestMethod]
    public async Task With_Default_Options()
    {
        const string fileName = "QueryToJsonLinesFileAsync-Test.jsonl";
        if (File.Exists(fileName))
            File.Delete(fileName);

        var dbContext = SetupDbContext(true);
        var query = dbContext.Orders.Where(o => o.Price < 10M);
        int count = query.Count();
        var result = await query.QueryToJsonLinesFileAsync(fileName);

        Assert.IsTrue(count > 0, "There should be existing data in the source table");
        Assert.AreEqual(count, result.DataRowCount);
        Assert.AreEqual(count, result.TotalRowCount);

        var lines = (await File.ReadAllLinesAsync(fileName)).Where(line => !string.IsNullOrWhiteSpace(line)).ToArray();
        Assert.AreEqual(count, lines.Length);
        using var document = JsonDocument.Parse(lines[0]);
        Assert.AreEqual(JsonValueKind.Object, document.RootElement.ValueKind);
    }
}
