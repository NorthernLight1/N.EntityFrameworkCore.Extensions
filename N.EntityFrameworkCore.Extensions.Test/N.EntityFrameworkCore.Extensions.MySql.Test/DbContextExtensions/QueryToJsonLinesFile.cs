using System.IO;
using System.Linq;
using System.Text.Json;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions;

[TestClass]
public class QueryToJsonLinesFile : DbContextExtensionsBase
{
    [TestMethod]
    public void With_Default_Options()
    {
        const string fileName = "QueryToJsonLinesFile-Test.jsonl";
        if (File.Exists(fileName))
            File.Delete(fileName);

        var dbContext = SetupDbContext(true);
        var query = dbContext.Orders.Where(o => o.Price < 10M);
        int count = query.Count();
        var result = query.QueryToJsonLinesFile(fileName);

        Assert.IsTrue(count > 0, "There should be existing data in the source table");
        Assert.AreEqual(count, result.DataRowCount);
        Assert.AreEqual(count, result.TotalRowCount);

        var lines = File.ReadAllLines(fileName).Where(line => !string.IsNullOrWhiteSpace(line)).ToArray();
        Assert.AreEqual(count, lines.Length);
        using var document = JsonDocument.Parse(lines[0]);
        Assert.AreEqual(JsonValueKind.Object, document.RootElement.ValueKind);
    }
}
