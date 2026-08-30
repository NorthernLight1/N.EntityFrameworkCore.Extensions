using System.IO;
using System.Linq;
using System.Text.Json;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Common;

namespace N.EntityFrameworkCore.Extensions.Test.DatabaseExtensions;

[TestClass]
public class SqlQueryToJsonLinesFile : DatabaseExtensionsBase
{
    [TestMethod]
    public void With_Default_Options()
    {
        const string fileName = "SqlQueryToJsonLinesFile-Test.jsonl";
        if (File.Exists(fileName))
            File.Delete(fileName);

        var dbContext = SetupDbContext(true);
        string sql = $"SELECT * FROM {Config.DelimitTableName("Orders")} WHERE {Config.DelimitIdentifier("Price")} < @Price";
        int count = dbContext.Orders.Count(o => o.Price < 5M);
        var result = dbContext.Database.SqlQueryToJsonLinesFile(fileName, sql, Config.CreateParameter("@Price", 5M));

        Assert.IsTrue(count > 0, "There should be existing data in the source table");
        Assert.AreEqual(count, result.DataRowCount);
        Assert.AreEqual(count, result.TotalRowCount);

        var lines = File.ReadAllLines(fileName).Where(line => !string.IsNullOrWhiteSpace(line)).ToArray();
        Assert.AreEqual(count, lines.Length);
        using var document = JsonDocument.Parse(lines[0]);
        Assert.AreEqual(JsonValueKind.Object, document.RootElement.ValueKind);
    }
}
