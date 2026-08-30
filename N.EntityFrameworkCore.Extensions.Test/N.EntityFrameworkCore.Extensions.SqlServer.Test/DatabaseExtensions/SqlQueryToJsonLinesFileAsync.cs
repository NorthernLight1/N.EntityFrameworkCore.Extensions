using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace N.EntityFrameworkCore.Extensions.Test.DatabaseExtensions;

[TestClass]
public class SqlQueryToJsonLinesFileAsync : DatabaseExtensionsBase
{
    [TestMethod]
    public async Task With_Default_Options()
    {
        const string fileName = "SqlQueryToJsonLinesFileAsync-Test.jsonl";
        if (File.Exists(fileName))
            File.Delete(fileName);

        var dbContext = SetupDbContext(true);
        string sql = "SELECT * FROM Orders WHERE Price < @Price";
        int count = dbContext.Orders.Count(o => o.Price < 5M);
        var result = await dbContext.Database.SqlQueryToJsonLinesFileAsync(fileName, sql, new object[] { new Microsoft.Data.SqlClient.SqlParameter("@Price", 5M) });

        Assert.IsTrue(count > 0, "There should be existing data in the source table");
        Assert.AreEqual(count, result.DataRowCount);
        Assert.AreEqual(count, result.TotalRowCount);

        var lines = (await File.ReadAllLinesAsync(fileName)).Where(line => !string.IsNullOrWhiteSpace(line)).ToArray();
        Assert.AreEqual(count, lines.Length);
        using var document = JsonDocument.Parse(lines[0]);
        Assert.AreEqual(JsonValueKind.Object, document.RootElement.ValueKind);
    }
}
