using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace N.EntityFrameworkCore.Extensions.Test.DatabaseExtensions;

[TestClass]
public class ClearTableAsync : DatabaseExtensionsBase
{
    [TestMethod]
    public async Task With_Orders_Table()
    {
        var dbContext = SetupDbContext(true);
        int oldCount = dbContext.Orders.Count();
        await dbContext.Database.ClearTableAsync("Orders");
        int newCount = dbContext.Orders.Count();

        Assert.IsTrue(oldCount > 0, "Orders table should have data");
        Assert.AreEqual(0, newCount, "Orders table should be empty after ClearTableAsync");
    }
}
