using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace N.EntityFrameworkCore.Extensions.Test.DatabaseExtensions;

[TestClass]
public class ClearTable : DatabaseExtensionsBase
{
    [TestMethod]
    public void With_Orders_Table()
    {
        var dbContext = SetupDbContext(true);
        int oldCount = dbContext.Orders.Count();
        dbContext.Database.ClearTable("Orders");
        int newCount = dbContext.Orders.Count();

        Assert.IsTrue(oldCount > 0, "Orders table should have data");
        Assert.AreEqual(0, newCount, "Orders table should be empty after ClearTable");
    }
}
