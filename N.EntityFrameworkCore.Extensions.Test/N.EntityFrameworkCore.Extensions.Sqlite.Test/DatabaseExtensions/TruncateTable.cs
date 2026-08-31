using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Data;

namespace N.EntityFrameworkCore.Extensions.Test.DatabaseExtensions;

[TestClass]
public class TruncateTable : DatabaseExtensionsBase
{
    [TestMethod]
    public void With_Orders_Table()
    {
        var dbContext = SetupDbContext(true);
        int oldOrdersCount = dbContext.Orders.Count();
        dbContext.Database.TruncateTable("Orders");
        int newOrdersCount = dbContext.Orders.Count();

        Assert.IsTrue(oldOrdersCount > 0, "Orders table should have data");
        Assert.IsTrue(newOrdersCount == 0, "Order table should be empty after truncating");
    }

    [TestMethod]
    public void With_IfExists_Missing_Table()
    {
        var dbContext = SetupDbContext(false);
        dbContext.Database.TruncateTable("TruncateMissingTable_XYZ", true);
    }

    [TestMethod]
    public void With_Orders_Table_Does_Not_Reset_Autoincrement()
    {
        var dbContext = SetupDbContext(true);
        long maxIdBefore = dbContext.Orders.Max(o => o.Id);
        dbContext.Database.TruncateTable("Orders");
        Assert.AreEqual(0, dbContext.Orders.Count());

        dbContext.Orders.Add(new Order { Price = 2.22M });
        dbContext.SaveChanges();
        dbContext.ChangeTracker.Clear();

        long newId = dbContext.Orders.Single().Id;
        Assert.IsTrue(newId > maxIdBefore, "SQLite TruncateTable uses DELETE and must not reset AUTOINCREMENT");
    }
}