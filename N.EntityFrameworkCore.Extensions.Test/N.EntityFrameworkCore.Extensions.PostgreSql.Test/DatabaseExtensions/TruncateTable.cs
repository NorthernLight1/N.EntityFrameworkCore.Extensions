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
    public void With_Orders_Table_Restarts_Identity()
    {
        var dbContext = SetupDbContext(true);
        Assert.IsTrue(dbContext.Orders.Max(o => o.Id) > 1);
        dbContext.Database.TruncateTable("Orders");
        Assert.AreEqual(0, dbContext.Orders.Count());

        dbContext.Orders.Add(new Order { Price = 2.22M });
        dbContext.SaveChanges();
        dbContext.ChangeTracker.Clear();

        Assert.AreEqual(1L, dbContext.Orders.Single().Id, "PostgreSQL TruncateTable should RESTART IDENTITY");
    }
}
