using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace N.EntityFrameworkCore.Extensions.Test.DatabaseExtensions;

[TestClass]
public class TableHasIdentity : DatabaseExtensionsBase
{
    [TestMethod]
    public void With_Orders_Table()
    {
        var dbContext = SetupDbContext(false);
        Assert.IsTrue(dbContext.Database.TableHasIdentity("Orders"), "Orders.Id should be an identity/autoincrement column");
    }
}
