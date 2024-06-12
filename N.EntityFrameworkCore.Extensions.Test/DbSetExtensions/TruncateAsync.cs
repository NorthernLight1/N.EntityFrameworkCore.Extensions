using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Data;
using N.EntityFrameworkCore.Extensions.Test.DbContextExtensions;

namespace N.EntityFrameworkCore.Extensions.Test.DbSetExtensions;

[TestClass]
public class TruncateAsync : DbContextExtensionsBase
{
    [TestMethod]
    public async Task Using_Dbset()
    {
        var dbContext = SetupDbContext(true);
        int oldOrdersCount = dbContext.Orders.Count();
        await dbContext.Orders.TruncateAsync();
        int newOrdersCount = dbContext.Orders.Count();

        Assert.IsTrue(oldOrdersCount > 0, "Orders table should have data");
        Assert.IsTrue(newOrdersCount == 0, "Order table should be empty after truncating");
    }
}