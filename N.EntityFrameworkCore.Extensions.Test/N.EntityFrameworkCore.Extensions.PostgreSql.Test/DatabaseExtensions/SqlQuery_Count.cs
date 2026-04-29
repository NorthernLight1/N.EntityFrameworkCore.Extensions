using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Common;

namespace N.EntityFrameworkCore.Extensions.Test.DatabaseExtensions;

[TestClass]
public class SqlQuery_Count : DatabaseExtensionsBase
{
    [TestMethod]
    public void With_Decimal_Value()
    {
        var dbContext = SetupDbContext(true);
        int efCount = dbContext.Orders.Where(o => o.Price > 5M).Count();
        string sql = $"SELECT * FROM {Config.DelimitTableName("Orders")} WHERE {Config.DelimitIdentifier("Price")} > @Price";
        var sqlCount = dbContext.Database.FromSqlQuery(sql, Config.CreateParameter("@Price", 5M)).Count();

        Assert.IsTrue(efCount > 0, "Count from EF should be greater than zero");
        Assert.IsTrue(efCount > 0, "Count from SQL should be greater than zero");
        Assert.IsTrue(efCount == sqlCount, "Count from EF should match the count from the SqlQuery");
    }
    [TestMethod]
    public void With_OrderBy()
    {
        var dbContext = SetupDbContext(true);
        int efCount = dbContext.Orders.Where(o => o.Price > 5M).Count();
        string sql = $"SELECT * FROM {Config.DelimitTableName("Orders")} WHERE {Config.DelimitIdentifier("Price")} > @Price ORDER BY {Config.DelimitIdentifier("Id")}";
        var sqlCount = dbContext.Database.FromSqlQuery(sql, Config.CreateParameter("@Price", 5M)).Count();

        Assert.IsTrue(efCount > 0, "Count from EF should be greater than zero");
        Assert.IsTrue(efCount > 0, "Count from SQL should be greater than zero");
        Assert.IsTrue(efCount == sqlCount, "Count from EF should match the count from the SqlQuery");
    }
}
