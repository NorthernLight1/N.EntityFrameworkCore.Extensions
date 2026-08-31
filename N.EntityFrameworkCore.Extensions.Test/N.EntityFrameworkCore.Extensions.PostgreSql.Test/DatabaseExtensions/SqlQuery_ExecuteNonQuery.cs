using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Common;

namespace N.EntityFrameworkCore.Extensions.Test.DatabaseExtensions;

[TestClass]
public class SqlQuery_ExecuteNonQuery : DatabaseExtensionsBase
{
    [TestMethod]
    public void With_Update_Statement()
    {
        var dbContext = SetupDbContext(true);
        string sql = $"UPDATE {Config.DelimitTableName("Orders")} SET {Config.DelimitIdentifier("Price")} = {Config.DelimitIdentifier("Price")} WHERE {Config.DelimitIdentifier("Price")} > @Price";
        int rows = dbContext.Database.FromSqlQuery(sql, Config.CreateParameter("@Price", 100000M)).ExecuteNonQuery();
        Assert.IsTrue(rows >= 0);
    }
}