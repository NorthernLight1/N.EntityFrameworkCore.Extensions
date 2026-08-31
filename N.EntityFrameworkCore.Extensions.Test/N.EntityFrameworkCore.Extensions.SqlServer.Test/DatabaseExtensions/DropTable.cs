using Microsoft.EntityFrameworkCore;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace N.EntityFrameworkCore.Extensions.Test.DatabaseExtensions;

[TestClass]
public class DropTable : DatabaseExtensionsBase
{
    [TestMethod]
    public void With_Existing_Table()
    {
        var dbContext = SetupDbContext(false);
        string tableName = "DropTableTest_Existing";
        dbContext.Database.ExecuteSqlRaw("CREATE TABLE DropTableTest_Existing (Id INT)");
        Assert.IsTrue(dbContext.Database.TableExists(tableName));

        dbContext.Database.DropTable(tableName);

        Assert.IsFalse(dbContext.Database.TableExists(tableName));
    }

    [TestMethod]
    public void With_IfExists_Missing_Table()
    {
        var dbContext = SetupDbContext(false);
        dbContext.Database.DropTable("DropTableTest_Missing_XYZ", true);
    }
}
