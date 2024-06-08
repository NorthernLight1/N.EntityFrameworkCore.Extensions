using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace N.EntityFrameworkCore.Extensions.Test.DatabaseExtensions
{
    [TestClass]
    public class SqlQuery_CountAsync : DatabaseExtensionsBase
    {
        [TestMethod]
        public async Task With_Decimal_Value()
        {
            var dbContext = SetupDbContext(true);
            int efCount = dbContext.Orders.Where(o => o.Price > 5M).Count();
            var sqlCount = await dbContext.Database.FromSqlQuery("SELECT * FROM Orders WHERE Price > @Price", new SqlParameter("@Price", 5M)).CountAsync();

            Assert.IsTrue(efCount > 0, "Count from EF should be greater than zero");
            Assert.IsTrue(efCount > 0, "Count from SQL should be greater than zero");
            Assert.IsTrue(efCount == sqlCount, "Count from EF should match the count from the SqlQuery");
        }
        [TestMethod]
        public async Task With_OrderBy()
        {
            var dbContext = SetupDbContext(true);
            int efCount = dbContext.Orders.Where(o => o.Price > 5M).Count();
            var sqlCount = await dbContext.Database.FromSqlQuery("SELECT * FROM Orders WHERE Price > @Price ORDER BY Id", new SqlParameter("@Price", 5M)).CountAsync();

            Assert.IsTrue(efCount > 0, "Count from EF should be greater than zero");
            Assert.IsTrue(efCount > 0, "Count from SQL should be greater than zero");
            Assert.IsTrue(efCount == sqlCount, "Count from EF should match the count from the SqlQuery");
        }
    }
}