﻿using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace N.EntityFrameworkCore.Extensions.Test.DatabaseExtensions;

[TestClass]
public class TruncateTableAsync : DatabaseExtensionsBase
{
    [TestMethod]
    public async Task With_Orders_Table()
    {
        var dbContext = SetupDbContext(true);
        int oldOrdersCount = dbContext.Orders.Count();
        await dbContext.Database.TruncateTableAsync("Orders");
        int newOrdersCount = dbContext.Orders.Count();

        Assert.IsTrue(oldOrdersCount > 0, "Orders table should have data");
        Assert.IsTrue(newOrdersCount == 0, "Order table should be empty after truncating");
    }
}