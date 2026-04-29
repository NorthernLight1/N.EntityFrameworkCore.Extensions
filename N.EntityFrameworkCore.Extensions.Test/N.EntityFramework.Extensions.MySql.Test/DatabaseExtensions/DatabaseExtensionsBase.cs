using System;
using System.Collections.Generic;
using System.Diagnostics;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Common;
using N.EntityFrameworkCore.Extensions.Test.Data;

namespace N.EntityFrameworkCore.Extensions.Test.DatabaseExtensions;

public class DatabaseExtensionsBase
{
    private TestDbContext _currentDbContext;

    [TestCleanup]
    public void Cleanup()
    {
        _currentDbContext?.Dispose();
        _currentDbContext = null;
    }

    protected TestDbContext SetupDbContext(bool populateData)
    {
        var dbContext = new TestDbContext();
        _currentDbContext = dbContext;
        TestDatabaseInitializer.EnsureCreated(dbContext);
        dbContext.Orders.Truncate();
        if (populateData)
        {
            var orders = new List<Order>();
            int id = 1;
            for (int i = 0; i < 2050; i++)
            {
                DateTime addedDateTime = DateTime.UtcNow.AddDays(-id);
                orders.Add(new Order
                {
                    Id = id,
                    ExternalId = string.Format("id-{0}", i),
                    Price = 1.25M,
                    AddedDateTime = addedDateTime,
                    ModifiedDateTime = addedDateTime.AddHours(3)
                });
                id++;
            }
            for (int i = 0; i < 1050; i++)
            {
                orders.Add(new Order { Id = id, Price = 5.35M });
                id++;
            }
            for (int i = 0; i < 2050; i++)
            {
                orders.Add(new Order { Id = id, Price = 1.25M });
                id++;
            }
            for (int i = 0; i < 6000; i++)
            {
                orders.Add(new Order { Id = id, Price = 15.35M });
                id++;
            }
            for (int i = 0; i < 6000; i++)
            {
                orders.Add(new Order { Id = id, Price = 15.35M });
                id++;
            }

            Debug.WriteLine("Last Id for Order is {0}", id);
            dbContext.BulkInsert(orders, new BulkInsertOptions<Order>() { KeepIdentity = true });
        }
        return dbContext;
    }
}
