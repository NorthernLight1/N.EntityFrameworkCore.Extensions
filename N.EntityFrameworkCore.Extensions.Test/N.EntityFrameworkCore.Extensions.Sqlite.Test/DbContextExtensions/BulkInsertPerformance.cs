using System;
using System.Collections.Generic;
using System.Diagnostics;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Common;
using N.EntityFrameworkCore.Extensions.Test.Data;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions;

[TestClass]
public class BulkInsertPerformance : DbContextExtensionsBase
{
    private const int RecordCount = 10_000;

    private static List<Order> CreateOrders()
    {
        var orders = new List<Order>(RecordCount);
        for (int i = 1; i <= RecordCount; i++)
        {
            orders.Add(new Order
            {
                Id = i,
                ExternalId = $"perf-{i}",
                Price = 9.99M,
                AddedDateTime = DateTime.UtcNow
            });
        }
        return orders;
    }

    [TestMethod]
    public void BulkInsert_Is_Faster_Than_EfCore_AddRange()
    {
        // --- EF Core AddRange + SaveChanges ---
        var dbContext1 = SetupDbContext(false);
        var orders1 = CreateOrders();
        var sw1 = Stopwatch.StartNew();
        dbContext1.Orders.AddRange(orders1);
        dbContext1.SaveChanges();
        sw1.Stop();
        long efMs = sw1.ElapsedMilliseconds;

        // --- BulkInsert ---
        var dbContext2 = SetupDbContext(false);
        var orders2 = CreateOrders();
        var sw2 = Stopwatch.StartNew();
        dbContext2.BulkInsert(orders2, new BulkInsertOptions<Order> { KeepIdentity = true });
        sw2.Stop();
        long bulkMs = sw2.ElapsedMilliseconds;

        Console.WriteLine($"Records inserted: {RecordCount:N0}");
        Console.WriteLine($"EF Core AddRange + SaveChanges: {efMs} ms");
        Console.WriteLine($"BulkInsert:                     {bulkMs} ms");
        Console.WriteLine($"BulkInsert is {(double)efMs / bulkMs:F1}x faster");

        Assert.IsTrue(bulkMs < efMs,
            $"BulkInsert ({bulkMs} ms) should be faster than EF Core AddRange ({efMs} ms) for {RecordCount:N0} records.");
    }
}
