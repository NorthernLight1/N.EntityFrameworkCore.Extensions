using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Data;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions;

[TestClass]
public class BulkInsertAsyncEnumerable : DbContextExtensionsBase
{
    [TestMethod]
    public async Task With_Default_Batching()
    {
        var dbContext = SetupDbContext(true);
        int rows = await dbContext.BulkInsertAsync(CreateOrders(100), o => { o.AutoMapOutput = false; o.BatchSize = 25; });
        int inserted = dbContext.Orders.Count(o => o.ExternalId != null && o.ExternalId.StartsWith("stream-"));
        Assert.AreEqual(100, rows);
        Assert.AreEqual(100, inserted);
    }

    private static async IAsyncEnumerable<Order> CreateOrders(int count)
    {
        for (int i = 0; i < count; i++)
            yield return new Order { ExternalId = $"stream-{i}", Price = 1.5M };
        await Task.CompletedTask;
    }

    [TestMethod]
    public async Task With_Empty_Stream()
    {
        var dbContext = SetupDbContext(true);
        int rows = await dbContext.BulkInsertAsync(EmptyOrders());
        Assert.AreEqual(0, rows);
        Assert.AreEqual(0, dbContext.Orders.Count(o => o.ExternalId != null && o.ExternalId.StartsWith("stream-empty-")));
    }

    [TestMethod]
    public async Task With_Partial_Final_Batch()
    {
        var dbContext = SetupDbContext(true);
        int rows = await dbContext.BulkInsertAsync(CreateOrdersWithPrefix(103, "stream-partial-"), o => { o.AutoMapOutput = false; o.BatchSize = 25; });
        int inserted = dbContext.Orders.Count(o => o.ExternalId != null && o.ExternalId.StartsWith("stream-partial-"));
        Assert.AreEqual(103, rows);
        Assert.AreEqual(103, inserted);
    }

    [TestMethod]
    public async Task With_KeepIdentity()
    {
        var dbContext = SetupDbContext(false);
        long startId = 9_000_000;
        int rows = await dbContext.BulkInsertAsync(CreateOrdersWithIds(startId, 50), o => { o.KeepIdentity = true; o.BatchSize = 25; });
        Assert.AreEqual(50, rows);
        Assert.AreEqual(50, dbContext.Orders.Count(o => o.Id >= startId && o.Id < startId + 50));
    }

    [TestMethod]
    public async Task With_Options_InsertIfNotExists()
    {
        var dbContext = SetupDbContext(true);
        long maxId = dbContext.Orders.Max(o => o.Id);
        int rows = await dbContext.BulkInsertAsync(CreateInsertIfNotExistsOrders(maxId), o => o.InsertIfNotExists = true);
        Assert.AreEqual(10, rows);
    }

    private static async IAsyncEnumerable<Order> CreateOrdersWithPrefix(int count, string prefix)
    {
        for (int i = 0; i < count; i++)
            yield return new Order { ExternalId = $"{prefix}{i}", Price = 1.5M };
        await Task.CompletedTask;
    }

    private static async IAsyncEnumerable<Order> CreateOrdersWithIds(long startId, int count)
    {
        for (int i = 0; i < count; i++)
            yield return new Order { Id = startId + i, Price = 1.57M };
        await Task.CompletedTask;
    }

    private static async IAsyncEnumerable<Order> CreateInsertIfNotExistsOrders(long maxId)
    {
        for (long i = maxId - 4; i <= maxId + 10; i++)
            yield return new Order { Id = i, Price = 7.77M, ExternalId = $"stream-insertif-{i}" };
        await Task.CompletedTask;
    }

    private static async IAsyncEnumerable<Order> EmptyOrders()
    {
        await Task.CompletedTask;
        yield break;
    }
}