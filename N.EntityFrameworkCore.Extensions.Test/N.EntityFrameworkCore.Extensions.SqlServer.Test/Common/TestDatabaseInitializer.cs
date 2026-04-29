using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using N.EntityFrameworkCore.Extensions.Test.Data;

namespace N.EntityFrameworkCore.Extensions.Test.Common;

internal static class TestDatabaseInitializer
{
    internal static void EnsureCreated(TestDbContext dbContext)
    {
        if (Config.UseSqlServerContainer)
            SqlServerContainerManager.EnsureStarted();

        dbContext.Database.EnsureCreated();
        CreateSqlServerObjects(dbContext);
    }

    internal static async Task EnsureCreatedAsync(TestDbContext dbContext)
    {
        if (Config.UseSqlServerContainer)
            await SqlServerContainerManager.EnsureStartedAsync();

        await dbContext.Database.EnsureCreatedAsync();
        await CreateSqlServerObjectsAsync(dbContext);
    }

    internal static void CreateSqlServerObjects(TestDbContext dbContext)
    {
        dbContext.Database.ExecuteSqlRaw("""
            CREATE OR ALTER TRIGGER trgProductWithTriggers
            ON ProductsWithTrigger
            FOR INSERT, UPDATE, DELETE
            AS
            BEGIN
                PRINT 1
            END
            """);
    }

    internal static async Task CreateSqlServerObjectsAsync(TestDbContext dbContext)
    {
        await dbContext.Database.ExecuteSqlRawAsync("""
            CREATE OR ALTER TRIGGER trgProductWithTriggers
            ON ProductsWithTrigger
            FOR INSERT, UPDATE, DELETE
            AS
            BEGIN
                PRINT 1
            END
            """);
    }
}
