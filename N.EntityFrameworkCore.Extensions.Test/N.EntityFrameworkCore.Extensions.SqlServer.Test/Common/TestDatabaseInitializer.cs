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
        CreateRestrictedUser(dbContext);
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
        await CreateRestrictedUserAsync(dbContext);
    }

    internal static void CreateRestrictedUser(TestDbContext dbContext)
    {
        dbContext.Database.ExecuteSqlRaw("""
            IF NOT EXISTS (SELECT * FROM sys.server_principals WHERE name = 'limited_test_user')
                CREATE LOGIN limited_test_user WITH PASSWORD = 'LimitedTest123!';
            IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'limited_test_user')
            BEGIN
                CREATE USER limited_test_user FOR LOGIN limited_test_user;
                GRANT SELECT, INSERT, UPDATE, DELETE ON dbo.Orders TO limited_test_user;
            END
            """);
    }

    internal static async Task CreateRestrictedUserAsync(TestDbContext dbContext)
    {
        await dbContext.Database.ExecuteSqlRawAsync("""
            IF NOT EXISTS (SELECT * FROM sys.server_principals WHERE name = 'limited_test_user')
                CREATE LOGIN limited_test_user WITH PASSWORD = 'LimitedTest123!';
            IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'limited_test_user')
            BEGIN
                CREATE USER limited_test_user FOR LOGIN limited_test_user;
                GRANT SELECT, INSERT, UPDATE, DELETE ON dbo.Orders TO limited_test_user;
            END
            """);
    }
}
