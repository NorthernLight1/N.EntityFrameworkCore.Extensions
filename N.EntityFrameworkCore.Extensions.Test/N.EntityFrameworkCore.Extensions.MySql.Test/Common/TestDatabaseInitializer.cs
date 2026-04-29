using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using N.EntityFrameworkCore.Extensions.Test.Data;

namespace N.EntityFrameworkCore.Extensions.Test.Common;

internal static class TestDatabaseInitializer
{
    internal static void EnsureCreated(TestDbContext dbContext)
    {
        if (Config.UseMySqlContainer)
            MySqlContainerManager.EnsureStarted();

        dbContext.Database.EnsureCreated();
        CreateProviderSpecificObjects(dbContext);
    }

    internal static async Task EnsureCreatedAsync(TestDbContext dbContext)
    {
        if (Config.UseMySqlContainer)
            await MySqlContainerManager.EnsureStartedAsync();

        await dbContext.Database.EnsureCreatedAsync();
        await CreateProviderSpecificObjectsAsync(dbContext);
    }

    internal static void CreateProviderSpecificObjects(TestDbContext dbContext)
    {
        dbContext.Database.ExecuteSqlRaw("DROP TRIGGER IF EXISTS trg_order_modified_datetime_before_insert");
        dbContext.Database.ExecuteSqlRaw("""
            CREATE TRIGGER trg_order_modified_datetime_before_insert
            BEFORE INSERT ON `Orders`
            FOR EACH ROW
            SET NEW.`DbModifiedDateTime` = NOW(6)
            """);
        dbContext.Database.ExecuteSqlRaw("DROP TRIGGER IF EXISTS trg_order_modified_datetime_before_update");
        dbContext.Database.ExecuteSqlRaw("""
            CREATE TRIGGER trg_order_modified_datetime_before_update
            BEFORE UPDATE ON `Orders`
            FOR EACH ROW
            SET NEW.`DbModifiedDateTime` = NOW(6)
            """);
        dbContext.Database.ExecuteSqlRaw("DROP TRIGGER IF EXISTS trgProductWithTriggers");
        dbContext.Database.ExecuteSqlRaw("""
            CREATE TRIGGER trgProductWithTriggers
            BEFORE INSERT ON `ProductsWithTrigger`
            FOR EACH ROW
            SET NEW.`Id` = NEW.`Id`
            """);
    }

    internal static async Task CreateProviderSpecificObjectsAsync(TestDbContext dbContext)
    {
        await dbContext.Database.ExecuteSqlRawAsync("DROP TRIGGER IF EXISTS trg_order_modified_datetime_before_insert");
        await dbContext.Database.ExecuteSqlRawAsync("""
            CREATE TRIGGER trg_order_modified_datetime_before_insert
            BEFORE INSERT ON `Orders`
            FOR EACH ROW
            SET NEW.`DbModifiedDateTime` = NOW(6)
            """);
        await dbContext.Database.ExecuteSqlRawAsync("DROP TRIGGER IF EXISTS trg_order_modified_datetime_before_update");
        await dbContext.Database.ExecuteSqlRawAsync("""
            CREATE TRIGGER trg_order_modified_datetime_before_update
            BEFORE UPDATE ON `Orders`
            FOR EACH ROW
            SET NEW.`DbModifiedDateTime` = NOW(6)
            """);
        await dbContext.Database.ExecuteSqlRawAsync("DROP TRIGGER IF EXISTS trgProductWithTriggers");
        await dbContext.Database.ExecuteSqlRawAsync("""
            CREATE TRIGGER trgProductWithTriggers
            BEFORE INSERT ON `ProductsWithTrigger`
            FOR EACH ROW
            SET NEW.`Id` = NEW.`Id`
            """);
    }
}
