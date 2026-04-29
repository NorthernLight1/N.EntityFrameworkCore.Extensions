using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using N.EntityFrameworkCore.Extensions.Test.Data;

namespace N.EntityFrameworkCore.Extensions.Test.Common;

internal static class TestDatabaseInitializer
{
    internal static void EnsureCreated(TestDbContext dbContext)
    {
        if (Config.UsePostgreSqlContainer)
            PostgreSqlContainerManager.EnsureStarted();

        if (Config.IsPostgreSql)
            dbContext.Database.ExecuteSqlRaw("CREATE EXTENSION IF NOT EXISTS pgcrypto");

        dbContext.Database.EnsureCreated();
        CreateProviderSpecificObjects(dbContext);
    }

    internal static async Task EnsureCreatedAsync(TestDbContext dbContext)
    {
        if (Config.UsePostgreSqlContainer)
            await PostgreSqlContainerManager.EnsureStartedAsync();

        if (Config.IsPostgreSql)
            await dbContext.Database.ExecuteSqlRawAsync("CREATE EXTENSION IF NOT EXISTS pgcrypto");

        await dbContext.Database.EnsureCreatedAsync();
        await CreateProviderSpecificObjectsAsync(dbContext);
    }

    internal static void CreateProviderSpecificObjects(TestDbContext dbContext)
    {
        if (Config.IsPostgreSql)
        {
            dbContext.Database.ExecuteSqlRaw("""
                CREATE OR REPLACE FUNCTION set_order_modified_datetime()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW."DbModifiedDateTime" = CURRENT_TIMESTAMP;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                """);
            dbContext.Database.ExecuteSqlRaw("""
                DROP TRIGGER IF EXISTS trg_order_modified_datetime ON "Orders";
                CREATE TRIGGER trg_order_modified_datetime
                BEFORE INSERT OR UPDATE ON "Orders"
                FOR EACH ROW
                EXECUTE FUNCTION set_order_modified_datetime();
                """);
            dbContext.Database.ExecuteSqlRaw("""
                CREATE OR REPLACE FUNCTION trg_product_with_triggers()
                RETURNS TRIGGER AS $$
                BEGIN
                    RETURN COALESCE(NEW, OLD);
                END;
                $$ LANGUAGE plpgsql;
                """);
            dbContext.Database.ExecuteSqlRaw("""
                DROP TRIGGER IF EXISTS trgProductWithTriggers ON "ProductsWithTrigger";
                CREATE TRIGGER trgProductWithTriggers
                BEFORE INSERT OR UPDATE OR DELETE ON "ProductsWithTrigger"
                FOR EACH ROW
                EXECUTE FUNCTION trg_product_with_triggers();
                """);
        }
        else
        {
            dbContext.Database.ExecuteSqlRaw("""
                IF OBJECT_ID('trgProductWithTriggers', 'TR') IS NOT NULL
                    DROP TRIGGER trgProductWithTriggers
                """);
            dbContext.Database.ExecuteSqlRaw("""
                CREATE TRIGGER trgProductWithTriggers
                ON ProductsWithTrigger
                FOR INSERT, UPDATE, DELETE
                AS
                BEGIN
                    PRINT 1
                END
                """);
        }
    }

    internal static async Task CreateProviderSpecificObjectsAsync(TestDbContext dbContext)
    {
        if (Config.IsPostgreSql)
        {
            await dbContext.Database.ExecuteSqlRawAsync("""
                CREATE OR REPLACE FUNCTION set_order_modified_datetime()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW."DbModifiedDateTime" = CURRENT_TIMESTAMP;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                """);
            await dbContext.Database.ExecuteSqlRawAsync("""
                DROP TRIGGER IF EXISTS trg_order_modified_datetime ON "Orders";
                CREATE TRIGGER trg_order_modified_datetime
                BEFORE INSERT OR UPDATE ON "Orders"
                FOR EACH ROW
                EXECUTE FUNCTION set_order_modified_datetime();
                """);
            await dbContext.Database.ExecuteSqlRawAsync("""
                CREATE OR REPLACE FUNCTION trg_product_with_triggers()
                RETURNS TRIGGER AS $$
                BEGIN
                    RETURN COALESCE(NEW, OLD);
                END;
                $$ LANGUAGE plpgsql;
                """);
            await dbContext.Database.ExecuteSqlRawAsync("""
                DROP TRIGGER IF EXISTS trgProductWithTriggers ON "ProductsWithTrigger";
                CREATE TRIGGER trgProductWithTriggers
                BEFORE INSERT OR UPDATE OR DELETE ON "ProductsWithTrigger"
                FOR EACH ROW
                EXECUTE FUNCTION trg_product_with_triggers();
                """);
        }
        else
        {
            await dbContext.Database.ExecuteSqlRawAsync("""
                IF OBJECT_ID('trgProductWithTriggers', 'TR') IS NOT NULL
                    DROP TRIGGER trgProductWithTriggers
                """);
            await dbContext.Database.ExecuteSqlRawAsync("""
                CREATE TRIGGER trgProductWithTriggers
                ON ProductsWithTrigger
                FOR INSERT, UPDATE, DELETE
                AS
                BEGIN
                    PRINT 1
                END
                """);
        }
    }
}
