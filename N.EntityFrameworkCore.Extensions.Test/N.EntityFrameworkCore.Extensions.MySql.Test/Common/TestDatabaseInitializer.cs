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
        dbContext.Database.ExecuteSqlRaw("""
            CREATE TABLE IF NOT EXISTS `GuidKeyEntities` (
                `Id` char(36) NOT NULL PRIMARY KEY,
                `Name` varchar(100) NULL
            );
            """);
        dbContext.Database.ExecuteSqlRaw("""
            CREATE TABLE IF NOT EXISTS `Blogs` (
                `Id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
                `Title` varchar(200) NULL
            );
            """);
        dbContext.Database.ExecuteSqlRaw("""
            CREATE TABLE IF NOT EXISTS `BlogPosts` (
                `Id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
                `Title` varchar(200) NULL,
                `BlogId` int NOT NULL,
                CONSTRAINT `FK_BlogPosts_Blogs_BlogId` FOREIGN KEY (`BlogId`) REFERENCES `Blogs` (`Id`) ON DELETE CASCADE
            );
            """);
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
        dbContext.Database.ExecuteSqlRaw("""
            SET @exists := (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'Orders' AND column_name = 'IsDeleted');
            SET @sql := IF(@exists = 0, 'ALTER TABLE `Orders` ADD `IsDeleted` tinyint(1) NOT NULL DEFAULT 0', 'SELECT 1');
            PREPARE stmt FROM @sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
            """);
        dbContext.Database.ExecuteSqlRaw("""
            CREATE TABLE IF NOT EXISTS `OrdersWithOwnedDetail` (
                `Id` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
                `Price` decimal(65,30) NOT NULL,
                `Detail_City` longtext NULL,
                `Detail_Region` longtext NULL
            );
            """);
    }

    internal static async Task CreateProviderSpecificObjectsAsync(TestDbContext dbContext)
    {
        await dbContext.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS `GuidKeyEntities` (
                `Id` char(36) NOT NULL PRIMARY KEY,
                `Name` varchar(100) NULL
            );
            """);
        await dbContext.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS `Blogs` (
                `Id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
                `Title` varchar(200) NULL
            );
            """);
        await dbContext.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS `BlogPosts` (
                `Id` int NOT NULL AUTO_INCREMENT PRIMARY KEY,
                `Title` varchar(200) NULL,
                `BlogId` int NOT NULL,
                CONSTRAINT `FK_BlogPosts_Blogs_BlogId` FOREIGN KEY (`BlogId`) REFERENCES `Blogs` (`Id`) ON DELETE CASCADE
            );
            """);
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
        await dbContext.Database.ExecuteSqlRawAsync("""
            SET @exists := (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'Orders' AND column_name = 'IsDeleted');
            SET @sql := IF(@exists = 0, 'ALTER TABLE `Orders` ADD `IsDeleted` tinyint(1) NOT NULL DEFAULT 0', 'SELECT 1');
            PREPARE stmt FROM @sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
            """);
        await dbContext.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS `OrdersWithOwnedDetail` (
                `Id` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
                `Price` decimal(65,30) NOT NULL,
                `Detail_City` longtext NULL,
                `Detail_Region` longtext NULL
            );
            """);
    }
}