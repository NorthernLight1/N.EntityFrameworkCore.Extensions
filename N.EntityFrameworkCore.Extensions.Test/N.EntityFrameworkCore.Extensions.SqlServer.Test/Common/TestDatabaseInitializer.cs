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
            IF OBJECT_ID(N'dbo.GuidKeyEntities', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.GuidKeyEntities (
                    Id uniqueidentifier NOT NULL PRIMARY KEY,
                    Name nvarchar(100) NULL
                );
            END
            """);
        dbContext.Database.ExecuteSqlRaw("""
            IF OBJECT_ID(N'dbo.Blogs', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.Blogs (
                    Id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    Title nvarchar(200) NULL
                );
            END
            """);
        dbContext.Database.ExecuteSqlRaw("""
            IF OBJECT_ID(N'dbo.BlogPosts', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.BlogPosts (
                    Id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    Title nvarchar(200) NULL,
                    BlogId int NOT NULL,
                    CONSTRAINT FK_BlogPosts_Blogs_BlogId FOREIGN KEY (BlogId) REFERENCES dbo.Blogs (Id) ON DELETE CASCADE
                );
            END
            """);
        dbContext.Database.ExecuteSqlRaw("""
            IF COL_LENGTH('dbo.Orders', 'IsDeleted') IS NULL
                ALTER TABLE dbo.Orders ADD IsDeleted bit NOT NULL CONSTRAINT DF_Orders_IsDeleted DEFAULT 0;
            """);
        dbContext.Database.ExecuteSqlRaw("""
            IF OBJECT_ID(N'dbo.OrdersWithOwnedDetail', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.OrdersWithOwnedDetail (
                    Id bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    Price decimal(18,2) NOT NULL,
                    Detail_City nvarchar(max) NULL,
                    Detail_Region nvarchar(max) NULL
                );
            END
            """);
        dbContext.Database.ExecuteSqlRaw("""
            IF OBJECT_ID(N'dbo.LocationsWithSpatial', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.LocationsWithSpatial (
                    Id int NOT NULL PRIMARY KEY,
                    Name nvarchar(max) NULL,
                    Location geography NULL
                );
            END
            """);
        dbContext.Database.ExecuteSqlRaw("""
            IF OBJECT_ID(N'dbo.OrgNodes', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.OrgNodes (
                    Id int NOT NULL PRIMARY KEY,
                    Name nvarchar(max) NULL,
                    NodePath hierarchyid NULL
                );
            END
            """);
        dbContext.Database.ExecuteSqlRaw("""
            IF OBJECT_ID(N'dbo.TemporalOrders', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.TemporalOrders (
                    Id int NOT NULL PRIMARY KEY,
                    Price decimal(18,2) NOT NULL,
                    PeriodStart datetime2 GENERATED ALWAYS AS ROW START HIDDEN NOT NULL,
                    PeriodEnd datetime2 GENERATED ALWAYS AS ROW END HIDDEN NOT NULL,
                    PERIOD FOR SYSTEM_TIME (PeriodStart, PeriodEnd)
                ) WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.TemporalOrdersHistory));
            END
            """);
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
            IF OBJECT_ID(N'dbo.GuidKeyEntities', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.GuidKeyEntities (
                    Id uniqueidentifier NOT NULL PRIMARY KEY,
                    Name nvarchar(100) NULL
                );
            END
            """);
        await dbContext.Database.ExecuteSqlRawAsync("""
            IF OBJECT_ID(N'dbo.Blogs', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.Blogs (
                    Id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    Title nvarchar(200) NULL
                );
            END
            """);
        await dbContext.Database.ExecuteSqlRawAsync("""
            IF OBJECT_ID(N'dbo.BlogPosts', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.BlogPosts (
                    Id int IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    Title nvarchar(200) NULL,
                    BlogId int NOT NULL,
                    CONSTRAINT FK_BlogPosts_Blogs_BlogId FOREIGN KEY (BlogId) REFERENCES dbo.Blogs (Id) ON DELETE CASCADE
                );
            END
            """);
        await dbContext.Database.ExecuteSqlRawAsync("""
            IF COL_LENGTH('dbo.Orders', 'IsDeleted') IS NULL
                ALTER TABLE dbo.Orders ADD IsDeleted bit NOT NULL CONSTRAINT DF_Orders_IsDeleted DEFAULT 0;
            """);
        await dbContext.Database.ExecuteSqlRawAsync("""
            IF OBJECT_ID(N'dbo.OrdersWithOwnedDetail', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.OrdersWithOwnedDetail (
                    Id bigint IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    Price decimal(18,2) NOT NULL,
                    Detail_City nvarchar(max) NULL,
                    Detail_Region nvarchar(max) NULL
                );
            END
            """);
        await dbContext.Database.ExecuteSqlRawAsync("""
            IF OBJECT_ID(N'dbo.LocationsWithSpatial', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.LocationsWithSpatial (
                    Id int NOT NULL PRIMARY KEY,
                    Name nvarchar(max) NULL,
                    Location geography NULL
                );
            END
            """);
        await dbContext.Database.ExecuteSqlRawAsync("""
            IF OBJECT_ID(N'dbo.OrgNodes', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.OrgNodes (
                    Id int NOT NULL PRIMARY KEY,
                    Name nvarchar(max) NULL,
                    NodePath hierarchyid NULL
                );
            END
            """);
        await dbContext.Database.ExecuteSqlRawAsync("""
            IF OBJECT_ID(N'dbo.TemporalOrders', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.TemporalOrders (
                    Id int NOT NULL PRIMARY KEY,
                    Price decimal(18,2) NOT NULL,
                    PeriodStart datetime2 GENERATED ALWAYS AS ROW START HIDDEN NOT NULL,
                    PeriodEnd datetime2 GENERATED ALWAYS AS ROW END HIDDEN NOT NULL,
                    PERIOD FOR SYSTEM_TIME (PeriodStart, PeriodEnd)
                ) WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.TemporalOrdersHistory));
            END
            """);
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