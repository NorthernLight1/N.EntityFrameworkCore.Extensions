using System;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using N.EntityFrameworkCore.Extensions.Test.Data;

namespace N.EntityFrameworkCore.Extensions.Test.Common;

internal static class TestDatabaseInitializer
{
    internal static void EnsureCreated(TestDbContext dbContext)
    {
        dbContext.Database.EnsureCreated();
        CreateSqliteObjects(dbContext);
    }

    internal static async Task EnsureCreatedAsync(TestDbContext dbContext)
    {
        await dbContext.Database.EnsureCreatedAsync();
        await CreateSqliteObjectsAsync(dbContext);
    }

    internal static void CreateSqliteObjects(TestDbContext dbContext)
    {
        dbContext.Database.ExecuteSqlRaw("""
            CREATE TABLE IF NOT EXISTS "GuidKeyEntities" (
                "Id" TEXT NOT NULL CONSTRAINT "PK_GuidKeyEntities" PRIMARY KEY,
                "Name" TEXT NULL
            );
            """);
        dbContext.Database.ExecuteSqlRaw("""
            CREATE TABLE IF NOT EXISTS "Blogs" (
                "Id" INTEGER NOT NULL CONSTRAINT "PK_Blogs" PRIMARY KEY AUTOINCREMENT,
                "Title" TEXT NULL
            );
            """);
        dbContext.Database.ExecuteSqlRaw("""
            CREATE TABLE IF NOT EXISTS "BlogPosts" (
                "Id" INTEGER NOT NULL CONSTRAINT "PK_BlogPosts" PRIMARY KEY AUTOINCREMENT,
                "Title" TEXT NULL,
                "BlogId" INTEGER NOT NULL,
                CONSTRAINT "FK_BlogPosts_Blogs_BlogId" FOREIGN KEY ("BlogId") REFERENCES "Blogs" ("Id") ON DELETE CASCADE
            );
            """);
        try
        {
            dbContext.Database.ExecuteSqlRaw("ALTER TABLE \"Orders\" ADD COLUMN \"IsDeleted\" INTEGER NOT NULL DEFAULT 0");
        }
        catch (Exception)
        {
            // Column already exists on databases created before this schema change.
        }
        dbContext.Database.ExecuteSqlRaw("""
            CREATE TABLE IF NOT EXISTS "OrdersWithOwnedDetail" (
                "Id" INTEGER NOT NULL CONSTRAINT "PK_OrdersWithOwnedDetail" PRIMARY KEY AUTOINCREMENT,
                "Price" TEXT NOT NULL,
                "Detail_City" TEXT NULL,
                "Detail_Region" TEXT NULL
            );
            """);
    }

    internal static async Task CreateSqliteObjectsAsync(TestDbContext dbContext)
    {
        await dbContext.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "GuidKeyEntities" (
                "Id" TEXT NOT NULL CONSTRAINT "PK_GuidKeyEntities" PRIMARY KEY,
                "Name" TEXT NULL
            );
            """);
        await dbContext.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "Blogs" (
                "Id" INTEGER NOT NULL CONSTRAINT "PK_Blogs" PRIMARY KEY AUTOINCREMENT,
                "Title" TEXT NULL
            );
            """);
        await dbContext.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "BlogPosts" (
                "Id" INTEGER NOT NULL CONSTRAINT "PK_BlogPosts" PRIMARY KEY AUTOINCREMENT,
                "Title" TEXT NULL,
                "BlogId" INTEGER NOT NULL,
                CONSTRAINT "FK_BlogPosts_Blogs_BlogId" FOREIGN KEY ("BlogId") REFERENCES "Blogs" ("Id") ON DELETE CASCADE
            );
            """);
        try
        {
            dbContext.Database.ExecuteSqlRaw("ALTER TABLE \"Orders\" ADD COLUMN \"IsDeleted\" INTEGER NOT NULL DEFAULT 0");
        }
        catch (Exception)
        {
            // Column already exists on databases created before this schema change.
        }
        dbContext.Database.ExecuteSqlRaw("""
            CREATE TABLE IF NOT EXISTS "OrdersWithOwnedDetail" (
                "Id" INTEGER NOT NULL CONSTRAINT "PK_OrdersWithOwnedDetail" PRIMARY KEY AUTOINCREMENT,
                "Price" TEXT NOT NULL,
                "Detail_City" TEXT NULL,
                "Detail_Region" TEXT NULL
            );
            """);
    }
}
