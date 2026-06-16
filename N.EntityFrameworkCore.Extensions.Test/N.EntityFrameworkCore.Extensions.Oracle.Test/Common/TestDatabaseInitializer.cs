using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using N.EntityFrameworkCore.Extensions.Test.Data;

namespace N.EntityFrameworkCore.Extensions.Test.Common;

internal static class TestDatabaseInitializer
{
    internal static void EnsureCreated(TestDbContext dbContext)
    {
        if (Config.UseOracleContainer)
            OracleContainerManager.EnsureStarted();

        dbContext.Database.EnsureCreated();
        CreateOracleObjects(dbContext);
    }

    internal static async Task EnsureCreatedAsync(TestDbContext dbContext)
    {
        if (Config.UseOracleContainer)
            await OracleContainerManager.EnsureStartedAsync();

        await dbContext.Database.EnsureCreatedAsync();
        await CreateOracleObjectsAsync(dbContext);
    }

    internal static void CreateOracleObjects(TestDbContext dbContext)
    {
    }

    internal static async Task CreateOracleObjectsAsync(TestDbContext dbContext)
    {
    }

    internal static void CreateRestrictedUser(TestDbContext dbContext)
    {
    }

    internal static async Task CreateRestrictedUserAsync(TestDbContext dbContext)
    {
    }
}
