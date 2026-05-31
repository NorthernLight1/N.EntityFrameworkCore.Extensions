using System.Threading.Tasks;
using N.EntityFrameworkCore.Extensions.Test.Data;

namespace N.EntityFrameworkCore.Extensions.Test.Common;

internal static class TestDatabaseInitializer
{
    internal static void EnsureCreated(TestDbContext dbContext)
    {
        dbContext.Database.EnsureCreated();
    }

    internal static async Task EnsureCreatedAsync(TestDbContext dbContext)
    {
        await dbContext.Database.EnsureCreatedAsync();
    }
}

