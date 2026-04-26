using System;
using System.Threading.Tasks;
using Testcontainers.PostgreSql;

namespace N.EntityFrameworkCore.Extensions.Test.Common;

internal static class PostgreSqlContainerManager
{
    private static readonly object syncRoot = new();
    private static Task initializationTask;
    private static PostgreSqlContainer container;
    private static bool cleanupRegistered;

    internal static string GetConnectionString()
    {
        EnsureStarted();
        return container.GetConnectionString();
    }

    internal static void EnsureStarted()
    {
        EnsureStartedAsync().GetAwaiter().GetResult();
    }

    internal static Task EnsureStartedAsync()
    {
        lock (syncRoot)
        {
            initializationTask ??= StartContainerAsync();
            return initializationTask;
        }
    }

    private static async Task StartContainerAsync()
    {
        try
        {
            container = new PostgreSqlBuilder("postgres:17-alpine")
                .WithDatabase("N.EntityFrameworkCore.Test")
                .WithUsername("postgres")
                .WithPassword("postgres")
                .Build();

            await container.StartAsync();
            RegisterCleanup();
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException("PostgreSQL tests require Docker when UsePostgreSqlContainer is enabled.", ex);
        }
    }

    private static void RegisterCleanup()
    {
        lock (syncRoot)
        {
            if (cleanupRegistered)
                return;

            AppDomain.CurrentDomain.ProcessExit += (_, _) =>
            {
                if (container != null)
                    container.DisposeAsync().AsTask().GetAwaiter().GetResult();
            };
            cleanupRegistered = true;
        }
    }
}
