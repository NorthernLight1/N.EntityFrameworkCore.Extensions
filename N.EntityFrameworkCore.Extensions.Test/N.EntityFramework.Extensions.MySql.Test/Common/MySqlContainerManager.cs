using System;
using System.Threading.Tasks;
using Testcontainers.MySql;

namespace N.EntityFrameworkCore.Extensions.Test.Common;

internal static class MySqlContainerManager
{
    private static readonly object syncRoot = new();
    private static Task initializationTask;
    private static MySqlContainer container;
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
            container = new MySqlBuilder("mysql:8.0")
                .WithDatabase("NEntityFrameworkCoreExtensions")
                .WithUsername("root")
                .WithPassword("mysql")
                .Build();

            await container.StartAsync();
            RegisterCleanup();
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException("MySql tests require Docker when UseMySqlContainer is enabled.", ex);
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
