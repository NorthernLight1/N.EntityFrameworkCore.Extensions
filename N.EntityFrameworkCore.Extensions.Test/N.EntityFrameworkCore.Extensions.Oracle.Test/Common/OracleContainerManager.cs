using System;
using System.Threading.Tasks;
using Testcontainers.Oracle;

namespace N.EntityFrameworkCore.Extensions.Test.Common;

internal static class OracleContainerManager
{
    private static readonly object syncRoot = new();
    private static Task initializationTask;
    private static OracleContainer container;
    private static bool cleanupRegistered;

    internal static string GetConnectionString()
    {
        EnsureStarted();
        var connStr = container.GetConnectionString();
        return AddPoolingParameters(connStr);
    }

    internal static string GetRestrictedConnectionString()
    {
        EnsureStarted();
        var connStr = container.GetConnectionString();
        return AddPoolingParameters(connStr);
    }

    private static string AddPoolingParameters(string connectionString)
    {
        if (connectionString.Contains("Min Pool Size"))
            return connectionString;
        
        return $"{connectionString};Min Pool Size=5;Max Pool Size=10;Connection Lifetime=300";
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
            container = new OracleBuilder("gvenzl/oracle-free:23-slim-faststart")
                .Build();

            await container.StartAsync();
            RegisterCleanup();
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException("Oracle tests require Docker when UseOracleContainer is enabled.", ex);
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
