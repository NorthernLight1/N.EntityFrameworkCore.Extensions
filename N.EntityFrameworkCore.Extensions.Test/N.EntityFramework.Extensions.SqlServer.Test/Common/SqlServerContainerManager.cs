using System;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Testcontainers.MsSql;

namespace N.EntityFrameworkCore.Extensions.Test.Common;

internal static class SqlServerContainerManager
{
    private static readonly object syncRoot = new();
    private static Task initializationTask;
    private static MsSqlContainer container;
    private static bool cleanupRegistered;

    internal static string GetConnectionString()
    {
        EnsureStarted();
        var builder = new SqlConnectionStringBuilder(container.GetConnectionString())
        {
            InitialCatalog = "NEntityFrameworkCoreExtensions"
        };
        return builder.ConnectionString;
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
            container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
                .Build();

            await container.StartAsync();
            RegisterCleanup();
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException("SqlServer tests require Docker when UseSqlServerContainer is enabled.", ex);
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
