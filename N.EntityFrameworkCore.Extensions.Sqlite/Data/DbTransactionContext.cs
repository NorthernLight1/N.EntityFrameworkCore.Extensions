using System;
using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using N.EntityFrameworkCore.Extensions.Enums;
using N.EntityFrameworkCore.Extensions.Util;


namespace N.EntityFrameworkCore.Extensions;

internal sealed class DbTransactionContext : IDisposable
{
    private bool closeConnection;
    private bool ownsTransaction;
    private bool transactionCompleted;
    private int? defaultCommandTimeout;
    private DbContext context;
    private IDbContextTransaction transaction;

    public DbConnection Connection { get; internal set; }
    public DbTransaction CurrentTransaction { get; private set; }
    public DbContext DbContext => context;
    public bool OwnsTransaction => ownsTransaction;

    public DbTransactionContext(DbContext context, BulkOptions bulkOptions, bool openConnection = true) : this(context, bulkOptions.CommandTimeout, bulkOptions.ConnectionBehavior, openConnection)
    {

    }
    public DbTransactionContext(DbContext context, int? commandTimeout = null, ConnectionBehavior connectionBehavior = ConnectionBehavior.Default, bool openConnection = true)
    {
        this.context = context;
        Connection = context.GetDbConnection(connectionBehavior);
        if (openConnection)
        {
            if (Connection.State == System.Data.ConnectionState.Closed)
            {
                Connection.Open();
                closeConnection = true;
            }
        }
        defaultCommandTimeout = context.Database.GetCommandTimeout();
        if (connectionBehavior == ConnectionBehavior.Default)
        {
            ownsTransaction = context.Database.CurrentTransaction == null;
            transaction = context.Database.CurrentTransaction;
            if (transaction != null)
                CurrentTransaction = transaction.GetDbTransaction();
            else if (ownsTransaction)
            {
                transaction = context.Database.BeginTransaction();
                CurrentTransaction = transaction.GetDbTransaction();
            }
        }

        context.Database.SetCommandTimeout(commandTimeout);
    }

    public void Dispose()
    {
        context.Database.SetCommandTimeout(defaultCommandTimeout);
        if (ownsTransaction && transaction != null && !transactionCompleted)
        {
            transaction.Rollback();
            transaction.Dispose();
            transaction = null;
            CurrentTransaction = null;
        }
        if (closeConnection)
        {
            Connection.Close();
        }
    }

    internal void Commit()
    {
        if (ownsTransaction && transaction != null && !transactionCompleted)
        {
            transaction.Commit();
            transactionCompleted = true;
            transaction.Dispose();
            transaction = null;
            CurrentTransaction = null;
        }
    }
    internal void Rollback()
    {
        if (ownsTransaction && transaction != null && !transactionCompleted)
        {
            transaction.Rollback();
            transactionCompleted = true;
            transaction.Dispose();
            transaction = null;
            CurrentTransaction = null;
        }
    }
}
