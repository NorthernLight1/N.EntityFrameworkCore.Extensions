using System;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using N.EntityFrameworkCore.Extensions.Enums;


namespace N.EntityFrameworkCore.Extensions
{
    internal class DbTransactionContext : IDisposable
    {
        private bool closeConnection;
        private bool ownsTransaction;
        private int? defaultCommandTimeout;
        private DbContext context;
        private IDbContextTransaction transaction;

        public SqlConnection Connection { get; internal set; }
        public SqlTransaction CurrentTransaction { get; private set; }
        public DbContext DbContext => context;

        public DbTransactionContext(DbContext context, BulkOptions bulkOptions, bool openConnection = true) : this(context, bulkOptions.CommandTimeout, bulkOptions.ConnectionBehavior, openConnection)
        {

        }
        public DbTransactionContext(DbContext context, int? commandTimeout = null, ConnectionBehavior connectionBehavior = ConnectionBehavior.Default, bool openConnection = true)
        {
            this.context = context;
            this.Connection = context.GetSqlConnection(connectionBehavior);
            if (openConnection)
            {
                if (this.Connection.State == System.Data.ConnectionState.Closed)
                {
                    this.Connection.Open();
                    this.closeConnection = true;
                }
            }
            if (connectionBehavior == ConnectionBehavior.Default)
            {
                this.ownsTransaction = context.Database.CurrentTransaction == null;
                this.transaction = context.Database.CurrentTransaction; //?? context.Database.BeginTransaction();
                this.defaultCommandTimeout = context.Database.GetCommandTimeout();
                if (this.transaction != null)
                    this.CurrentTransaction = transaction.GetDbTransaction() as SqlTransaction;
            }
            else
            {
                //this.CurrentTransaction = this.Connection.BeginTransaction();
            }

            context.Database.SetCommandTimeout(commandTimeout);
        }

        public void Dispose()
        {
            context.Database.SetCommandTimeout(defaultCommandTimeout);
            if (closeConnection)
            {
                this.Connection.Close();
            }
        }

        internal void Commit()
        {
            if (this.ownsTransaction && this.transaction != null)
                transaction.Commit();
        }
        internal void Rollback()
        {
            if (this.transaction != null)
                transaction.Rollback();
        }
    }
}