using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using System;


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
        public SqlTransaction CurrentTransaction => transaction.GetDbTransaction() as SqlTransaction;
        public DbContext DbContext => context;

        public DbTransactionContext(DbContext context, BulkOptions bulkOptions, bool openConnection = true) : this(context, bulkOptions.CommandTimeout, openConnection)
        {

        }
        public DbTransactionContext(DbContext context, int? commandTimeout = null, bool openConnection = true)
        {
            this.context = context;
            this.ownsTransaction = context.Database.CurrentTransaction == null;
            this.transaction = context.Database.CurrentTransaction ?? context.Database.BeginTransaction();
            this.Connection = context.GetSqlConnection();
            this.defaultCommandTimeout = context.Database.GetCommandTimeout();
            context.Database.SetCommandTimeout(commandTimeout);

            if (openConnection)
            {
                if (this.Connection.State == System.Data.ConnectionState.Closed)
                {
                    this.Connection.Open();
                    this.closeConnection = true;
                }
            }
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
            transaction.Rollback();
        }
    }
}
