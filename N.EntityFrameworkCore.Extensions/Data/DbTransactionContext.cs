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
        private DbContext context;
        private IDbContextTransaction dbContextTransaction;

        public SqlConnection Connection { get; internal set; }
        public SqlTransaction CurrentTransaction => dbContextTransaction.GetDbTransaction() as SqlTransaction;
        public DbContext DbContext => context;


        public DbTransactionContext(DbContext context, bool openConnection = true)
        {
            this.context = context;
            this.ownsTransaction = context.Database.CurrentTransaction == null;
            this.dbContextTransaction = context.Database.CurrentTransaction ?? context.Database.BeginTransaction();
            this.Connection = context.GetSqlConnection();

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
            if (closeConnection)
            {
                this.Connection.Close();
            }
        }

        internal void Commit()
        {
            if (this.ownsTransaction)
                dbContextTransaction.Commit();
        }
        internal void Rollback()
        {
            dbContextTransaction.Rollback();
        }
    }
}
