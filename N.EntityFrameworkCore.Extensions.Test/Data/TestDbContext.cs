using Microsoft.EntityFrameworkCore;
using N.EntityFrameworkCore.Extensions.Test.Common;
using System;

namespace N.EntityFrameworkCore.Extensions.Test.Data
{
    public class TestDbContext : DbContext
    {
        public virtual DbSet<Product> Products { get; set; }
        public virtual DbSet<Order> Orders { get; set;  }
        public virtual DbSet<TphPerson> TphPeople { get; set; }
        public virtual DbSet<TphCustomer> TphCustomers { get; set; }
        public virtual DbSet<TphVendor> TphVendors { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSqlServer(Config.GetConnectionString("TestDatabase"));
            optionsBuilder.SetupEfCoreExtensions();
        }
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<TphPerson>().Property<DateTime>("CreatedDate");
        }
    }
}
