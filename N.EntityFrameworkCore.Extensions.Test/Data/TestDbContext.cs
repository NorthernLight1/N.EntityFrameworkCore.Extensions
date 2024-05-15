using Microsoft.EntityFrameworkCore;
using N.EntityFrameworkCore.Extensions.Test.Common;
using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Drawing;
using System.Reflection.Metadata;
using System.Runtime.ConstrainedExecution;

namespace N.EntityFrameworkCore.Extensions.Test.Data
{
    public class TestDbContext : DbContext
    {
        public virtual DbSet<Product> Products { get; set; }
        public virtual DbSet<ProductCategory> ProductCategories { get; set; }
        public virtual DbSet<ProductWithCustomSchema> ProductsWithCustomSchema { get; set; }
        public virtual DbSet<ProductWithComplexKey> ProductsWithComplexKey { get; set; }
        public virtual DbSet<Order> Orders { get; set;  }
        public virtual DbSet<TpcPerson> TpcPeople { get; set; }
        public virtual DbSet<TphPerson> TphPeople { get; set; }
        public virtual DbSet<TphCustomer> TphCustomers { get; set; }
        public virtual DbSet<TphVendor> TphVendors { get; set; }
        public virtual DbSet<TptPerson> TptPeople { get; set; }
        public virtual DbSet<TptCustomer> TptCustomers { get; set; }
        public virtual DbSet<TptVendor> TptVendors { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSqlServer(Config.GetConnectionString("TestDatabase"));
            optionsBuilder.SetupEfCoreExtensions();
        }
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ProductWithCustomSchema>().ToTable("Product", "top");
            modelBuilder.Entity<ProductWithComplexKey>().HasKey(c => new { c.Key1 });
            modelBuilder.Entity<ProductWithComplexKey>().Property<Guid>("Key1").HasDefaultValueSql("newsequentialid()");
            modelBuilder.Entity<ProductWithComplexKey>().Property<Guid>("Key2").HasDefaultValueSql("newsequentialid()");
            modelBuilder.Entity<ProductWithComplexKey>().Property<Guid>("Key3").HasDefaultValueSql("newsequentialid()");
            modelBuilder.Entity<Order>().Property<DateTime>("DbAddedDateTime").HasDefaultValueSql("getdate()");
            modelBuilder.Entity<Order>().Property<DateTime>("DbModifiedDateTime").HasComputedColumnSql("getdate()");
            modelBuilder.Entity<Order>().Property(p => p.Status).HasConversion<string>();
            modelBuilder.Entity<TpcPerson>().UseTpcMappingStrategy();
            modelBuilder.Entity<TpcCustomer>().ToTable("TpcCustomer");
            modelBuilder.Entity<TpcVendor>().ToTable("TpcVendor");
            modelBuilder.Entity<TphPerson>().Property<DateTime>("CreatedDate");
            modelBuilder.Entity<TptPerson>().ToTable("TptPeople");
            modelBuilder.Entity<TptCustomer>().ToTable("TptCustomer");
            modelBuilder.Entity<TptVendor>().ToTable("TptVendor");
            modelBuilder.Entity<Product>(t => 
                t.Property(p => p.Color).HasConversion(x => x.ToArgb(), x => Color.FromArgb(x))
            );
        }
    }
}
