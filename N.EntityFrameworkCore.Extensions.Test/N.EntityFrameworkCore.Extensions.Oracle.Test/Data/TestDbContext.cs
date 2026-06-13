using System;
using System.Drawing;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using N.EntityFrameworkCore.Extensions.Test.Common;

namespace N.EntityFrameworkCore.Extensions.Test.Data;

public class TestDbContext : DbContext
{
    private readonly string _connectionString;

    public TestDbContext(string connectionString = null)
    {
        _connectionString = connectionString;
    }

    public virtual DbSet<Product> Products { get; set; }
    public virtual DbSet<ProductCategory> ProductCategories { get; set; }
    public virtual DbSet<ProductWithCustomSchema> ProductsWithCustomSchema { get; set; }
    public virtual DbSet<ProductWithComplexKey> ProductsWithComplexKey { get; set; }
    public virtual DbSet<ProductWithTrigger> ProductsWithTrigger { get; set; }
    public virtual DbSet<Order> Orders { get; set; }
    public virtual DbSet<OrderWithComplexType> OrdersWithComplexType { get; set; }
    public virtual DbSet<TpcPerson> TpcPeople { get; set; }
    public virtual DbSet<TphPerson> TphPeople { get; set; }
    public virtual DbSet<TphCustomer> TphCustomers { get; set; }
    public virtual DbSet<TphVendor> TphVendors { get; set; }
    public virtual DbSet<TptPerson> TptPeople { get; set; }
    public virtual DbSet<TptCustomer> TptCustomers { get; set; }
    public virtual DbSet<TptVendor> TptVendors { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseOracle(_connectionString ?? Config.GetTestDatabaseConnectionString());
        optionsBuilder.SetupEfCoreExtensions();
        optionsBuilder.UseLazyLoadingProxies();
        // Tell EF Core to allow mismatched models for this test run
        optionsBuilder.ConfigureWarnings(warnings =>
            warnings.Ignore(RelationalEventId.PendingModelChangesWarning));
    }
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Product>().ToTable("PRODUCTS");
        modelBuilder.Entity<ProductCategory>().ToTable("PRODUCTCATEGORIES");
        modelBuilder.Entity<ProductWithCustomSchema>().ToTable("PRODUCTSWITHCUSTOMSCHEMA");
        modelBuilder.Entity<ProductWithComplexKey>().ToTable("PRODUCTSWITHCOMPLEXKEY");
        modelBuilder.Entity<ProductWithTrigger>().ToTable("PRODUCTSWITHTRIGGER");
        modelBuilder.Entity<Order>().ToTable("ORDERS");
        modelBuilder.Entity<OrderWithComplexType>().ToTable("ORDERSWITHCOMPLEXTYPE");
        modelBuilder.Entity<ProductWithComplexKey>().HasKey(c => new { c.Key1 });
        modelBuilder.Entity<ProductWithComplexKey>().Property<Guid>("Key1").HasDefaultValueSql("SYS_GUID()");
        modelBuilder.Entity<ProductWithComplexKey>().Property<Guid>("Key2").HasDefaultValueSql("SYS_GUID()");
        modelBuilder.Entity<ProductWithComplexKey>().HasKey(p => new { p.Key3, p.Key4 });
        modelBuilder.Entity<Order>().Property<DateTime>("DbAddedDateTime").HasDefaultValueSql("SYSTIMESTAMP");
        modelBuilder.Entity<Order>().Property<DateTime>("DbModifiedDateTime").HasDefaultValueSql("SYSTIMESTAMP");
        modelBuilder.Entity<Order>().Property<bool>(p => p.DbActive).HasDefaultValueSql("1");
        modelBuilder.Entity<Order>().Property(p => p.Status).HasConversion<string>();
        modelBuilder.Entity<OrderWithComplexType>(b =>
        {
            b.ComplexProperty(e => e.BillingAddress);
            b.ComplexProperty(e => e.ShippingAddress);
        });
        modelBuilder.Entity<TpcPerson>().UseTpcMappingStrategy();
        modelBuilder.Entity<TpcCustomer>().ToTable("TPCCUSTOMER");
        modelBuilder.Entity<TpcVendor>().ToTable("TPCVENDOR");
        modelBuilder.Entity<TphPerson>().UseTphMappingStrategy();
        modelBuilder.Entity<TphPerson>().ToTable("TphPeople");
        modelBuilder.Entity<TphPerson>().Property<DateTime>("CreatedDate");
        modelBuilder.Entity<TptPerson>().ToTable("TPTPEOPLE");
        modelBuilder.Entity<TptCustomer>().ToTable("TPTCUSTOMER");
        modelBuilder.Entity<TptVendor>().ToTable("TPTVENDOR");
        modelBuilder.Entity<Product>(t =>
        {
            t.ComplexProperty(p => p.Position).IsRequired();
            t.Property(p => p.Color).HasConversion(x => x.ToArgb(), x => Color.FromArgb(x));
        });
    }
}
