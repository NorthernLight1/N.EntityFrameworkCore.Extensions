using System;
using System.Drawing;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using N.EntityFrameworkCore.Extensions.Test.Common;

namespace N.EntityFrameworkCore.Extensions.Test.Data;

public class TestDbContext : DbContext
{
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
        optionsBuilder.UseSqlite(Config.GetTestDatabaseConnectionString());
        optionsBuilder.SetupEfCoreExtensions();
        optionsBuilder.UseLazyLoadingProxies();
        optionsBuilder.ConfigureWarnings(warnings =>
            warnings.Ignore(RelationalEventId.PendingModelChangesWarning));
    }
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ProductWithCustomSchema>().ToTable("Product");
        modelBuilder.Entity<ProductWithComplexKey>().HasKey(c => new { c.Key1 });
        modelBuilder.Entity<ProductWithComplexKey>().Property<Guid>("Key1").HasDefaultValueSql("(lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || substr(lower(hex(randomblob(2))), 2) || '-' || substr('89ab', abs(random()) % 4 + 1, 1) || substr(lower(hex(randomblob(2))), 2) || '-' || lower(hex(randomblob(6))))");
        modelBuilder.Entity<ProductWithComplexKey>().Property<Guid>("Key2").HasDefaultValueSql("(lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || substr(lower(hex(randomblob(2))), 2) || '-' || substr('89ab', abs(random()) % 4 + 1, 1) || substr(lower(hex(randomblob(2))), 2) || '-' || lower(hex(randomblob(6))))");
        modelBuilder.Entity<ProductWithComplexKey>().HasKey(p => new { p.Key3, p.Key4 });
        modelBuilder.Entity<Order>().Property<DateTime>("DbAddedDateTime").HasDefaultValueSql("(strftime('%Y-%m-%d %H:%M:%f', 'now') || '9999')");
        modelBuilder.Entity<Order>().Property<DateTime>("DbModifiedDateTime").HasDefaultValueSql("(strftime('%Y-%m-%d %H:%M:%f', 'now') || '9999')").ValueGeneratedOnAddOrUpdate();
        modelBuilder.Entity<Order>().Property<bool>(p => p.DbActive).HasDefaultValue(true);
        modelBuilder.Entity<Order>().Property(p => p.Status).HasConversion<string>();
        modelBuilder.Entity<OrderWithComplexType>(b =>
        {
            b.ComplexProperty(e => e.BillingAddress);
            b.ComplexProperty(e => e.ShippingAddress);
        });
        modelBuilder.Entity<TpcPerson>().UseTpcMappingStrategy();
        modelBuilder.Entity<TpcCustomer>().ToTable("TpcCustomer");
        modelBuilder.Entity<TpcVendor>().ToTable("TpcVendor");
        modelBuilder.Entity<TphPerson>().Property<DateTime>("CreatedDate");
        modelBuilder.Entity<TptPerson>().ToTable("TptPeople");
        modelBuilder.Entity<TptCustomer>().ToTable("TptCustomer");
        modelBuilder.Entity<TptVendor>().ToTable("TptVendor");
        modelBuilder.Entity<Product>(t =>
        {
            t.ComplexProperty(p => p.Position).IsRequired();
            t.Property(p => p.Color).HasConversion(x => x.ToArgb(), x => Color.FromArgb(x));
        });
    }
}

