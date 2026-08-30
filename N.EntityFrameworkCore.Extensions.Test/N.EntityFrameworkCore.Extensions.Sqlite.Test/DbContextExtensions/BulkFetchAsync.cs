using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Data;

namespace N.EntityFrameworkCore.Extensions.Test.DbContextExtensions;

[TestClass]
public class BulkFetchAsync : DbContextExtensionsBase
{
    [TestMethod]
    public async Task With_Complex_Property()
    {
        var dbContext = SetupDbContext(true);
        var products = dbContext.Products.Where(o => o.Price == 1.25m).ToList();
        var fetchedProducts = (await dbContext.Products.BulkFetchAsync(products)).ToList();
        bool foundNullPositionProperty = fetchedProducts.Any(o => o.Position == null);

        Assert.IsTrue(products.Count > 0, "There must be orders in database that match this condition (Price = $1.25)");
        Assert.IsTrue(products.Count == fetchedProducts.Count, "The number of rows deleted must match the count of existing rows in database");
        Assert.IsFalse(foundNullPositionProperty, "The Position complex property should be populated when using BulkFetchAsync()");
    }
    [TestMethod]
    public async Task With_Default_Options()
    {
        var dbContext = SetupDbContext(true);
        var orders = dbContext.Orders.Where(o => o.Price == 1.25M).ToList();
        var fetchedOrders = (await dbContext.Orders.BulkFetchAsync(orders)).ToList();
        bool ordersAreMatched = true;

        foreach (var fetchedOrder in fetchedOrders)
        {
            var order = orders.First(o => o.Id == fetchedOrder.Id);
            if (order.ExternalId != fetchedOrder.ExternalId || order.AddedDateTime != fetchedOrder.AddedDateTime || order.ModifiedDateTime != fetchedOrder.ModifiedDateTime)
            {
                ordersAreMatched = false;
                break;
            }
        }

        Assert.IsTrue(orders.Count > 0, "There must be orders in database that match this condition (Price = $1.25)");
        Assert.IsTrue(orders.Count == fetchedOrders.Count, "The number of rows deleted must match the count of existing rows in database");
        Assert.IsTrue(ordersAreMatched, "The orders from BulkFetchAsync() should match what is retrieved from DbContext");
    }
    [TestMethod]
    public async Task With_Enum()
    {
        var dbContext = SetupDbContext(true);
        var products = dbContext.Products.Where(o => o.Price == 1.25m).ToList();
        var fetchedProducts = (await dbContext.Products.BulkFetchAsync(products)).ToList();
        bool productsAreMatched = true;

        foreach (var fetchedProduct in fetchedProducts)
        {
            var product = products.First(o => o.Id == fetchedProduct.Id);
            if (product.Id != fetchedProduct.Id || product.Name != fetchedProduct.Name || product.StatusEnum != fetchedProduct.StatusEnum)
            {
                productsAreMatched = false;
                break;
            }
        }

        Assert.IsTrue(products.Count > 0, "There must be orders in database that match this condition (Price = $1.25)");
        Assert.IsTrue(products.Count == fetchedProducts.Count, "The number of rows deleted must match the count of existing rows in database");
        Assert.IsTrue(productsAreMatched, "The products from BulkFetchAsync() should match what is retrieved from DbContext");
    }
    [TestMethod]
    public async Task With_IQueryable()
    {
        var dbContext = SetupDbContext(true);
        var orders = dbContext.Orders.Where(o => o.Price <= 10 && o.ExternalId != null);
        var fetchedOrders = (await dbContext.Orders.BulkFetchAsync(orders, options => { options.IgnoreColumns = o => new { o.ExternalId }; })).ToList();

        Assert.IsTrue(orders.Count() > 0, "There must be orders in the database that match condition (Price <= 10 And ExternalId != null)");
        Assert.IsTrue(orders.Count() == fetchedOrders.Count, "The number of orders must match the number of fetched orders");
        Assert.IsTrue(fetchedOrders.All(o => o.ExternalId == null), "Fetched orders should have ExternalId cleared (IgnoreColumns)");
        Assert.IsTrue(fetchedOrders.Any(f => orders.Any(o => o.Id == f.Id && o.Price == f.Price)), "Non-ignored columns like Price should still match the source");
    }
    [TestMethod]
    public async Task With_KeyOnly_Stubs()
    {
        var dbContext = SetupDbContext(true);
        var dbOrders = dbContext.Orders.Where(o => o.Price == 1.25M && o.ExternalId != null).Take(20).ToList();
        Assert.IsTrue(dbOrders.Count > 0);

        // Key-only stubs with deliberately wrong Price/ExternalId
        var stubs = dbOrders.Select(o => new Order { Id = o.Id, Price = 999M, ExternalId = "wrong" }).ToList();
        var fetched = (await dbContext.Orders.BulkFetchAsync(stubs)).ToList();

        Assert.AreEqual(stubs.Count, fetched.Count);
        foreach (var stub in stubs)
            Assert.IsFalse(object.ReferenceEquals(stub, fetched.FirstOrDefault(f => f.Id == stub.Id)),
                "BulkFetch must return database entities, not the input stubs");

        foreach (var f in fetched)
        {
            var db = dbOrders.First(o => o.Id == f.Id);
            Assert.AreEqual(db.Price, f.Price);
            Assert.AreEqual(db.ExternalId, f.ExternalId);
        }
    }
    [TestMethod]
    public async Task With_Options_IgnoreColumns()
    {
        var dbContext = SetupDbContext(true);
        var orders = dbContext.Orders.Where(o => o.Price <= 10 && o.ExternalId != null).ToList();
        var fetchedOrders = (await dbContext.Orders.BulkFetchAsync(orders, options => { options.IgnoreColumns = o => new { o.ExternalId }; })).ToList();

        Assert.IsTrue(orders.Count() > 0, "There must be orders in the database that match condition (Price <= 10 And ExternalId != null)");
        Assert.IsTrue(orders.Count() == fetchedOrders.Count, "The number of orders must match the number of fetched orders");
        Assert.IsTrue(fetchedOrders.All(o => o.ExternalId == null), "Fetched orders should have ExternalId cleared (IgnoreColumns)");
        Assert.IsTrue(fetchedOrders.Any(f => orders.Any(o => o.Id == f.Id && o.Price == f.Price)), "Non-ignored columns like Price should still match the source");
    }
    [TestMethod]
    public async Task With_ValueConverter()
    {
        var dbContext = SetupDbContext(true);
        var products = dbContext.Products.Where(o => o.Price == 1.25M).ToList();
        var fetchedProducts = (await dbContext.Products.BulkFetchAsync(products)).ToList();
        bool areMatched = true;

        foreach (var fetchedProduct in fetchedProducts)
        {
            var product = products.First(o => o.Id == fetchedProduct.Id);
            if (product.Name != fetchedProduct.Name || product.Price != fetchedProduct.Price
                                                    || product.Color != fetchedProduct.Color)
            {
                areMatched = false;
                break;
            }
        }

        Assert.IsTrue(products.Count > 0, "There must be orders in database that match this condition (Price = $1.25)");
        Assert.IsTrue(products.Count == fetchedProducts.Count, "The number of rows deleted must match the count of existing rows in database");
        Assert.IsTrue(areMatched, "The products from BulkFetchAsync() should match what is retrieved from DbContext");
    }
    [TestMethod]
    public async Task With_AttachToContext()
    {
        var dbContext = SetupDbContext(true);
        var stubs = dbContext.Orders.Where(o => o.Price == 1.25M).Take(3).Select(o => new Order { Id = o.Id }).ToList();
        var fetched = (await dbContext.Orders.BulkFetchAsync(stubs, o => o.AttachToContext = true)).ToList();
        Assert.AreEqual(3, fetched.Count);
        Assert.IsTrue(fetched.All(o => dbContext.Entry(o).State == EntityState.Unchanged));
    }


    [TestMethod]
    public async Task With_Options_JoinOnCondition()
    {
        var dbContext = SetupDbContext(true);
        var dbOrders = dbContext.Orders.Where(o => o.Price == 1.25M && o.ExternalId != null).Take(5).ToList();
        Assert.IsTrue(dbOrders.Count > 0);
        var stubs = dbOrders.Select(o => new Order { Id = o.Id + 999999, ExternalId = o.ExternalId, Price = 999M }).ToList();
        var fetched = (await dbContext.Orders.BulkFetchAsync(stubs, o => o.JoinOnCondition = (s, t) => s.ExternalId == t.ExternalId)).ToList();

        Assert.AreEqual(stubs.Count, fetched.Count);
        foreach (var f in fetched)
        {
            var db = dbOrders.First(o => o.ExternalId == f.ExternalId);
            Assert.AreEqual(db.Id, f.Id);
            Assert.AreEqual(db.Price, f.Price);
        }
    }

    [TestMethod]
    public async Task With_Options_InputColumns()
    {
        var dbContext = SetupDbContext(true);
        var dbOrders = dbContext.Orders.Where(o => o.Price == 1.25M && o.ExternalId != null).Take(5).ToList();
        Assert.IsTrue(dbOrders.Count > 0);
        var stubs = dbOrders.Select(o => new Order { ExternalId = o.ExternalId, Price = 999M }).ToList();
        var fetched = (await dbContext.Orders.BulkFetchAsync(stubs, o =>
        {
            o.JoinOnCondition = (s, t) => s.ExternalId == t.ExternalId;
            o.InputColumns = x => new { x.ExternalId };
        })).ToList();

        Assert.AreEqual(stubs.Count, fetched.Count);
        foreach (var f in fetched)
        {
            var db = dbOrders.First(o => o.ExternalId == f.ExternalId);
            Assert.AreEqual(db.ExternalId, f.ExternalId);
        }
    }
}