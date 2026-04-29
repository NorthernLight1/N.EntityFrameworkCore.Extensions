using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

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
        bool foundNonNullExternalId = fetchedOrders.Any(o => o.ExternalId != null);

        Assert.IsTrue(orders.Count() > 0, "There must be orders in the database that match condition (Price <= 10 And ExternalId != null)");
        Assert.IsTrue(orders.Count() == fetchedOrders.Count, "The number of orders must match the number of fetched orders");
        Assert.IsFalse(foundNonNullExternalId, "Fetched orders should not contain any items where ExternalId is not null.");
    }
    [TestMethod]
    public async Task With_Options_IgnoreColumns()
    {
        var dbContext = SetupDbContext(true);
        var orders = dbContext.Orders.Where(o => o.Price <= 10 && o.ExternalId != null).ToList();
        var fetchedOrders = (await dbContext.Orders.BulkFetchAsync(orders, options => { options.IgnoreColumns = o => new { o.ExternalId }; })).ToList();
        bool foundNonNullExternalId = fetchedOrders.Any(o => o.ExternalId != null);

        Assert.IsTrue(orders.Count() > 0, "There must be orders in the database that match condition (Price <= 10 And ExternalId != null)");
        Assert.IsTrue(orders.Count() == fetchedOrders.Count, "The number of orders must match the number of fetched orders");
        Assert.IsFalse(foundNonNullExternalId, "Fetched orders should not contain any items where ExternalId is not null.");
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
}
