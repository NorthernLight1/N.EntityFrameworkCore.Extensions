N.EntityFrameworkCore.Extensions
--------------------

[![latest version](https://img.shields.io/nuget/v/N.EntityFrameworkCore.Extensions)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions)

N.EntityFrameworkCore.Extensions adds Bulk data support to EntityFrameworkCore v5.0.1+

The framework currently supports the following operations:

  BulkDelete, BulkInsert, BulkMerge, BulkUpdate, DeleteFromQuery, InsertFromQuery, UpdateFromQuery, Fetch
  
  ### Installation

  The latest stable version is available on [NuGet](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions).

  ```sh
  dotnet add package N.EntityFrameworkCore.Extensions
  ```
  
 ## Usage
   
  **BulkInsert() - Performs a insert operation with a large number of entities**  
   ```
  var dbcontext = new MyDbContext();  
  var orders = new List<Order>();  
  for(int i=0; i<10000; i++)  
  {  
      orders.Add(new Order { OrderDate = DateTime.UtcNow, TotalPrice = 2.99 });  
  }  
  dbcontext.BulkInsert(orders);  
 ```
  **BulkDelete() - Performs a delete operation with a large number of entities**  
  ```
  var dbcontext = new MyDbContext();  
  var orders = dbcontext.Orders.Where(o => o.TotalPrice < 5.35M);  
  dbcontext.BulkDelete(orders);
  ```
  **BulkUpdate() - Performs a update operation with a large number of entities**  
  ```
  var dbcontext = new MyDbContext();  
  var products = dbcontext.Products.Where(o => o.Price < 5.35M);
  foreach(var product in products)
  {
      order.Price = 6M;
  }
  dbcontext.BulkUpdate(products);
  ```
  **BulkMerge() - Performs a merge operation with a large number of entities**
  ```
  var dbcontext = new MyDbContext();
  var products = new List<Product>();
  var existingProducts = dbcontext.Products.Where(o => o.Price < 5.35M);
  foreach(var product in existingProducts)
  {
      product.Price = 6M;
  }
  products.AddRange(existingProducts);
  products.Add(new Product { Name="Hat", Price=10.25M });
  products.Add(new Product { Name="Shirt", Price=20.95M });
  dbcontext.BulkMerge(products);
  ```
   **BulkSync() - Performs a sync operation with a large number of entities. By default any entities that do not exists in the source list will be deleted, but this can be disalbed in the options.**
  ```
  var dbcontext = new MyDbContext();
  var products = new List<Product>();
  var existingProducts = dbcontext.Products.Where(o => o.Id <= 1000);
  foreach(var product in existingProducts)
  {
      product.Price = 6M;
  }
  products.AddRange(existingProducts);
  products.Add(new Product { Name="Hat", Price=10.25M });
  products.Add(new Product { Name="Shirt", Price=20.95M });
  //All existing products with Id > 1000 will be deleted
  dbcontext.BulkSync(products);
  ```
  **Fetch() - Retrieves data in batches.**  
  ```
  var dbcontext = new MyDbContext();  
  var query = dbcontext.Products.Where(o => o.Price < 5.35M);
  query.Fetch(result =>
    {
      batchCount++;
      totalCount += result.Results.Count();
    }, 
    new FetchOptions { BatchSize = 1000 }
  );
  dbcontext.BulkUpdate(products);
  ```
  **DeleteFromQuery() - Deletes records from the database using a LINQ query without loading data in the context**  
   ``` 
  var dbcontext = new MyDbContext(); 
  
  //This will delete all products  
  dbcontext.Products.DeleteFromQuery() 
  
  //This will delete all products that are under $5.35  
  dbcontext.Products.Where(x => x.Price < 5.35M).DeleteFromQuery()  
```
  **InsertFromQuery() - Inserts records from the database using a LINQ query without loading data in the context**  
   ``` 
  var dbcontext = new MyDbContext(); 
  
  //This will take all products priced under $10 from the Products table and 
  //insert it into the ProductsUnderTen table
  dbcontext.Products.Where(x => x.Price < 10M).InsertFromQuery("ProductsUnderTen", o => new { o.Id, o.Price });
```
  **UpdateFromQuery() - Updates records from the database using a LINQ query without loading data in the context**  
   ``` 
  var dbcontext = new MyDbContext(); 
  
  //This will change all products priced at $5.35 to $5.75 
  dbcontext.Products.Where(x => x.Price == 5.35M).UpdateFromQuery(o => new Product { Price = 5.75M }) 
```

## Options
  **Transaction** 
  
  When using any of the following bulk data operations (BulkDelete, BulkInsert, BulkMerge, BulkSync, BulkUpdate, DeleteFromQuery, InsertFromQuery), if an external transaction exists, then it will be utilized.
   
   ``` 
  var dbcontext = new MyDbContext(); 
  var transaction = context.Database.BeginTransaction();
  try
  {
      dbcontext.BulkInsert(orders);
      transaction.Commit();
  }
  catch
  {
      transaction.Rollback();
  }
```  
