N.EntityFrameworkCore.Extensions
--------------------

[![latest version](https://img.shields.io/nuget/v/N.EntityFrameworkCore.Extensions)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions)

N.EntityFrameworkCore.Extensions adds Bulk data support to EntityFrameworkCore v5.0.1+

The framework currently supports the following operations:

  BulkDelete, BulkInsert, BulkMerge, BulkSync, BulkUpdate, DeleteFromQuery, InsertFromQuery, UpdateFromQuery, Fetch
  
  Inheritance models supported: Table-Per-Hierarchy
  
  Supports: transaction, synchronous & asynchronous execution
  
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
   **BulkSync() - Performs a sync operation with a large number of entities.** 
   
   By default any entities that do not exists in the source list will be deleted, but this can be disabled in the options.
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
## Documentation
| Name  | Description |
| ------------- | ------------- |
| **BulkDelete** |
| BulkDelete<T>(items)  | Bulk delete entities in your database.  |
| BulkDelete<T>(items, options)  | Bulk delete entities in your database.   |
| BulkDeleteAsync(items)  | Bulk delete entities asynchronously in your database.  |
| BulkDeleteAsync(items, cancellationToken)  | Bulk delete entities asynchronously in your database.  |
| BulkDeleteAsync(items, options)  | Bulk delete entities asynchronously in your database.  |
| BulkDeleteAsync(items, options, cancellationToken)  | Bulk delete entities asynchronously in your database.  |
| **BulkInsert** |
| BulkInsert<T>(items)  | Bulk insert entities in your database.  |
| BulkInsert<T>(items, options)  | Bulk insert entities in your database.   |
| BulkInsertAsync(items)  | Bulk insert entities asynchronously in your database.  |
| BulkInsertAsync(items, cancellationToken)  | Bulk insert entities asynchronously in your database.  |
| BulkInsertAsync(items, options)  | Bulk insert entities asynchronously in your database.  |
| BulkInsertAsync(items, options, cancellationToken)  | Bulk insert entities asynchronously in your database.  |
| **BulkMerge** |
| BulkMerge<T>(items)  | Bulk merge entities in your database.  |
| BulkMerge<T>(items, options)  | Bulk merge entities in your database.   |
| BulkMergeAsync(items)  | Bulk merge entities asynchronously in your database.  |
| BulkMergeAsync(items, cancellationToken)  | Bulk merge entities asynchronously in your database.  |
| BulkMergeAsync(items, options)  | Bulk merge entities asynchronously in your database.  |
| BulkMergeAsync(items, options, cancellationToken)  | Bulk merge entities asynchronously in your database.  |
| **BulkSync** |
| BulkSync<T>(items)  | Bulk sync entities in your database.  |
| BulkSync<T>(items, options)  | Bulk sync entities in your database.   |
| BulkSyncAsync(items)  | Bulk sync entities asynchronously in your database.  |
| BulkSyncAsync(items, cancellationToken)  | Bulk sync entities asynchronously in your database.  |
| BulkSyncAsync(items, options)  | Bulk sync entities asynchronously in your database.  |
| BulkSyncAsync(items, options, cancellationToken)  | Bulk sync entities asynchronously in your database.  |
| **BulkUpdate** |  
| BulkUpdate<T>(items)  | Bulk update entities in your database.  |
| BulkUpdate<T>(items, options)  | Bulk update entities in your database.   |
| BulkUpdateAsync(items)  | Bulk update entities asynchronously in your database.  |
| BulkUpdateAsync(items, cancellationToken)  | Bulk update entities asynchronously in your database.  |
| BulkUpdateAsync(items, options)  | Bulk update entities asynchronously in your database.  |
| BulkUpdateAsync(items, options, cancellationToken)  | Bulk update entities asynchronously in your database.  |
| **DeleteFromQuery** |
| DeleteFromQuery() | Deletes all rows from the database using a LINQ query without loading in context |
| DeleteFromQueryAsync() | Deletes all rows from the database using a LINQ query without loading in context using asynchronous task |
| DeleteFromQueryAsync(cancellationToken) | Deletes all rows from the database using a LINQ query without loading in context using asynchronous task  |
| **InsertFromQuery** |
| InsertFromQuery(tableName, selectExpression) | Insert all rows from the database using a LINQ query without loading in context |
| InsertFromQueryAsync(tableName, selectExpression) | Insert all rows from the database using a LINQ query without loading in context using asynchronous task |
| InsertFromQueryAsync(tableName, selectExpression, cancellationToken) | Insert all rows from the database using a LINQ query without loading in context using asynchronous task  |
| **UpdateFromQuery** |
| UpdateFromQuery(updateExpression) | Updates all rows from the database using a LINQ query without loading in context |
| UpdateFromQueryAsync(updateExpression) | Updates all rows from the database using a LINQ query without loading in context using asynchronous task |
| UpdateFromQueryAsync(updateExpression, cancellationToken) | Updates all rows from the database using a LINQ query without loading in context using asynchronous task  |
| **Fetch** |
| Fetch(fetchAction) | Fetch rows in batches from the database using a LINQ query |
| FetchAsync(fetchAction)  | Fetch rows asynchronously in batches from the database using a LINQ query |
| FetchAsync(fetchAction, cancellationToken) | Fetch rows asynchronously in batches from the database using a LINQ query  | 
  
  
