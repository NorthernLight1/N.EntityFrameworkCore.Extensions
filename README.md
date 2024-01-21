N.EntityFrameworkCore.Extensions
--------------------

[![latest version](https://img.shields.io/nuget/v/N.EntityFrameworkCore.Extensions)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions) [![downloads](https://img.shields.io/nuget/dt/N.EntityFrameworkCore.Extensions)](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions)


## Bulk data support  data support for EntityFrameworkCore v8.0.0+

N.EntityFrameworkCore.Extensions extends your DbContext with high-performance bulk operations: BulkDelete, BulkFetch, BulkInsert, BulkMerge, BulkSaveChanges, BulkSync, BulkUpdate, Fetch, FromSqlQuery, DeleteFromQuery, InsertFromQuery, UpdateFromQuery, QueryToCsvFile, SqlQueryToCsvFile

Supports: Transaction, Synchronous & Asynchronous Execution

Inheritance Models: Table-Per-Concrete, Table-Per-Hierarchy, Table-Per-Type

  ### Installation

  The latest stable version is available on [NuGet](https://www.nuget.org/packages/N.EntityFrameworkCore.Extensions).

  ```sh
  dotnet add package N.EntityFrameworkCore.Extensions
  ```
  
 ## Usage
   
  **BulkInsert() - Performs an insert operation with a large number of entities**  
   ```
  var dbContext = new MyDbContext();  
  var orders = new List<Order>();  
  for(int i=0; i<10000; i++)  
  {  
      orders.Add(new Order { OrderDate = DateTime.UtcNow, TotalPrice = 2.99 });  
  }  
  dbContext.BulkInsert(orders);  
 ```
  **BulkDelete() - Performs a delete operation with a large number of entities**  
  ```
  var dbContext = new MyDbContext();  
  var orders = dbContext.Orders.Where(o => o.TotalPrice < 5.35M);  
  dbContext.BulkDelete(orders);
  ```
  **BulkFetch() - Retrieves entities that are contained in a list**  
  ```
  var ids = new List<int> { 10001, 10002, 10003, 10004, 10005 };
  var products = dbContext.Products.BulkFetch(ids, options => { options.JoinOnCondition = (s, t) => s.Id == t.Id; }).ToList();
  ```
  **BulkUpdate() - Performs an update operation with a large number of entities**  
  ```
  var dbContext = new MyDbContext();  
  var products = dbContext.Products.Where(o => o.Price < 5.35M);
  foreach(var product in products)
  {
      order.Price = 6M;
  }
  dbContext.BulkUpdate(products);
  ```
  **BulkMerge() - Performs a merge operation with a large number of entities**
  ```
  var dbContext = new MyDbContext();
  var products = new List<Product>();
  var existingProducts = dbContext.Products.Where(o => o.Price < 5.35M);
  foreach(var product in existingProducts)
  {
      product.Price = 6M;
  }
  products.AddRange(existingProducts);
  products.Add(new Product { Name="Hat", Price=10.25M });
  products.Add(new Product { Name="Shirt", Price=20.95M });
  dbContext.BulkMerge(products);
  ```
  **BulkSaveChanges() - Saves all changes using bulk operations**  
   ```
  var dbContext = new MyDbContext();  
  var orders = new List<Order>();  
  for(int i=0; i<10000; i++)  
  {  
      orders.Add(new Order { Id=-i,OrderDate = DateTime.UtcNow, TotalPrice = 2.99 });  
  }
  dbContext.Orders.AddRange(orders);
  dbContext.BulkSaveChanges();  
 ```
   **BulkSync() - Performs a sync operation with a large number of entities.** 
   
   By default any entities that do not exists in the source list will be deleted, but this can be disabled in the options.
  ```
  var dbContext = new MyDbContext();
  var products = new List<Product>();
  var existingProducts = dbContext.Products.Where(o => o.Id <= 1000);
  foreach(var product in existingProducts)
  {
      product.Price = 6M;
  }
  products.AddRange(existingProducts);
  products.Add(new Product { Name="Hat", Price=10.25M });
  products.Add(new Product { Name="Shirt", Price=20.95M });
  //All existing products with Id > 1000 will be deleted
  dbContext.BulkSync(products);
  ```
  **Fetch() - Retrieves data in batches.**  
  ```
  var dbContext = new MyDbContext();  
  var query = dbContext.Products.Where(o => o.Price < 5.35M);
  query.Fetch(result =>
    {
      batchCount++;
      totalCount += result.Results.Count();
    }, 
    new FetchOptions { BatchSize = 1000 }
  );
  dbContext.BulkUpdate(products);
  ```
  **DeleteFromQuery() - Deletes records from the database using a LINQ query without loading data into DbContext**  
   ``` 
  var dbContext = new MyDbContext(); 
  
  //This will delete all products  
  dbContext.Products.DeleteFromQuery() 
  
  //This will delete all products that are under $5.35  
  dbContext.Products.Where(x => x.Price < 5.35M).DeleteFromQuery()  
```
  **InsertFromQuery() - Inserts records from the database using a LINQ query without loading data into DbContext**  
   ``` 
  var dbContext = new MyDbContext(); 
  
  //This will take all products priced under $10 from the Products table and 
  //insert it into the ProductsUnderTen table
  dbContext.Products.Where(x => x.Price < 10M).InsertFromQuery("ProductsUnderTen", o => new { o.Id, o.Price });
```
  **UpdateFromQuery() - Updates records from the database using a LINQ query without loading data into DbContext**  
   ``` 
  var dbContext = new MyDbContext(); 
  
  //This will change all products priced at $5.35 to $5.75 
  dbContext.Products.Where(x => x.Price == 5.35M).UpdateFromQuery(o => new Product { Price = 5.75M }) 
```
## Options
  **Transaction** 
  
  When using any of the following bulk data operations (BulkDelete, BulkInsert, BulkMerge, BulkSaveChanges, BulkSync, BulkUpdate, DeleteFromQuery, InsertFromQuery), if an external transaction exists, then it will be utilized.
   
   ``` 
  var dbContext = new MyDbContext(); 
  var transaction = context.Database.BeginTransaction();
  try
  {
      dbContext.BulkInsert(orders);
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
| **BulkFetch** |
| BulkFetch<T>(items)  | Retrieve entities that are contained in the items list.  |
| BulkFetch<T>(items, options)  | Retrieve entities that are contained in the items list.  |
| BulkFetchAsync<T>(items)  | Retrieve entities that are contained in the items list.  |
| BulkFetchAsync<T>(items, options)  | Retrieve entities that are contained in the items list.  | 
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
| **BulkSaveChanges** |
| BulkSaveChanges<T>()  | Save changes using high-performance bulk operations. Should be used instead of SaveChanges(). |
| BulkSaveChanges<T>( acceptAllChangesOnSave)  | Save changes using high-performance bulk operations. Should be used instead of SaveChanges(). |
| BulkSaveChangesAsync<T>()  | Save changes using high-performance bulk operations. Should be used instead of SaveChanges(). |
| BulkSaveChangesAsync<T>( acceptAllChangesOnSave)  | Save changes using high-performance bulk operations. Should be used instead of SaveChanges(). |
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
| Fetch(fetchAction, options) | Fetch rows in batches from the database using a LINQ query |
| FetchAsync(fetchAction)  | Fetch rows asynchronously in batches from the database using a LINQ query |
| FetchAsync(fetchAction, options)  | Fetch rows asynchronously in batches from the database using a LINQ query |
| FetchAsync(fetchAction, cancellationToken) | Fetch rows asynchronously in batches from the database using a LINQ query  | 
| FetchAsync(fetchAction, options, cancellationToken) | Fetch rows asynchronously in batches from the database using a LINQ query  | 
  
## Donations
---------

If you found this project helpful and you would like to support it, feel free to donate through paypal or bitcoin.

| Paypal | Bitcoin |
| ------ | ------- |
| [![](https://www.paypalobjects.com/en_US/i/btn/btn_donateCC_LG.gif)](https://www.paypal.com/donate/?hosted_button_id=HR6JSVYKAMLSQ) |  <center> [![](bitcoin.png)](bitcoin:bc1qxqpymnf4gj22nt4wj3wy56ks48fw59v8y9sg9z)<br />[1H5tqKZoWdqkR54PGe9w67EzBnLXHBFmt9](bitcoin:3ApywX5cqu9Nu2Qhz4pC1iVztBdoKVzWHu)</center> |
bc1qxqpymnf4gj22nt4wj3wy56ks48fw59v8y9sg9z