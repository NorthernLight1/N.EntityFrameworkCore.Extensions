using Microsoft.EntityFrameworkCore;
using N.EntityFrameworkCore.Extensions;
using N.EntityFrameworkCore.Extensions.Test.Common;
using N.EntityFrameworkCore.Extensions.Test.Data;

TestDatabaseInitializer.EnsureCreated(new TestDbContext());
using var db = new TestDbContext();
db.OrdersWithOwnedDetail.Truncate();
var orders = new List<OrderWithOwnedDetail> { new() { Price = 9.99M, Detail = new OwnedDetail { City = "Seattle", Region = "WA" } } };
var rows = db.BulkInsert(orders, o => o.AutoMapOutput = true);
Console.WriteLine($"Rows: {rows}");
var raw = db.Database.SqlQueryRaw<string>("SELECT \"Detail_City\" FROM \"OrdersWithOwnedDetail\"").ToList();
Console.WriteLine($"Raw City: '{raw.FirstOrDefault()}'");
var saved = db.OrdersWithOwnedDetail.AsNoTracking().FirstOrDefault();
Console.WriteLine($"EF Detail null: {saved?.Detail == null}, City: {saved?.Detail?.City}");
