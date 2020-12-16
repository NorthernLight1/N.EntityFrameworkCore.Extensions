using Microsoft.EntityFrameworkCore;
using N.EntityFrameworkCore.Extensions.Test.Common;

namespace N.EntityFrameworkCore.Extensions.Test.Data
{
    public class TestDbContext : DbContext
    {
        public virtual DbSet<Order> Orders { get; set;  }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSqlServer(Config.GetConnectionString("TestDatabase"));
        }
    }
}
