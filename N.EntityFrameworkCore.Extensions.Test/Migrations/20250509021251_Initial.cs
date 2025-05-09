using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace N.EntityFrameworkCore.Extensions.Test.Migrations
{
    public partial class Initial : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
           migrationBuilder.Sql("CREATE TRIGGER trgProductWithTriggers\r\nON ProductsWithTrigger\r\nFOR INSERT, UPDATE, DELETE\r\nAS\r\nBEGIN\r\n PRINT 1 END");
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {

        }
    }
}
