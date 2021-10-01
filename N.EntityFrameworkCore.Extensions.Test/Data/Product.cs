using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace N.EntityFrameworkCore.Extensions.Test.Data
{
    public class Product
    {
        [Key]
        public string Id { get; set; }
        public decimal Price { get; set; }
        public bool OutOfStock { get; set; }
        public DateTime? UpdatedDateTime { get; set; }
        public Product()
        {

        }
    }
}
