using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text;

namespace N.EntityFrameworkCore.Extensions.Test.Data
{
    public class Product
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.None)]
        public string Id { get; set; }
        public decimal Price { get; set; }
        public bool OutOfStock { get; set; }
        [Column("Status")]
        [StringLength(25)]
        public string StatusString { get; set; }
        public DateTime? UpdatedDateTime { get; set; }
        public Product()
        {

        }
    }
}
