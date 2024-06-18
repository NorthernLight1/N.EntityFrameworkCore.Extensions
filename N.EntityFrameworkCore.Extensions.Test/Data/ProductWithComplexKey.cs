﻿using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace N.EntityFrameworkCore.Extensions.Test.Data;

public class ProductWithComplexKey
{
    public Guid Key1 { get; set; }
    public Guid Key2 { get; set; }
    public Guid Key3 { get; set; }
    public decimal Price { get; set; }
    public bool OutOfStock { get; set; }
    [Column("Status")]
    [StringLength(25)]
    public string StatusString { get; set; }
    public DateTime? UpdatedDateTime { get; set; }
    public ProductWithComplexKey()
    {
        //Key1 = Guid.NewGuid();
        //Key2 = Guid.NewGuid();
        //Key3 = Guid.NewGuid();
    }
}