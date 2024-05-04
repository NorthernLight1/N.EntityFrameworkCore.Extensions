using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Data.Enums;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace N.EntityFrameworkCore.Extensions.Test.Data
{
    public class ProductCategory
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public bool Active { get; internal set; }
    }
}
