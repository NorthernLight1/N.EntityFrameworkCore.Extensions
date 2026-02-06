using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace N.EntityFrameworkCore.Extensions.Test.Data;
public class OrderWithComplexType
{
    [Key]
    public long Id { get; set; }
    [Required]
    public Address ShippingAddress { get; set; }
    [Required]
    public Address BillingAddress { get; set; }
}
