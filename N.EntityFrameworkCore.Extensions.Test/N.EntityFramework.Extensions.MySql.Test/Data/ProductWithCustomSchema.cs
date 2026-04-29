using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace N.EntityFrameworkCore.Extensions.Test.Data;

public class ProductWithCustomSchema
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.None)]
    public string Id { get; set; }
    [StringLength(50)]
    public string Name { get; set; }
    public decimal Price { get; set; }
}