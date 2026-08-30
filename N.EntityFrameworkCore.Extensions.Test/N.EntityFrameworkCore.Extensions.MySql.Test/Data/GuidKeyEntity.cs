using System;
using System.ComponentModel.DataAnnotations;

namespace N.EntityFrameworkCore.Extensions.Test.Data;

/// <summary>
/// Client-assigned Guid primary key (no SQL default / identity). Used for #82/#65 coverage.
/// </summary>
public class GuidKeyEntity
{
    [Key]
    public Guid Id { get; set; }
    [StringLength(100)]
    public string Name { get; set; }
}
