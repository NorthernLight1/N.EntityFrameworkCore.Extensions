using Microsoft.EntityFrameworkCore;

namespace N.EntityFrameworkCore.Extensions.Test.Data;

public class OrgNode
{
    public int Id { get; set; }
    public string Name { get; set; }
    public HierarchyId NodePath { get; set; }
}
