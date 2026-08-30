using NetTopologySuite.Geometries;

namespace N.EntityFrameworkCore.Extensions.Test.Data;

public class LocationWithSpatial
{
    public int Id { get; set; }
    public string Name { get; set; }
    public Point Location { get; set; }
}
