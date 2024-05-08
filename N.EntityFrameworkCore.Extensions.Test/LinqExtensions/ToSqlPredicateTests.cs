using System;
using System.Linq.Expressions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace N.EntityFrameworkCore.Extensions.Test.LinqExtensions;

[TestClass]
public class ToSqlPredicateTests
{
    [TestMethod]
    public void Should_handle_int()
    {
        Expression<Func<Entity, Entity, bool>> expression = (s, t) => s.Id == t.Id;

        var sqlPredicate = expression.ToSqlPredicate("s", "t");

        Assert.AreEqual("s.Id = t.Id",sqlPredicate);
    }
    
    [TestMethod]
    public void Should_handle_enum()
    {
        Expression<Func<Entity, Entity, bool>> expression = (s, t) => s.Type == t.Type;

        var sqlPredicate = expression.ToSqlPredicate("s", "t");

        Assert.AreEqual("s.Type = t.Type",sqlPredicate);
    }

    [TestMethod]
    public void Should_handle_complex_one()
    {
        Expression<Func<Entity, Entity, bool>> expression = (s, t) => s.Type == t.Type &&
                                                                      (s.Id == t.Id &&
                                                                       s.ExternalId == t.ExternalId);

        var sqlPredicate = expression.ToSqlPredicate("s", "t");

        Assert.AreEqual("s.Type = t.Type AND s.Id = t.Id AND s.ExternalId = t.ExternalId", sqlPredicate);
    }

    [TestMethod]
    public void Should_handle_prop_naming()
    {
        Expression<Func<Entity, Entity, bool>> expression = (source, target) => source.Id == target.Id &&
                                                                                source.ExternalId == target.ExternalId;

        var sqlPredicate = expression.ToSqlPredicate("s", "t");

        Assert.AreEqual("s.Id = t.Id AND s.ExternalId = t.ExternalId", sqlPredicate);
    }
    
    record Entity
    {
        public Guid Id { get; set; }
        public EntityType Type { get; set; }
        public int ExternalId { get; set; }
    }

    enum EntityType
    {
        One,
        Two,
        Three
    }
}
