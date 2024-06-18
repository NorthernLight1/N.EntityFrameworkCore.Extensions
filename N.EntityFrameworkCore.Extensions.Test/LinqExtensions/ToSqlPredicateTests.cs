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

        Assert.AreEqual("s.Id = t.Id", sqlPredicate);
    }

    [TestMethod]
    public void Should_handle_enum()
    {
        Expression<Func<Entity, Entity, bool>> expression = (s, t) => s.Type == t.Type;

        var sqlPredicate = expression.ToSqlPredicate("s", "t");

        Assert.AreEqual("s.Type = t.Type", sqlPredicate);
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

    [TestMethod]
    public void Should_handle_simple_big_one()
    {
        Expression<Func<Entity, Entity, bool>> expression = (s, t) => s.Type == t.Type &&
                                                                      s.Id == t.Id &&
                                                                      s.ExternalId == t.ExternalId &&
                                                                      s.TesterVar1 == t.TesterVar1 &&
                                                                      s.TesterVar2 == t.TesterVar2 &&
                                                                      s.TesterVar3 == t.TesterVar3 &&
                                                                      s.TesterVar4 == t.TesterVar4 &&
                                                                      s.TesterVar5 == t.TesterVar5;

        var sqlPredicate = expression.ToSqlPredicate("s", "t");

        Assert.AreEqual("s.Type = t.Type AND s.Id = t.Id AND s.ExternalId = t.ExternalId AND s.TesterVar1 = t.TesterVar1 AND s.TesterVar2 = t.TesterVar2 AND s.TesterVar3 = t.TesterVar3 AND s.TesterVar4 = t.TesterVar4 AND s.TesterVar5 = t.TesterVar5", sqlPredicate);
    }

    [TestMethod]
    public void Should_handle_complex_big_one()
    {
        Expression<Func<Entity, Entity, bool>> expression = (s, t) => s.Type == t.Type &&
                                                                      s.Id == t.Id &&
                                                                      (s.ExternalId == t.ExternalId || s.TesterVar1 == t.TesterVar1) &&
                                                                      (s.TesterVar2 == t.TesterVar2 || (s.TesterVar2 == null && t.TesterVar2 == null)) &&
                                                                      (s.TesterVar3 == t.TesterVar3 || (s.TesterVar3 != null && t.TesterVar3 != null));

        var sqlPredicate = expression.ToSqlPredicate("s", "t");

        Assert.AreEqual("s.Type = t.Type AND s.Id = t.Id AND (s.ExternalId = t.ExternalId OR s.TesterVar1 = t.TesterVar1) AND (s.TesterVar2 = t.TesterVar2 OR s.TesterVar2 IS NULL AND t.TesterVar2 IS NULL) AND (s.TesterVar3 = t.TesterVar3 OR s.TesterVar3 IS NOT NULL AND t.TesterVar3 IS NOT NULL)", sqlPredicate);
    }

    record Entity
    {
        public Guid Id { get; set; }
        public EntityType Type { get; set; }
        public int ExternalId { get; set; }
        public string TesterVar1 { get; set; }
        public string TesterVar2 { get; set; }
        public string TesterVar3 { get; set; }
        public string TesterVar4 { get; set; }
        public string TesterVar5 { get; set; }
    }
    
    enum EntityType
    {
        One,
        Two,
        Three
    }
}