using System;
using System.Linq.Expressions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using N.EntityFrameworkCore.Extensions.Test.Data;

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

        Assert.AreEqual("s.Type = t.Type AND s.Id = t.Id AND s.ExternalId = t.ExternalId AND (s.TesterVar1 = t.TesterVar1 OR (s.TesterVar1 IS NULL AND t.TesterVar1 IS NULL)) AND (s.TesterVar2 = t.TesterVar2 OR (s.TesterVar2 IS NULL AND t.TesterVar2 IS NULL)) AND (s.TesterVar3 = t.TesterVar3 OR (s.TesterVar3 IS NULL AND t.TesterVar3 IS NULL)) AND (s.TesterVar4 = t.TesterVar4 OR (s.TesterVar4 IS NULL AND t.TesterVar4 IS NULL)) AND (s.TesterVar5 = t.TesterVar5 OR (s.TesterVar5 IS NULL AND t.TesterVar5 IS NULL))", sqlPredicate);
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

        Assert.AreEqual("s.Type = t.Type AND s.Id = t.Id AND (s.ExternalId = t.ExternalId OR (s.TesterVar1 = t.TesterVar1 OR (s.TesterVar1 IS NULL AND t.TesterVar1 IS NULL))) AND ((s.TesterVar2 = t.TesterVar2 OR (s.TesterVar2 IS NULL AND t.TesterVar2 IS NULL)) OR s.TesterVar2 IS NULL AND t.TesterVar2 IS NULL) AND ((s.TesterVar3 = t.TesterVar3 OR (s.TesterVar3 IS NULL AND t.TesterVar3 IS NULL)) OR s.TesterVar3 IS NOT NULL AND t.TesterVar3 IS NOT NULL)", sqlPredicate);
    }

    [TestMethod]
    public void Should_handle_nullable_column_equality()
    {
        Expression<Func<Entity, Entity, bool>> expression = (s, t) => s.ExternalId == t.ExternalId && s.OptionalCode == t.OptionalCode;

        var sqlPredicate = expression.ToSqlPredicate("s", "t");

        Assert.AreEqual(
            "s.ExternalId = t.ExternalId AND (s.OptionalCode = t.OptionalCode OR (s.OptionalCode IS NULL AND t.OptionalCode IS NULL))",
            sqlPredicate);
    }

    [TestMethod]
    public void Should_handle_nullable_guid_equality()
    {
        Expression<Func<Entity, Entity, bool>> expression = (s, t) => s.GlobalId == t.GlobalId;

        var sqlPredicate = expression.ToSqlPredicate("s", "t");

        Assert.AreEqual("(s.GlobalId = t.GlobalId OR (s.GlobalId IS NULL AND t.GlobalId IS NULL))", sqlPredicate);
    }

    [TestMethod]
    public void Should_delimit_reserved_keyword_members()
    {
        Expression<Func<ReservedEntity, ReservedEntity, bool>> expression = (s, t) => s.Order == t.Order && s.Index == t.Index;
        using var dbContext = new TestDbContext();

        var sqlPredicate = expression.ToSqlPredicate(dbContext, "s", "t");

        Assert.AreEqual("[s].[Order] = [t].[Order] AND [s].[Index] = [t].[Index]", sqlPredicate);
    }

    [TestMethod]
    public void Should_handle_nested_member_access()
    {
        Expression<Func<NestedEntity, NestedEntity, bool>> expression = (s, t) => s.Address.City == t.Address.City && s.Address.Zip == t.Address.Zip;

        var sqlPredicate = expression.ToSqlPredicate("s", "t");

        Assert.AreEqual(
            "(s.Address.City = t.Address.City OR (s.Address.City IS NULL AND t.Address.City IS NULL)) AND (s.Address.Zip = t.Address.Zip OR (s.Address.Zip IS NULL AND t.Address.Zip IS NULL))",
            sqlPredicate);
    }

    [TestMethod]
    public void Should_handle_string_equals_method()
    {
        Expression<Func<Entity, Entity, bool>> expression = (s, t) => string.Equals(s.TesterVar1, t.TesterVar1);

        var sqlPredicate = expression.ToSqlPredicate("s", "t");

        Assert.AreEqual("(s.TesterVar1 = t.TesterVar1 OR (s.TesterVar1 IS NULL AND t.TesterVar1 IS NULL))", sqlPredicate);
    }

    [TestMethod]
    public void Should_handle_boolean_member_shorthand()
    {
        Expression<Func<FlagEntity, FlagEntity, bool>> expression = (s, t) => s.IsActive && t.IsActive;

        var sqlPredicate = expression.ToSqlPredicate("s", "t");

        Assert.AreEqual("s.IsActive AND t.IsActive", sqlPredicate);
    }

    [TestMethod]
    public void Should_handle_enum_constant_comparison()
    {
        Expression<Func<Entity, Entity, bool>> expression = (s, t) => s.Type == EntityType.One && t.Type == EntityType.Two;

        var sqlPredicate = expression.ToSqlPredicate("s", "t");

        Assert.AreEqual("s.Type = 0 AND t.Type = 1", sqlPredicate);
    }

    record NestedEntity
    {
        public Address Address { get; set; }
    }

    record Address
    {
        public string City { get; set; }
        public string Zip { get; set; }
    }

    record FlagEntity
    {
        public bool IsActive { get; set; }
    }

    record Entity
    {
        public Guid Id { get; set; }
        public Guid? GlobalId { get; set; }
        public EntityType Type { get; set; }
        public int ExternalId { get; set; }
        public string TesterVar1 { get; set; }
        public string TesterVar2 { get; set; }
        public string TesterVar3 { get; set; }
        public string TesterVar4 { get; set; }
        public string TesterVar5 { get; set; }
        public string OptionalCode { get; set; }
    }

    record ReservedEntity
    {
        public int Order { get; set; }
        public int Index { get; set; }
    }

    enum EntityType
    {
        One,
        Two,
        Three
    }
}