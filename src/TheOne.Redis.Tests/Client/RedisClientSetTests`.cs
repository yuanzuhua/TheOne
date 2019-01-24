using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Common;
using TheOne.Redis.Tests.Shared;

namespace TheOne.Redis.Tests.Client {

    [TestFixture(typeof(CustomType), typeof(CustomTypeFactory))]
    [TestFixture(typeof(DateTime), typeof(DateTimeFactory))]
    [TestFixture(typeof(int), typeof(IntFactory))]
    [TestFixture(typeof(ModelWithFieldsOfDifferentTypes), typeof(ModelWithFieldsOfDifferentTypesFactory))]
    [TestFixture(typeof(Shipper), typeof(ShipperFactory))]
    [TestFixture(typeof(string), typeof(BuiltInsFactory))]
    internal sealed class RedisClientSetTests<T, TFactory> : RedisClientTestsBase where TFactory : class, IModelFactory<T>, new() {

        private const string _setId = "testset";
        private const string _setId2 = "testset2";
        private const string _setId3 = "testset3";
        private readonly IModelFactory<T> _factory = new TFactory();
        private IRedisTypedClient<T> _redis;
        private IRedisSet<T> _set;
        private IRedisSet<T> _set2;
        private IRedisSet<T> _set3;

        [SetUp]
        public override void SetUp() {
            base.SetUp();
            this._redis = this.Redis.As<T>();
            this._set = this._redis.Sets[_setId];
            this._set2 = this._redis.Sets[_setId2];
            this._set3 = this._redis.Sets[_setId3];
        }

        [Test]
        public void Can_Add_to_ICollection_Set() {
            var storeMembers = this._factory.CreateList();
            storeMembers.ForEach(x => this._redis.AddItemToSet(this._set, x));

            var members = this._set.ToList();
            Assert.That(members, Is.EquivalentTo(storeMembers));
        }

        [Test]
        public void Can_AddToSet_and_GetAllFromSet() {
            var storeMembers = this._factory.CreateList();
            storeMembers.ForEach(x => this._redis.AddItemToSet(this._set, x));

            var members = this._redis.GetAllItemsFromSet(this._set);
            Assert.That(members, Is.EquivalentTo(storeMembers));
        }

        [Test]
        public void Can_Clear_ICollection_Set() {
            var storeMembers = this._factory.CreateList();
            storeMembers.ForEach(x => this._redis.AddItemToSet(this._set, x));

            Assert.That(this._set.Count, Is.EqualTo(storeMembers.Count));

            this._set.Clear();

            Assert.That(this._set.Count, Is.EqualTo(0));
        }

        [Test]
        public void Can_DiffBetweenSets() {
            var storeMembers = this._factory.CreateList();
            storeMembers.Add(this._factory.CreateInstance(1));

            var storeMembers2 = this._factory.CreateList2();
            storeMembers2.Insert(0, this._factory.CreateInstance(4));

            var storeMembers3 = new List<T> {
                this._factory.CreateInstance(1),
                this._factory.CreateInstance(5),
                this._factory.CreateInstance(7),
                this._factory.CreateInstance(11)
            };

            storeMembers.ForEach(x => this._redis.AddItemToSet(this._set, x));
            storeMembers2.ForEach(x => this._redis.AddItemToSet(this._set2, x));
            storeMembers3.ForEach(x => this._redis.AddItemToSet(this._set3, x));

            var diffMembers = this._redis.GetDifferencesFromSet(this._set, this._set2, this._set3);

            Assert.That(diffMembers,
                Is.EquivalentTo(
                    new List<T> { this._factory.CreateInstance(2), this._factory.CreateInstance(3) }));
        }

        [Test]
        public void Can_enumerate_large_ICollection_Set() {
            const int setSize = 888;

            var storeMembers = new List<T>();
            for (var i = 0; i < setSize; i++) {
                this._redis.AddItemToSet(this._set, this._factory.CreateInstance(i));
                storeMembers.Add(this._factory.CreateInstance(i));
            }

            var members = new List<T>();
            foreach (var item in this._set) {
                members.Add(item);
            }

            members.Sort((x, y) => string.Compare(x.GetId().ToString(), y.GetId().ToString(), StringComparison.Ordinal));
            Assert.That(members.Count, Is.EqualTo(storeMembers.Count));
            Assert.That(members, Is.EquivalentTo(storeMembers));
        }


        [Test]
        public void Can_enumerate_small_ICollection_Set() {
            var storeMembers = this._factory.CreateList();
            storeMembers.ForEach(x => this._redis.AddItemToSet(this._set, x));

            var members = new List<T>();
            foreach (var item in this._set) {
                members.Add(item);
            }

            Assert.That(members.Count, Is.EqualTo(storeMembers.Count));
            Assert.That(members, Is.EquivalentTo(storeMembers));
        }

        [Test]
        public void Can_GetRandomEntryFromSet() {
            var storeMembers = this._factory.CreateList();
            storeMembers.ForEach(x => this._redis.AddItemToSet(this._set, x));

            var randomEntry = this._redis.GetRandomItemFromSet(this._set);

            Assert.That(storeMembers.Contains(randomEntry), Is.True);
        }

        [Test]
        public void Can_GetSetCount() {
            var storeMembers = this._factory.CreateList();
            storeMembers.ForEach(x => this._redis.AddItemToSet(this._set, x));

            var setCount = this._redis.GetSetCount(this._set);

            Assert.That(setCount, Is.EqualTo(storeMembers.Count));
        }

        [Test]
        public void Can_IntersectBetweenSets() {
            var storeMembers = this._factory.CreateList();
            var storeMembers2 = this._factory.CreateList2();

            storeMembers.Add(storeMembers2.First());
            storeMembers2.Add(storeMembers.First());

            storeMembers.ForEach(x => this._redis.AddItemToSet(this._set, x));
            storeMembers2.ForEach(x => this._redis.AddItemToSet(this._set2, x));

            var intersectingMembers = this._redis.GetIntersectFromSets(this._set, this._set2);

            var intersect = this._set.ToList().Intersect(this._set2.ToList()).ToList();

            Assert.That(intersectingMembers, Is.EquivalentTo(intersect));
        }

        [Test]
        public void Can_MoveBetweenSets() {
            var fromSetMembers = this._factory.CreateList();
            var toSetMembers = this._factory.CreateList2();

            fromSetMembers.ForEach(x => this._redis.AddItemToSet(this._set, x));
            toSetMembers.ForEach(x => this._redis.AddItemToSet(this._set2, x));

            this._redis.MoveBetweenSets(this._set, this._set2, this._factory.ExistingValue);

            fromSetMembers.Remove(this._factory.ExistingValue);
            toSetMembers.Add(this._factory.ExistingValue);

            var readFromSetId = this._redis.GetAllItemsFromSet(this._set);
            var readToSetId = this._redis.GetAllItemsFromSet(this._set2);

            Assert.That(readFromSetId, Is.EquivalentTo(fromSetMembers));
            Assert.That(readToSetId, Is.EquivalentTo(toSetMembers));
        }

        [Test]
        public void Can_PopFromSet() {
            var storeMembers = this._factory.CreateList();
            storeMembers.ForEach(x => this._redis.AddItemToSet(this._set, x));

            var member = this._redis.PopItemFromSet(this._set);

            Assert.That(storeMembers.Contains(member), Is.True);
        }

        [Test]
        public void Can_Remove_value_from_ICollection_Set() {
            var storeMembers = this._factory.CreateList();
            storeMembers.ForEach(x => this._redis.AddItemToSet(this._set, x));

            storeMembers.Remove(this._factory.ExistingValue);
            this._set.Remove(this._factory.ExistingValue);

            var members = this._set.ToList();

            Assert.That(members, Is.EquivalentTo(storeMembers));
        }

        [Test]
        public void Can_RemoveFromSet() {
            var storeMembers = this._factory.CreateList();

            storeMembers.ForEach(x => this._redis.AddItemToSet(this._set, x));

            this._redis.RemoveItemFromSet(this._set, this._factory.ExistingValue);

            storeMembers.Remove(this._factory.ExistingValue);

            var members = this._redis.GetAllItemsFromSet(this._set);
            Assert.That(members, Is.EquivalentTo(storeMembers));
        }

        [Test]
        public void Can_Store_DiffBetweenSets() {
            var storeMembers = this._factory.CreateList();
            storeMembers.Add(this._factory.CreateInstance(1));

            var storeMembers2 = this._factory.CreateList2();
            storeMembers2.Insert(0, this._factory.CreateInstance(4));

            var storeMembers3 = new List<T> {
                this._factory.CreateInstance(1),
                this._factory.CreateInstance(5),
                this._factory.CreateInstance(7),
                this._factory.CreateInstance(11)
            };

            storeMembers.ForEach(x => this._redis.AddItemToSet(this._set, x));
            storeMembers2.ForEach(x => this._redis.AddItemToSet(this._set2, x));
            storeMembers3.ForEach(x => this._redis.AddItemToSet(this._set3, x));

            var storeSet = this._redis.Sets["testdiffsetstore"];

            this._redis.StoreDifferencesFromSet(storeSet, this._set, this._set2, this._set3);

            Assert.That(storeSet,
                Is.EquivalentTo(
                    new List<T> { this._factory.CreateInstance(2), this._factory.CreateInstance(3) }));

        }

        [Test]
        public void Can_Store_IntersectBetweenSets() {
            var storeMembers = this._factory.CreateList();
            var storeMembers2 = this._factory.CreateList2();

            storeMembers.ForEach(x => this._redis.AddItemToSet(this._set, x));
            storeMembers2.ForEach(x => this._redis.AddItemToSet(this._set2, x));

            this._redis.StoreIntersectFromSets(this._set3, this._set, this._set2);

            var intersect = this._set.ToList().Intersect(this._set2).ToList();

            Assert.That(this._set3, Is.EquivalentTo(intersect));
        }

        [Test]
        public void Can_Store_UnionBetweenSets() {
            var storeMembers = this._factory.CreateList();
            var storeMembers2 = this._factory.CreateList2();

            storeMembers.ForEach(x => this._redis.AddItemToSet(this._set, x));
            storeMembers2.ForEach(x => this._redis.AddItemToSet(this._set2, x));

            this._redis.StoreUnionFromSets(this._set3, this._set, this._set2);

            var union = this._set.ToList().Union(this._set2).ToList();

            Assert.That(this._set3, Is.EquivalentTo(union));
        }

        [Test]
        public void Can_Test_Contains_in_ICollection_Set() {
            var storeMembers = this._factory.CreateList();
            storeMembers.ForEach(x => this._redis.AddItemToSet(this._set, x));

            Assert.That(this._set.Contains(this._factory.ExistingValue), Is.True);
            Assert.That(this._set.Contains(this._factory.NonExistingValue), Is.False);
        }

        [Test]
        public void Can_UnionBetweenSets() {
            var storeMembers = this._factory.CreateList();
            var storeMembers2 = this._factory.CreateList2();

            storeMembers.ForEach(x => this._redis.AddItemToSet(this._set, x));
            storeMembers2.ForEach(x => this._redis.AddItemToSet(this._set2, x));

            var unionMembers = this._redis.GetUnionFromSets(this._set, this._set2);

            var union = this._set.Union(this._set2).ToList();

            Assert.That(unionMembers, Is.EquivalentTo(union));
        }

        [Test]
        public void Does_SetContainsValue() {
            var storeMembers = this._factory.CreateList();
            storeMembers.ForEach(x => this._redis.AddItemToSet(this._set, x));

            Assert.That(this._redis.SetContainsItem(this._set, this._factory.ExistingValue), Is.True);
            Assert.That(this._redis.SetContainsItem(this._set, this._factory.NonExistingValue), Is.False);
        }

    }

}
