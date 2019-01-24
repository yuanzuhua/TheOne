using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Tests.Shared;

namespace TheOne.Redis.Tests.Client {

    [TestFixture(typeof(CustomType), typeof(CustomTypeFactory))]
    [TestFixture(typeof(DateTime), typeof(DateTimeFactory))]
    [TestFixture(typeof(int), typeof(IntFactory))]
    [TestFixture(typeof(ModelWithFieldsOfDifferentTypes), typeof(ModelWithFieldsOfDifferentTypesFactory))]
    [TestFixture(typeof(Shipper), typeof(ShipperFactory))]
    [TestFixture(typeof(string), typeof(BuiltInsFactory))]
    internal sealed class RedisClientHashTests<T, TFactory> : RedisClientTestsBase where TFactory : class, IModelFactory<T>, new() {

        private const string _hashId = "testhash";
        private readonly IModelFactory<T> _factory = new TFactory();
        private IRedisHash<string, T> _hash;
        private IRedisTypedClient<T> _redis;

        private Dictionary<string, T> CreateMap() {
            var listValues = this._factory.CreateList();
            var map = new Dictionary<string, T>();
            listValues.ForEach(x => map[x.ToString()] = x);
            return map;
        }

        private Dictionary<string, T> CreateMap2() {
            var listValues = this._factory.CreateList2();
            var map = new Dictionary<string, T>();
            listValues.ForEach(x => map[x.ToString()] = x);
            return map;
        }

        [SetUp]
        public override void SetUp() {
            base.SetUp();
            this._redis = this.Redis.As<T>();
            this._hash = this._redis.GetHash<string>(_hashId);
        }

        [Test]
        public void Can_Add_to_IDictionary_Hash() {
            var hash = this._redis.GetHash<string>(_hashId);
            var mapValues = this.CreateMap();
            foreach (var entry in mapValues) {
                hash.Add(entry.Key, entry.Value);
            }

            var members = this._redis.GetAllEntriesFromHash(this._hash);
            Assert.That(members, Is.EquivalentTo(mapValues));
        }

        [Test]
        public void Can_Clear_IDictionary_Hash() {
            var hash = this._redis.GetHash<string>(_hashId);
            var mapValues = this.CreateMap();
            foreach (var entry in mapValues) {
                hash.Add(entry.Key, entry.Value);
            }

            Assert.That(hash.Count, Is.EqualTo(mapValues.Count));

            hash.Clear();

            Assert.That(hash.Count, Is.EqualTo(0));
        }

        [Test]
        public void Can_enumerate_small_IDictionary_Hash() {
            var mapValues = this.CreateMap();
            foreach (var entry in mapValues) {
                ((Action<string, T>)((k, v) => this._redis.SetEntryInHash(this._hash, k, v)))(entry.Key, entry.Value);
            }

            var members = new List<string>();
            foreach (var item in this._redis.GetHash<string>(_hashId)) {
                Assert.That(mapValues.ContainsKey(item.Key), Is.True);
                members.Add(item.Key);
            }

            Assert.That(members.Count, Is.EqualTo(mapValues.Count));
        }

        [Test]
        public void Can_GetHashCount() {
            var mapValues = this.CreateMap();
            foreach (var entry in mapValues) {
                this._redis.SetEntryInHash(this._hash, entry.Key, entry.Value);
            }

            var hashCount = this._redis.GetHashCount(this._hash);

            Assert.That(hashCount, Is.EqualTo(mapValues.Count));
        }

        [Test]
        public void Can_GetHashKeys() {
            var mapValues = this.CreateMap();
            foreach (var entry in mapValues) {
                this._redis.SetEntryInHash(this._hash, entry.Key, entry.Value);
            }

            var expectedKeys = mapValues.Select(x => x.Key).ToList();

            var hashKeys = this._redis.GetHashKeys(this._hash);

            Assert.That(hashKeys, Is.EquivalentTo(expectedKeys));
        }

        [Test]
        public void Can_GetHashValues() {
            var mapValues = this.CreateMap();
            foreach (var entry in mapValues) {
                this._redis.SetEntryInHash(this._hash, entry.Key, entry.Value);
            }

            var expectedValues = mapValues.Select(x => x.Value).ToList();

            var hashValues = this._redis.GetHashValues(this._hash);

            Assert.That(hashValues, Is.EquivalentTo(expectedValues));
        }

        [Test]
        public void Can_GetItemFromHash() {
            var mapValues = this.CreateMap();
            foreach (var entry in mapValues) {
                this._redis.SetEntryInHash(this._hash, entry.Key, entry.Value);
            }

            var firstKey = mapValues.First().Key;

            var hashValue = this._redis.GetValueFromHash(this._hash, firstKey);

            Assert.That(hashValue, Is.EqualTo(mapValues[firstKey]));
        }

        [Test]
        public void Can_Remove_value_from_IDictionary_Hash() {
            var hash = this._redis.GetHash<string>(_hashId);
            var mapValues = this.CreateMap();
            foreach (var entry in mapValues) {
                hash.Add(entry.Key, entry.Value);
            }

            var firstKey = mapValues.First().Key;
            mapValues.Remove(firstKey);
            hash.Remove(firstKey);

            var members = this._redis.GetAllEntriesFromHash(this._hash);
            Assert.That(members, Is.EquivalentTo(mapValues));
        }

        [Test]
        public void Can_RemoveFromHash() {
            var mapValues = this.CreateMap();
            foreach (var entry in mapValues) {
                this._redis.SetEntryInHash(this._hash, entry.Key, entry.Value);
            }

            var firstKey = mapValues.First().Key;

            this._redis.RemoveEntryFromHash(this._hash, firstKey);

            mapValues.Remove(firstKey);

            var members = this._redis.GetAllEntriesFromHash(this._hash);
            Assert.That(members, Is.EquivalentTo(mapValues));
        }

        [Test]
        public void Can_SetItemInHash_and_GetAllFromHash() {
            var mapValues = this.CreateMap();
            foreach (var entry in mapValues) {
                this._redis.SetEntryInHash(this._hash, entry.Key, entry.Value);
            }

            var members = this._redis.GetAllEntriesFromHash(this._hash);
            Assert.That(members, Is.EquivalentTo(mapValues));
        }

        [Test]
        public void Can_SetItemInHashIfNotExists() {
            var mapValues = this.CreateMap();
            foreach (var entry in mapValues) {
                this._redis.SetEntryInHash(this._hash, entry.Key, entry.Value);
            }

            var existingMember = mapValues.First().Key;
            var nonExistingMember = existingMember + "notexists";

            var lastValue = mapValues.Last().Value;

            this._redis.SetEntryInHashIfNotExists(this._hash, existingMember, lastValue);
            this._redis.SetEntryInHashIfNotExists(this._hash, nonExistingMember, lastValue);

            mapValues[nonExistingMember] = lastValue;

            var members = this._redis.GetAllEntriesFromHash(this._hash);
            Assert.That(members, Is.EquivalentTo(mapValues));
        }

        [Test]
        public void Can_SetRangeInHash() {
            var mapValues = this.CreateMap();
            foreach (var entry in mapValues) {
                this._redis.SetEntryInHash(this._hash, entry.Key, entry.Value);
            }

            var newMapValues = this.CreateMap2();

            this._redis.SetRangeInHash(this._hash, newMapValues);

            foreach (var value in newMapValues) {
                mapValues[value.Key] = value.Value;
            }

            var members = this._redis.GetAllEntriesFromHash(this._hash);
            Assert.That(members, Is.EquivalentTo(mapValues));
        }

        [Test]
        public void Can_Test_Contains_in_IDictionary_Hash() {
            var hash = this._redis.GetHash<string>(_hashId);
            var mapValues = this.CreateMap();
            foreach (var entry in mapValues) {
                hash.Add(entry.Key, entry.Value);
            }

            var existingMember = mapValues.First().Key;
            var nonExistingMember = existingMember + "notexists";

            Assert.That(hash.ContainsKey(existingMember), Is.True);
            Assert.That(hash.ContainsKey(nonExistingMember), Is.False);
        }

        [Test]
        public void Does_HashContainsKey() {
            var mapValues = this.CreateMap();
            foreach (var entry in mapValues) {
                this._redis.SetEntryInHash(this._hash, entry.Key, entry.Value);
            }

            var existingMember = mapValues.First().Key;
            var nonExistingMember = existingMember + "notexists";

            Assert.That(this._redis.HashContainsEntry(this._hash, existingMember), Is.True);
            Assert.That(this._redis.HashContainsEntry(this._hash, nonExistingMember), Is.False);
        }

    }

}
