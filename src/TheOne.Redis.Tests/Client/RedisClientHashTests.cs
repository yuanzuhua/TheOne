using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Common;
using TheOne.Redis.Tests.Extensions;

namespace TheOne.Redis.Tests.Client {

    [TestFixture]
    internal sealed class RedisClientHashTests : RedisClientTestsBase {

        #region Models

        public class HashTest {

            public int Id { get; set; }
            public string Name { get; set; }

        }

        #endregion

        private const string _hashId = "rchtesthash";
        private Dictionary<string, int> _stringIntMap;

        private Dictionary<string, string> _stringMap;

        private static Dictionary<string, string> ToStringMap(Dictionary<string, int> stringIntMap) {
            var map = new Dictionary<string, string>();
            foreach (KeyValuePair<string, int> kvp in stringIntMap) {
                map[kvp.Key] = kvp.Value.ToString();
            }

            return map;
        }

        public override void SetUp() {
            base.SetUp();
            this._stringMap = new Dictionary<string, string> {
                { "one", "a" },
                { "two", "b" },
                { "three", "c" },
                { "four", "d" }
            };
            this._stringIntMap = new Dictionary<string, int> {
                { "one", 1 },
                { "two", 2 },
                { "three", 3 },
                { "four", 4 }
            };
        }

        [Test]
        public void Can_Add_to_IDictionary_Hash() {
            IRedisHash hash = this.Redis.Hashes[_hashId];
            foreach (var key in this._stringMap.Keys) {
                hash.Add(key, this._stringMap[key]);
            }

            Dictionary<string, string> members = this.Redis.GetAllEntriesFromHash(_hashId);
            Assert.That(members, Is.EquivalentTo(this._stringMap));
        }

        [Test]
        public void Can_Clear_IDictionary_Hash() {
            IRedisHash hash = this.Redis.Hashes[_hashId];
            foreach (var key in this._stringMap.Keys) {
                hash.Add(key, this._stringMap[key]);
            }

            Assert.That(hash.Count, Is.EqualTo(this._stringMap.Count));

            hash.Clear();

            Assert.That(hash.Count, Is.EqualTo(0));
        }

        [Test]
        public void Can_enumerate_small_IDictionary_Hash() {
            foreach (KeyValuePair<string, string> value in this._stringMap) {
                this.Redis.SetEntryInHash(_hashId, value.Key, value.Value);
            }

            var members = new List<string>();
            foreach (KeyValuePair<string, string> item in this.Redis.Hashes[_hashId]) {
                Assert.That(this._stringMap.ContainsKey(item.Key), Is.True);
                members.Add(item.Key);
            }

            Assert.That(members.Count, Is.EqualTo(this._stringMap.Count));
        }

        [Test]
        public void Can_GetHashCount() {
            foreach (KeyValuePair<string, string> value in this._stringMap) {
                this.Redis.SetEntryInHash(_hashId, value.Key, value.Value);
            }

            var hashCount = this.Redis.GetHashCount(_hashId);

            Assert.That(hashCount, Is.EqualTo(this._stringMap.Count));
        }

        [Test]
        public void Can_GetHashKeys() {
            foreach (KeyValuePair<string, string> value in this._stringMap) {
                this.Redis.SetEntryInHash(_hashId, value.Key, value.Value);
            }

            List<string> expectedKeys = this._stringMap.Select(x => x.Key).ToList();

            List<string> hashKeys = this.Redis.GetHashKeys(_hashId);

            Assert.That(hashKeys, Is.EquivalentTo(expectedKeys));
        }

        [Test]
        public void Can_GetHashValues() {
            foreach (KeyValuePair<string, string> value in this._stringMap) {
                this.Redis.SetEntryInHash(_hashId, value.Key, value.Value);
            }

            List<string> expectedValues = this._stringMap.Select(x => x.Value).ToList();

            List<string> hashValues = this.Redis.GetHashValues(_hashId);

            Assert.That(hashValues, Is.EquivalentTo(expectedValues));
        }

        [Test]
        public void Can_GetItemFromHash() {
            foreach (KeyValuePair<string, string> value in this._stringMap) {
                this.Redis.SetEntryInHash(_hashId, value.Key, value.Value);
            }

            var hashValue = this.Redis.GetValueFromHash(_hashId, "two");

            Assert.That(hashValue, Is.EqualTo(this._stringMap["two"]));
        }

        [Test]
        public void Can_GetItemsFromHash() {
            foreach (KeyValuePair<string, string> value in this._stringMap) {
                this.Redis.SetEntryInHash(_hashId, value.Key, value.Value);
            }

            var expectedValues = new List<string> { this._stringMap["one"], this._stringMap["two"], null };
            List<string> hashValues = this.Redis.GetValuesFromHash(_hashId, "one", "two", "not-exists");

            Assert.That(hashValues.EquivalentTo(expectedValues), Is.True);
        }

        [Test]
        public void Can_hash_multi_set_and_get() {
            const string key = _hashId + "multitest";
            Assert.That(this.Redis.GetValue(key), Is.Null);
            var fields = new Dictionary<string, string> { { "field1", "1" }, { "field2", "2" }, { "field3", "3" } };

            this.Redis.SetRangeInHash(key, fields);
            Dictionary<string, string> members = this.Redis.GetAllEntriesFromHash(key);
            foreach (KeyValuePair<string, string> member in members) {
                Assert.IsTrue(fields.ContainsKey(member.Key));
                Assert.AreEqual(fields[member.Key], member.Value);
            }
        }

        [Test]
        public void Can_hash_set() {
            var key = _hashId + "key";
            byte[] field = Encoding.UTF8.GetBytes("foo");
            byte[] value = Encoding.UTF8.GetBytes("value");
            Assert.AreEqual(this.Redis.HDel(key, field), 0);
            Assert.AreEqual(this.Redis.HSet(key, field, value), 1);
            Assert.AreEqual(this.Redis.HDel(key, field), 1);
        }

        [Test]
        public void Can_increment_Hash_field() {
            IRedisHash hash = this.Redis.Hashes[_hashId];
            foreach (KeyValuePair<string, int> value in this._stringIntMap) {
                hash.Add(value.Key, value.Value.ToString());
            }

            this._stringIntMap["two"] += 10;
            this.Redis.IncrementValueInHash(_hashId, "two", 10);

            Dictionary<string, string> members = this.Redis.GetAllEntriesFromHash(_hashId);
            Assert.That(members, Is.EquivalentTo(ToStringMap(this._stringIntMap)));
        }

        [Test]
        public void Can_increment_Hash_field_beyond_32_bits() {
            this.Redis.SetEntryInHash(_hashId, "int", int.MaxValue.ToString());
            this.Redis.IncrementValueInHash(_hashId, "int", 1);
            var actual = long.Parse(this.Redis.GetValueFromHash(_hashId, "int"));
            var expected = int.MaxValue + 1L;
            Assert.That(actual, Is.EqualTo(expected));
        }

        [Test]
        public void Can_Remove_value_from_IDictionary_Hash() {
            IRedisHash hash = this.Redis.Hashes[_hashId];
            foreach (var key in this._stringMap.Keys) {
                hash.Add(key, this._stringMap[key]);
            }

            this._stringMap.Remove("two");
            hash.Remove("two");

            Dictionary<string, string> members = this.Redis.GetAllEntriesFromHash(_hashId);
            Assert.That(members, Is.EquivalentTo(this._stringMap));
        }

        [Test]
        public void Can_RemoveFromHash() {
            const string removeMember = "two";

            foreach (KeyValuePair<string, string> value in this._stringMap) {
                this.Redis.SetEntryInHash(_hashId, value.Key, value.Value);
            }

            this.Redis.RemoveEntryFromHash(_hashId, removeMember);

            this._stringMap.Remove(removeMember);

            Dictionary<string, string> members = this.Redis.GetAllEntriesFromHash(_hashId);
            Assert.That(members, Is.EquivalentTo(this._stringMap));
        }

        [Test]
        public void Can_SetItemInHash_and_GetAllFromHash() {
            foreach (KeyValuePair<string, string> value in this._stringMap) {
                this.Redis.SetEntryInHash(_hashId, value.Key, value.Value);
            }

            Dictionary<string, string> members = this.Redis.GetAllEntriesFromHash(_hashId);
            Assert.That(members, Is.EquivalentTo(this._stringMap));
        }

        [Test]
        public void Can_SetItemInHashIfNotExists() {
            foreach (KeyValuePair<string, string> value in this._stringMap) {
                this.Redis.SetEntryInHash(_hashId, value.Key, value.Value);
            }

            this.Redis.SetEntryInHashIfNotExists(_hashId, "two", "did not change existing item");
            this.Redis.SetEntryInHashIfNotExists(_hashId, "five", "changed non existing item");
            this._stringMap["five"] = "changed non existing item";

            Dictionary<string, string> members = this.Redis.GetAllEntriesFromHash(_hashId);
            Assert.That(members, Is.EquivalentTo(this._stringMap));
        }

        [Test]
        public void Can_SetRangeInHash() {
            var newStringMap = new Dictionary<string, string> {
                { "five", "e" },
                { "six", "f" },
                { "seven", "g" }
            };
            foreach (KeyValuePair<string, string> value in this._stringMap) {
                this.Redis.SetEntryInHash(_hashId, value.Key, value.Value);
            }

            this.Redis.SetRangeInHash(_hashId, newStringMap);

            foreach (KeyValuePair<string, string> value1 in newStringMap) {
                this._stringMap.Add(value1.Key, value1.Value);
            }

            Dictionary<string, string> members = this.Redis.GetAllEntriesFromHash(_hashId);
            Assert.That(members, Is.EquivalentTo(this._stringMap));
        }

        [Test]
        public void Can_store_as_Hash() {
            var dto = new HashTest { Id = 1 };
            this.Redis.StoreAsHash(dto);

            List<string> storedHash = this.Redis.GetHashKeys(dto.ToUrn());

            Assert.That(storedHash, Is.EquivalentTo(new[] { "Id" }));

            Func<object, Dictionary<string, string>> hold = RedisClient.ConvertToHashFn;

            RedisClient.ConvertToHashFn = o => {
                var dictionary = JObject.FromObject(o).ToObject<Dictionary<string, string>>();
                var dict = new Dictionary<string, string>();
                foreach (KeyValuePair<string, string> pair in dictionary) {
                    dict[pair.Key] = pair.Value ?? "";
                }

                return dict;
            };

            this.Redis.StoreAsHash(dto);
            storedHash = this.Redis.GetHashKeys(dto.ToUrn());
            Assert.That(storedHash, Is.EquivalentTo(new[] { "Id", "Name" }));

            RedisClient.ConvertToHashFn = hold;
        }

        [Test]
        public void Can_Test_Contains_in_IDictionary_Hash() {
            IRedisHash hash = this.Redis.Hashes[_hashId];
            foreach (var key in this._stringMap.Keys) {
                hash.Add(key, this._stringMap[key]);
            }

            Assert.That(hash.ContainsKey("two"), Is.True);
            Assert.That(hash.ContainsKey("five"), Is.False);
        }

        [Test]
        public void Does_HashContainsKey() {
            const string existingMember = "two";
            const string nonExistingMember = "five";

            foreach (KeyValuePair<string, string> value in this._stringMap) {
                this.Redis.SetEntryInHash(_hashId, value.Key, value.Value);
            }

            Assert.That(this.Redis.HashContainsEntry(_hashId, existingMember), Is.True);
            Assert.That(this.Redis.HashContainsEntry(_hashId, nonExistingMember), Is.False);
        }

    }

}
