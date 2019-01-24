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
            foreach (var kvp in stringIntMap) {
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
            var hash = this.Redis.Hashes[_hashId];
            foreach (var key in this._stringMap.Keys) {
                hash.Add(key, this._stringMap[key]);
            }

            var members = this.Redis.GetAllEntriesFromHash(_hashId);
            Assert.That(members, Is.EquivalentTo(this._stringMap));
        }

        [Test]
        public void Can_Clear_IDictionary_Hash() {
            var hash = this.Redis.Hashes[_hashId];
            foreach (var key in this._stringMap.Keys) {
                hash.Add(key, this._stringMap[key]);
            }

            Assert.That(hash.Count, Is.EqualTo(this._stringMap.Count));

            hash.Clear();

            Assert.That(hash.Count, Is.EqualTo(0));
        }

        [Test]
        public void Can_enumerate_small_IDictionary_Hash() {
            foreach (var value in this._stringMap) {
                this.Redis.SetEntryInHash(_hashId, value.Key, value.Value);
            }

            var members = new List<string>();
            foreach (var item in this.Redis.Hashes[_hashId]) {
                Assert.That(this._stringMap.ContainsKey(item.Key), Is.True);
                members.Add(item.Key);
            }

            Assert.That(members.Count, Is.EqualTo(this._stringMap.Count));
        }

        [Test]
        public void Can_GetHashCount() {
            foreach (var value in this._stringMap) {
                this.Redis.SetEntryInHash(_hashId, value.Key, value.Value);
            }

            var hashCount = this.Redis.GetHashCount(_hashId);

            Assert.That(hashCount, Is.EqualTo(this._stringMap.Count));
        }

        [Test]
        public void Can_GetHashKeys() {
            foreach (var value in this._stringMap) {
                this.Redis.SetEntryInHash(_hashId, value.Key, value.Value);
            }

            var expectedKeys = this._stringMap.Select(x => x.Key).ToList();

            var hashKeys = this.Redis.GetHashKeys(_hashId);

            Assert.That(hashKeys, Is.EquivalentTo(expectedKeys));
        }

        [Test]
        public void Can_GetHashValues() {
            foreach (var value in this._stringMap) {
                this.Redis.SetEntryInHash(_hashId, value.Key, value.Value);
            }

            var expectedValues = this._stringMap.Select(x => x.Value).ToList();

            var hashValues = this.Redis.GetHashValues(_hashId);

            Assert.That(hashValues, Is.EquivalentTo(expectedValues));
        }

        [Test]
        public void Can_GetItemFromHash() {
            foreach (var value in this._stringMap) {
                this.Redis.SetEntryInHash(_hashId, value.Key, value.Value);
            }

            var hashValue = this.Redis.GetValueFromHash(_hashId, "two");

            Assert.That(hashValue, Is.EqualTo(this._stringMap["two"]));
        }

        [Test]
        public void Can_GetItemsFromHash() {
            foreach (var value in this._stringMap) {
                this.Redis.SetEntryInHash(_hashId, value.Key, value.Value);
            }

            var expectedValues = new List<string> { this._stringMap["one"], this._stringMap["two"], null };
            var hashValues = this.Redis.GetValuesFromHash(_hashId, "one", "two", "not-exists");

            Assert.That(hashValues.EquivalentTo(expectedValues), Is.True);
        }

        [Test]
        public void Can_hash_multi_set_and_get() {
            const string key = _hashId + "multitest";
            Assert.That(this.Redis.GetValue(key), Is.Null);
            var fields = new Dictionary<string, string> { { "field1", "1" }, { "field2", "2" }, { "field3", "3" } };

            this.Redis.SetRangeInHash(key, fields);
            var members = this.Redis.GetAllEntriesFromHash(key);
            foreach (var member in members) {
                Assert.IsTrue(fields.ContainsKey(member.Key));
                Assert.AreEqual(fields[member.Key], member.Value);
            }
        }

        [Test]
        public void Can_hash_set() {
            var key = _hashId + "key";
            var field = Encoding.UTF8.GetBytes("foo");
            var value = Encoding.UTF8.GetBytes("value");
            Assert.AreEqual(this.Redis.HDel(key, field), 0);
            Assert.AreEqual(this.Redis.HSet(key, field, value), 1);
            Assert.AreEqual(this.Redis.HDel(key, field), 1);
        }

        [Test]
        public void Can_increment_Hash_field() {
            var hash = this.Redis.Hashes[_hashId];
            foreach (var value in this._stringIntMap) {
                hash.Add(value.Key, value.Value.ToString());
            }

            this._stringIntMap["two"] += 10;
            this.Redis.IncrementValueInHash(_hashId, "two", 10);

            var members = this.Redis.GetAllEntriesFromHash(_hashId);
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
            var hash = this.Redis.Hashes[_hashId];
            foreach (var key in this._stringMap.Keys) {
                hash.Add(key, this._stringMap[key]);
            }

            this._stringMap.Remove("two");
            hash.Remove("two");

            var members = this.Redis.GetAllEntriesFromHash(_hashId);
            Assert.That(members, Is.EquivalentTo(this._stringMap));
        }

        [Test]
        public void Can_RemoveFromHash() {
            const string removeMember = "two";

            foreach (var value in this._stringMap) {
                this.Redis.SetEntryInHash(_hashId, value.Key, value.Value);
            }

            this.Redis.RemoveEntryFromHash(_hashId, removeMember);

            this._stringMap.Remove(removeMember);

            var members = this.Redis.GetAllEntriesFromHash(_hashId);
            Assert.That(members, Is.EquivalentTo(this._stringMap));
        }

        [Test]
        public void Can_SetItemInHash_and_GetAllFromHash() {
            foreach (var value in this._stringMap) {
                this.Redis.SetEntryInHash(_hashId, value.Key, value.Value);
            }

            var members = this.Redis.GetAllEntriesFromHash(_hashId);
            Assert.That(members, Is.EquivalentTo(this._stringMap));
        }

        [Test]
        public void Can_SetItemInHashIfNotExists() {
            foreach (var value in this._stringMap) {
                this.Redis.SetEntryInHash(_hashId, value.Key, value.Value);
            }

            this.Redis.SetEntryInHashIfNotExists(_hashId, "two", "did not change existing item");
            this.Redis.SetEntryInHashIfNotExists(_hashId, "five", "changed non existing item");
            this._stringMap["five"] = "changed non existing item";

            var members = this.Redis.GetAllEntriesFromHash(_hashId);
            Assert.That(members, Is.EquivalentTo(this._stringMap));
        }

        [Test]
        public void Can_SetRangeInHash() {
            var newStringMap = new Dictionary<string, string> {
                { "five", "e" },
                { "six", "f" },
                { "seven", "g" }
            };
            foreach (var value in this._stringMap) {
                this.Redis.SetEntryInHash(_hashId, value.Key, value.Value);
            }

            this.Redis.SetRangeInHash(_hashId, newStringMap);

            foreach (var value1 in newStringMap) {
                this._stringMap.Add(value1.Key, value1.Value);
            }

            var members = this.Redis.GetAllEntriesFromHash(_hashId);
            Assert.That(members, Is.EquivalentTo(this._stringMap));
        }

        [Test]
        public void Can_store_as_Hash() {
            var dto = new HashTest { Id = 1 };
            this.Redis.StoreAsHash(dto);

            var storedHash = this.Redis.GetHashKeys(dto.ToUrn());

            Assert.That(storedHash, Is.EquivalentTo(new[] { "Id" }));

            var hold = RedisClient.ConvertToHashFn;

            RedisClient.ConvertToHashFn = o => {
                var dictionary = JObject.FromObject(o).ToObject<Dictionary<string, string>>();
                var dict = new Dictionary<string, string>();
                foreach (var pair in dictionary) {
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
            var hash = this.Redis.Hashes[_hashId];
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

            foreach (var value in this._stringMap) {
                this.Redis.SetEntryInHash(_hashId, value.Key, value.Value);
            }

            Assert.That(this.Redis.HashContainsEntry(_hashId, existingMember), Is.True);
            Assert.That(this.Redis.HashContainsEntry(_hashId, nonExistingMember), Is.False);
        }

    }

}
