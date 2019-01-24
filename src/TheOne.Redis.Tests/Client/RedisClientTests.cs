using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Common;
using TheOne.Redis.Queue.Locking;

namespace TheOne.Redis.Tests.Client {

    [TestFixture]
    internal sealed class RedisClientTests : RedisClientTestsBase {

        #region Models

        public class MyPoco {

            public int Id { get; set; }
            public string Name { get; set; }

        }

        #endregion

        private const string _value = "Value";

        private void IncrementKeyInsideLock(string key, string lockKey, int clientNo, IRedisClient client) {
            using (client.AcquireLock(lockKey)) {
                Console.WriteLine("client {0} acquired lock", clientNo);
                var val = client.Get<int>(key);

                Thread.Sleep(200);

                client.Set(key, val + 1);
                Console.WriteLine("client {0} released lock", clientNo);
            }
        }

        public override void SetUp() {
            base.SetUp();
            this.Redis.NamespacePrefix = "RedisClientTests";
        }

        [Test]
        public void Can_AcquireLock() {
            var key = this.PrefixedKey("AcquireLockKey");
            var lockKey = this.PrefixedKey("Can_AcquireLock");
            this.Redis.IncrementValue(key); // 1

            var tasks = new List<Task>();

            for (var i = 0; i < 5; i++) {
                var i1 = i;
                var item = Task.Run(() => {
                    var client = new RedisClient(Config.MasterHost) { NamespacePrefix = this.Redis.NamespacePrefix };
                    this.IncrementKeyInsideLock(key, lockKey, i1, client);
                });

                tasks.Add(item);
            }

            Task.WaitAll(tasks.ToArray(), TimeSpan.FromSeconds(5));

            var val = this.Redis.Get<int>(key);

            Assert.That(val, Is.EqualTo(1 + 5));
        }

        [Test]
        public void Can_AcquireLock_Timeout() {
            var key = this.PrefixedKey("AcquireLockKeyTimeout");
            var lockKey = this.PrefixedKey("Can_AcquireLock_Timeout");
            this.Redis.IncrementValue(key); // 1
            using (var acquiredLock = this.Redis.AcquireLock(lockKey)) {
                var waitFor = TimeSpan.FromMilliseconds(1000);
                var now = DateTime.Now;

                try {
                    using (var client = new RedisClient(Config.MasterHost)) {
                        using (client.AcquireLock(lockKey, waitFor)) {
                            this.Redis.IncrementValue(key); // 2
                        }
                    }
                } catch (TimeoutException) {
                    var val = this.Redis.Get<int>(key);
                    Assert.That(val, Is.EqualTo(1));

                    var timeTaken = DateTime.Now - now;
                    Assert.That(timeTaken.TotalMilliseconds > waitFor.TotalMilliseconds, Is.True);
                    Assert.That(timeTaken.TotalMilliseconds < waitFor.TotalMilliseconds + 1000, Is.True);
                    return;
                }
            }

            Assert.Fail("should have Timed out");
        }

        [Test]
        public void Can_Append() {
            const string expectedString = "Hello, " + "World!";
            this.Redis.SetValue("key", "Hello, ");
            var currentLength = this.Redis.AppendToValue("key", "World!");

            Assert.That(currentLength, Is.EqualTo(expectedString.Length));

            var val = this.Redis.GetValue("key");
            Assert.That(val, Is.EqualTo(expectedString));
        }

        [Test]
        public void Can_BgRewriteAof() {
            this.Redis.BgRewriteAof();
        }

        [Test]
        public void Can_BgSave() {
            try {
                this.Redis.BgSave();
            } catch (RedisResponseException e) {
                // if exception has that message then it still proves that BgSave works as expected.
                if (e.Message.StartsWith("Can't BGSAVE while AOF log rewriting is in progress")
                    || e.Message.StartsWith("An AOF log rewriting in progress: can't BGSAVE right now")
                    || e.Message.StartsWith("Background save already in progress")) {
                    return;
                }

                throw;
            }
        }


        [Test]
        public void Can_change_db_at_runtime() {
            using (var redis = new RedisClient(Config.Localhost, Config.LocalhostPort, Config.LocalhostPassword, 1)) {
                var val = Environment.TickCount;
                var key = "test" + val;
                try {
                    redis.Set(key, val);
                    redis.ChangeDb(2);
                    Assert.That(redis.Get<int>(key), Is.EqualTo(0));
                    redis.ChangeDb(1);
                    Assert.That(redis.Get<int>(key), Is.EqualTo(val));
                    redis.Dispose();
                } finally {
                    redis.ChangeDb(1);
                    redis.Del(key);
                }
            }
        }

        [Test]
        public void Can_create_distributed_lock() {
            var key = "lockkey";
            var lockTimeout = 2;

            var distributedLock = new DistributedLock();
            Assert.AreEqual(distributedLock.Lock(key, lockTimeout, lockTimeout, out var lockExpire, this.Redis),
                DistributedLock.LockAcquired);

            // can't re-lock
            distributedLock = new DistributedLock();
            Assert.AreEqual(distributedLock.Lock(key, lockTimeout, lockTimeout, out lockExpire, this.Redis),
                DistributedLock.LockNotAcquired);

            // re-acquire lock after timeout
            Thread.Sleep(lockTimeout * 1000 + 1000);
            distributedLock = new DistributedLock();
            Assert.AreEqual(distributedLock.Lock(key, lockTimeout, lockTimeout, out lockExpire, this.Redis),
                DistributedLock.LockRecovered);


            Assert.IsTrue(distributedLock.Unlock(key, lockExpire, this.Redis));

            // can now lock
            distributedLock = new DistributedLock();
            Assert.AreEqual(distributedLock.Lock(key, lockTimeout, lockTimeout, out lockExpire, this.Redis), DistributedLock.LockAcquired);


            // cleanup
            Assert.IsTrue(distributedLock.Unlock(key, lockExpire, this.Redis));
        }

        [Test]
        public void Can_delete_keys() {
            this.Redis.SetValue("key", "val");

            Assert.That(this.Redis.ContainsKey("key"), Is.True);

            this.Redis.Del("key");

            Assert.That(this.Redis.ContainsKey("key"), Is.False);

            var keysMap = Enumerable.Range(0, 10).ToDictionary(i => "key" + i, i => "val" + i);

            this.Redis.SetAll(keysMap);

            for (var i = 0; i < 10; i++) {
                Assert.That(this.Redis.ContainsKey("key" + i), Is.True);
            }

            this.Redis.Del(keysMap.Keys.ToArray());

            for (var i = 0; i < 10; i++) {
                Assert.That(this.Redis.ContainsKey("key" + i), Is.False);
            }
        }

        [Test]
        public void Can_Echo() {
            Assert.That(this.Redis.Echo("Hello"), Is.EqualTo("Hello"));
        }

        [Test]
        public void Can_Expire() {
            this.Redis.SetValue("key", "val");
            this.Redis.Expire("key", 1);
            Assert.That(this.Redis.ContainsKey("key"), Is.True);
            Thread.Sleep(2000);
            Assert.That(this.Redis.ContainsKey("key"), Is.False);
        }

        [Test]
        public void Can_Expire_Ms() {
            this.Redis.SetValue("key", "val");
            this.Redis.ExpireEntryIn("key", TimeSpan.FromMilliseconds(100));
            Assert.That(this.Redis.ContainsKey("key"), Is.True);
            Thread.Sleep(500);
            Assert.That(this.Redis.ContainsKey("key"), Is.False);
        }

        [Test]
        public void Can_ExpireAt() {
            this.Redis.SetValue("key", "val");

            var unixNow = DateTime.Now.ToUnixTime();
            var in2Secs = unixNow + 2;

            this.Redis.ExpireAt("key", in2Secs);

            Assert.That(this.Redis.ContainsKey("key"), Is.True);
            Thread.Sleep(3000);
            Assert.That(this.Redis.ContainsKey("key"), Is.False);
        }

        [Test]
        public void Can_get_Keys_with_pattern() {
            for (var i = 0; i < 5; i++) {
                this.Redis.SetValue("k1:" + i, "val");
            }

            for (var i = 0; i < 5; i++) {
                this.Redis.SetValue("k2:" + i, "val");
            }

            var keys = this.Redis.Keys("k1:*");
            Assert.That(keys.Length, Is.EqualTo(5));
        }

        [Test]
        public void Can_get_RandomKey() {
            this.Redis.Db = 15;
            var keysMap =
                Enumerable.Range(0, 5).ToDictionary(i => this.Redis.NamespacePrefix + "key" + i, i => "val" + i);

            this.Redis.SetAll(keysMap);

            var randKey = this.Redis.RandomKey();

            Assert.That(keysMap.ContainsKey(randKey), Is.True);
        }

        [Test]
        public void Can_get_Slowlog() {
            var log = this.Redis.GetSlowlog(10);

            foreach (var t in log) {
                Console.WriteLine(t.Id);
                Console.WriteLine(t.Duration);
                Console.WriteLine(t.Timestamp);
                Console.WriteLine(string.Join(":", t.Arguments));
            }
        }

        [Test]
        public void Can_get_Types() {
            this.Redis.SetValue("string", "string");
            this.Redis.AddItemToList("list", "list");
            this.Redis.AddItemToSet("set", "set");
            this.Redis.AddItemToSortedSet("sortedset", "sortedset");
            this.Redis.SetEntryInHash("hash", "key", "val");

            Assert.That(this.Redis.GetEntryType("nokey"), Is.EqualTo(RedisKeyType.None));
            Assert.That(this.Redis.GetEntryType("string"), Is.EqualTo(RedisKeyType.String));
            Assert.That(this.Redis.GetEntryType("list"), Is.EqualTo(RedisKeyType.List));
            Assert.That(this.Redis.GetEntryType("set"), Is.EqualTo(RedisKeyType.Set));
            Assert.That(this.Redis.GetEntryType("sortedset"), Is.EqualTo(RedisKeyType.SortedSet));
            Assert.That(this.Redis.GetEntryType("hash"), Is.EqualTo(RedisKeyType.Hash));
        }

        [Test]
        public void Can_GetAll() {
            var keysMap = Enumerable.Range(0, 5).ToDictionary(i => "key" + i, i => "val" + i);

            this.Redis.SetAll(keysMap);

            var map = this.Redis.GetAll<string>(keysMap.Keys);
            var mapKeys = this.Redis.GetValues(keysMap.Keys.ToList());

            foreach (var entry in keysMap) {
                Assert.That(map.ContainsKey(entry.Key), Is.True);
                Assert.That(mapKeys.Contains(entry.Value), Is.True);
            }
        }

        [Test]
        public void Can_GetRange() {
            const string helloWorld = "Hello, World!";
            this.Redis.SetValue("key", helloWorld);

            var fromIndex = "Hello, ".Length;
            var toIndex = "Hello, World".Length - 1;

            var expectedString = helloWorld.Substring(fromIndex, toIndex - fromIndex + 1);
            var world = this.Redis.GetRange("key", fromIndex, toIndex);

            Assert.That(world.Length, Is.EqualTo(expectedString.Length));
        }

        [Test]
        public void Can_GetServerTime() {
            var now = this.Redis.GetServerTime();

            Console.WriteLine(now.Kind.ToJson());
            Console.WriteLine(now.ToString("D"));
            Console.WriteLine(now.ToString("T"));

            Console.WriteLine("UtcNow");
            Console.WriteLine(DateTime.UtcNow.ToString("D"));
            Console.WriteLine(DateTime.UtcNow.ToString("T"));

            Assert.That(now.Date, Is.EqualTo(DateTime.UtcNow.Date));
        }

        [Test]
        public void Can_GetTimeToLive() {
            this.Redis.SetValue("key", "val");
            this.Redis.Expire("key", 10);

            var ttl = this.Redis.GetTimeToLive("key");
            Assert.NotNull(ttl);
            Assert.That(ttl.Value.TotalSeconds, Is.GreaterThanOrEqualTo(9));
            Thread.Sleep(1700);

            ttl = this.Redis.GetTimeToLive("key");
            Assert.NotNull(ttl);
            Assert.That(ttl.Value.TotalSeconds, Is.LessThanOrEqualTo(9));
        }

        [Test]
        public void Can_GetValues_JSON_strings() {
            var val =
                "{\"AuthorId\":0,\"Created\":\"\\/Date(1345961754013)\\/\",\"Name\":\"test\",\"Base64\":\"BQELAAEBWAFYAVgBWAFYAVgBWAFYAVgBWAFYAVgBWAFYAVgBWAFYAVgBWAFYAVgBWAFYAViA/4D/gP+A/4D/gP+A/4D/gP+A/4D/gP+A/4D/gP+A/4D/gP+A/4D/gP8BWAFYgP8BWAFYAViA/wFYAVgBWID/AVgBWAFYgP8BWAFYAViA/4D/gP+A/4D/AVgBWID/gP8BWID/gP8BWID/gP+A/wFYgP+A/4D/gP8BWID/gP+A/4D/gP+A/wFYAViA/4D/AViA/4D/AVgBWAFYgP8BWAFYAViA/4D/AViA/4D/gP+A/4D/gP8BWAFYgP+A/wFYgP+A/wFYgP+A/4D/gP+A/wFYgP+A/wFYgP+A/4D/gP+A/4D/AVgBWID/gP8BWID/gP8BWAFYAViA/wFYAVgBWID/gP8BWID/gP+A/4D/gP+A/wFYAViA/4D/gP+A/4D/gP+A/4D/gP+A/4D/gP+A/4D/gP+A/4D/gP+A/4D/gP8BWAFYgP+A/4D/gP+A/4D/gP+A/4D/gP+A/4D/gP+A/4D/gP+A/4D/gP+A/4D/AVgBWID/gP+A/4D/gP+A/4D/gP+A/4D/gP+A/4D/gP+A/4D/gP+A/4D/gP+A/wFYAViA/4D/gP+A/4D/gP+A/4D/gP+A/4D/gP+A/4D/gP+A/4D/gP+A/4D/gP8BWAFYgP+A/4D/gP+A/4D/gP+A/4D/gP+A/4D/gP+A/4D/gP+A/4D/gP+A/4D/AVgBWID/gP+A/4D/gP+A/4D/gP+A/4D/gP+A/4D/gP+A/4D/gP+A/4D/gP+A/wFYAVgBWAFYAVgBWAFYAVgBWAFYAVgBWAFYAVgBWAFYAVgBWAFYAVgBWAFYAVgBWAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\"}";

            this.Redis.SetValue("UserLevel/1", val);

            var strList = this.Redis.GetValues(new List<string>(new[] { "UserLevel/1" }));

            Assert.That(strList.Count, Is.EqualTo(1));
            Assert.That(strList[0], Is.EqualTo(val));
        }

        [Test]
        public void Can_Ping() {
            Assert.That(this.Redis.Ping(), Is.True);
        }

        [Test]
        public void Can_Quit() {
            this.Redis.Quit();
            this.Redis.NamespacePrefix = null;
        }

        [Test]
        public void Can_RenameKey() {
            this.Redis.SetValue("oldkey", "val");
            this.Redis.Rename("oldkey", "newkey");

            Assert.That(this.Redis.ContainsKey("oldkey"), Is.False);
            Assert.That(this.Redis.ContainsKey("newkey"), Is.True);
        }

        [Test]
        public void Can_Save() {
            try {
                this.Redis.Save();
            } catch (RedisResponseException e) {
                // if exception has that message then it still proves that BgSave works as expected.
                if (e.Message.StartsWith("Can't BGSAVE while AOF log rewriting is in progress")
                    || e.Message.StartsWith("An AOF log rewriting in progress: can't BGSAVE right now")
                    || e.Message.StartsWith("Background save already in progress")) {
                    return;
                }

                throw;
            }
        }

        [Test]
        public void Can_Set_and_Get_key_with_all_byte_values() {
            const string key = "bytesKey";

            var value = new byte[256];
            for (var i = 0; i < value.Length; i++) {
                value[i] = (byte)i;
            }

            this.Redis.Set(key, value);
            var resultValue = this.Redis.Get(key);

            Assert.That(resultValue, Is.EquivalentTo(value));
        }

        [Test]
        public void Can_Set_and_Get_key_with_space() {
            this.Redis.SetValue("key with space", _value);
            var valueBytes = this.Redis.Get("key with space");
            var valueString = Encoding.UTF8.GetString(valueBytes);
            this.Redis.Remove("key with space");

            Assert.That(valueString, Is.EqualTo(_value));
        }

        [Test]
        public void Can_Set_and_Get_key_with_spaces() {
            const string key = "key with spaces";

            this.Redis.SetValue(key, _value);
            var valueBytes = this.Redis.Get(key);
            var valueString = Encoding.UTF8.GetString(valueBytes);

            Assert.That(valueString, Is.EqualTo(_value));
        }

        [Test]
        public void Can_Set_and_Get_string() {
            this.Redis.SetValue("key", _value);
            var valueBytes = this.Redis.Get("key");
            var valueString = Encoding.UTF8.GetString(valueBytes);
            this.Redis.Remove("key");

            Assert.That(valueString, Is.EqualTo(_value));
        }

        [Test]
        public void Can_Set_Expire_Milliseconds() {
            this.Redis.SetValue("key", "val", TimeSpan.FromMilliseconds(1000));
            Assert.That(this.Redis.ContainsKey("key"), Is.True);
            Thread.Sleep(2000);
            Assert.That(this.Redis.ContainsKey("key"), Is.False);
        }

        [Test]
        public void Can_Set_Expire_Seconds() {
            this.Redis.SetValue("key", "val", TimeSpan.FromSeconds(1));
            Assert.That(this.Redis.ContainsKey("key"), Is.True);
            Thread.Sleep(2000);
            Assert.That(this.Redis.ContainsKey("key"), Is.False);
        }

        [Test]
        public void Can_Set_Expire_Seconds_if_exists() {
            Assert.That(this.Redis.SetValueIfExists("key", "val", TimeSpan.FromMilliseconds(1500)),
                Is.False);
            Assert.That(this.Redis.ContainsKey("key"), Is.False);

            this.Redis.SetValue("key", "val");
            Assert.That(this.Redis.SetValueIfExists("key", "val", TimeSpan.FromMilliseconds(1000)),
                Is.True);
            Assert.That(this.Redis.ContainsKey("key"), Is.True);

            Thread.Sleep(2000);
            Assert.That(this.Redis.ContainsKey("key"), Is.False);
        }

        [Test]
        public void Can_Set_Expire_Seconds_if_not_exists() {
            Assert.That(this.Redis.SetValueIfNotExists("key", "val", TimeSpan.FromMilliseconds(1000)),
                Is.True);
            Assert.That(this.Redis.ContainsKey("key"), Is.True);

            Assert.That(this.Redis.SetValueIfNotExists("key", "val", TimeSpan.FromMilliseconds(1000)),
                Is.False);

            Thread.Sleep(2000);
            Assert.That(this.Redis.ContainsKey("key"), Is.False);

            this.Redis.Remove("key");
            this.Redis.SetValueIfNotExists("key", "val", TimeSpan.FromMilliseconds(1000));
            Assert.That(this.Redis.ContainsKey("key"), Is.True);
        }

        [Test]
        [Ignore("Works too well and shutdown the server")]
        public void Can_Shutdown() {
            this.Redis.Shutdown();
        }

        [Test]
        public void Can_SlaveOfNoOne() {
            this.Redis.SlaveOfNoOne();
        }

        [Test]
        public void Can_store_Dictionary() {
            var keys = Enumerable.Range(0, 5).Select(x => "key" + x).ToList();
            var vals = Enumerable.Range(0, 5).Select(x => "val" + x).ToList();
            Console.WriteLine(vals.Count);
            var map = new Dictionary<string, string>();
            keys.ForEach(x => map[x] = "val" + x);

            this.Redis.SetAll(map);

            var all = this.Redis.GetValuesMap(keys);
            Assert.AreEqual(map, all);
        }


        [Test]
        public void Can_store_Dictionary_as_bytes() {
            var map = new Dictionary<string, byte[]>();
            map["key_a"] = "123".ToUtf8Bytes();
            map["key_b"] = null;

            this.Redis.SetAll(map);

            Assert.That(this.Redis.Get<string>("key_a"), Is.EqualTo("123"));
            Assert.That(this.Redis.Get("key_b"), Is.EqualTo(""));
        }

        [Test]
        public void Can_store_Dictionary_as_objects() {
            var map = new Dictionary<string, object> {
                ["key_a"] = "123",
                ["key_b"] = null
            };

            this.Redis.SetAll(map);

            Assert.That(this.Redis.Get<string>("key_a"), Is.EqualTo("123"));
            Assert.That(this.Redis.Get("key_b"), Is.EqualTo(""));
        }

        [Test]
        public void Can_store_multiple_keys() {
            var keys = Enumerable.Range(0, 5).Select(x => "key" + x).ToList();
            var vals = Enumerable.Range(0, 5).Select(x => "val" + x).ToList();

            this.Redis.SetAll(keys, vals);

            var all = this.Redis.GetValues(keys);
            Assert.AreEqual(vals, all);
        }

        [Test]
        public void Can_StoreObject() {
            object poco = new MyPoco { Id = 1, Name = "Test" };

            this.Redis.StoreObject(poco);

            Assert.That(this.Redis.GetValue(this.Redis.NamespacePrefix + "urn:mypoco:1"), Is.EqualTo("{\"Id\":1,\"Name\":\"Test\"}"));

            Assert.That(this.Redis.PopItemFromSet(this.Redis.NamespacePrefix + "ids:MyPoco"), Is.EqualTo("1"));
        }

        [Test]
        public void GetKeys_on_non_existent_keys_returns_empty_collection() {
            var matchingKeys = this.Redis.SearchKeys("ss-tests:NOTEXISTS");

            Assert.That(matchingKeys.Count, Is.EqualTo(0));
        }

        [Test]
        public void GetKeys_returns_matching_collection() {
            this.Redis.Set("ss-tests:a1", "One");
            this.Redis.Set("ss-tests:a2", "One");
            this.Redis.Set("ss-tests:b3", "One");

            var matchingKeys = this.Redis.SearchKeys("ss-tests:a*");

            Assert.That(matchingKeys.Count, Is.EqualTo(2));
        }

        [Test]
        public void Should_reset_slowlog() {
            this.Redis.SlowlogReset();
        }

    }

}
