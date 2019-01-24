using System;
using System.Collections.Generic;
using System.Threading;
using NUnit.Framework;
using TheOne.Redis.Client;

namespace TheOne.Redis.Tests.Client {

    [TestFixture]
    internal sealed class RedisTypedClientTests : RedisClientTestsBase {

        #region Models

        public class CacheRecord {

            public string Id { get; set; }
            public List<CacheRecordChild> Children { get; set; } = new List<CacheRecordChild>();

        }

        public class CacheRecordChild {

            public string Id { get; set; }
            public string Data { get; set; }

        }

        #endregion

        private IRedisTypedClient<CacheRecord> _redisTyped;

        [SetUp]
        public override void SetUp() {
            base.SetUp();
            this.Redis.NamespacePrefix = "RedisTypedClientTests:";
            this._redisTyped = this.Redis.As<CacheRecord>();
        }

        [Test]
        public void Can_Delete_All_Items() {
            var cachedRecord = new CacheRecord {
                Id = "key",
                Children = {
                    new CacheRecordChild { Id = "childKey", Data = "data" }
                }
            };

            this._redisTyped.Store(cachedRecord);
            Assert.That(this._redisTyped.GetById("key"), Is.Not.Null);
            this._redisTyped.DeleteAll();
            Assert.That(this._redisTyped.GetById("key"), Is.Null);

        }

        [Test]
        public void Can_Expire() {
            var cachedRecord = new CacheRecord {
                Id = "key",
                Children = {
                    new CacheRecordChild { Id = "childKey", Data = "data" }
                }
            };

            this._redisTyped.Store(cachedRecord);
            this._redisTyped.ExpireIn("key", TimeSpan.FromSeconds(1));
            Assert.That(this._redisTyped.GetById("key"), Is.Not.Null);
            Thread.Sleep(2000);
            Assert.That(this._redisTyped.GetById("key"), Is.Null);
        }

        [Test]
        public void Can_ExpireAt() {
            var cachedRecord = new CacheRecord {
                Id = "key",
                Children = {
                    new CacheRecordChild { Id = "childKey", Data = "data" }
                }
            };

            this._redisTyped.Store(cachedRecord);

            var in2Secs = DateTime.UtcNow.AddSeconds(2);

            this._redisTyped.ExpireAt("key", in2Secs);

            Assert.That(this._redisTyped.GetById("key"), Is.Not.Null);
            Thread.Sleep(3000);
            Assert.That(this._redisTyped.GetById("key"), Is.Null);
        }

        [Test]
        public void Can_Store_with_Prefix() {
            var expected = new CacheRecord { Id = "123" };
            this._redisTyped.Store(expected);
            var current = this.Redis.Get<CacheRecord>(this.PrefixedKey("urn:cacherecord:123"));
            Assert.AreEqual(expected.Id, current.Id);
        }

    }

}
