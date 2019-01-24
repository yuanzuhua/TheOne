using System;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.ClientManager;
using TheOne.Redis.Common;
using TheOne.Redis.Tests.Shared;

namespace TheOne.Redis.Tests.Client {

    [TestFixture]
    internal sealed class RedisCacheClientTests : RedisTestBase {

        private ICacheClientExtended _cacheClient;

        [SetUp]
        public void SetUp() {
            this._cacheClient = new RedisClient(Config.MasterHost);
        }

        [TearDown]
        public void TearDown() {
            this._cacheClient.FlushAll();
            this._cacheClient?.Dispose();
            this._cacheClient = null;
        }

        [Test]
        public void Can_GetTimeToLive() {
            var model = ModelWithIdAndName.Create(1);
            var key = "model:" + model.CreateUrn();
            this._cacheClient.Add(key, model);

            var ttl = this._cacheClient.GetTimeToLive(key);
            Assert.That(ttl, Is.EqualTo(TimeSpan.MaxValue));

            this._cacheClient.Set(key, model, TimeSpan.FromSeconds(10));
            ttl = this._cacheClient.GetTimeToLive(key);
            Assert.NotNull(ttl);
            Assert.That(ttl.Value, Is.GreaterThanOrEqualTo(TimeSpan.FromSeconds(9)));
            Assert.That(ttl.Value, Is.LessThanOrEqualTo(TimeSpan.FromSeconds(10)));

            this._cacheClient.Remove(key);
            ttl = this._cacheClient.GetTimeToLive(key);
            Assert.That(ttl, Is.Null);
        }

        [Test]
        public void Can_increment_and_reset_values() {
            using (var client = new RedisManagerPool(Config.MasterHost).GetCacheClient()) {
                Assert.That(client.Increment("incr:counter", 10), Is.EqualTo(10));
                client.Set("incr:counter", 0);
                Assert.That(client.Increment("incr:counter", 10), Is.EqualTo(10));
            }
        }

        [Test]
        public void Can_Replace_By_Pattern() {
            var model = ModelWithIdAndName.Create(1);
            var modelKey = "model:" + model.CreateUrn();
            this._cacheClient.Add(modelKey, model);

            model = ModelWithIdAndName.Create(2);
            var modelKey2 = "xxmodelxx:" + model.CreateUrn();
            this._cacheClient.Add(modelKey2, model);

            var s = "this is a string";
            this._cacheClient.Add("string1", s);

            this._cacheClient.RemoveByPattern("*model*");

            var result = this._cacheClient.Get<ModelWithIdAndName>(modelKey);
            Assert.That(result, Is.Null);

            result = this._cacheClient.Get<ModelWithIdAndName>(modelKey2);
            Assert.That(result, Is.Null);

            var result2 = this._cacheClient.Get<string>("string1");
            Assert.That(result2, Is.EqualTo(s));

            this._cacheClient.RemoveByPattern("string*");

            result2 = this._cacheClient.Get<string>("string1");
            Assert.That(result2, Is.Null);
        }

        [Test]
        public void Can_Set_and_Get_key_with_all_byte_values() {
            const string key = "bytesKey";

            var value = new byte[256];
            for (var i = 0; i < value.Length; i++) {
                value[i] = (byte)i;
            }

            this._cacheClient.Set(key, value);
            var resultValue = this._cacheClient.Get<byte[]>(key);

            Assert.That(resultValue, Is.EquivalentTo(value));
        }

        [Test]
        public void Can_store_and_get_model() {
            var model = ModelWithIdAndName.Create(1);
            var cacheKey = model.CreateUrn();
            this._cacheClient.Set(cacheKey, model);

            var existingModel = this._cacheClient.Get<ModelWithIdAndName>(cacheKey);
            ModelWithIdAndName.AssertIsEqual(existingModel, model);
        }

        [Test]
        public void Can_store_null_model() {
            this._cacheClient.Set<ModelWithIdAndName>("test-key", null);
        }

        [Test]
        public void Get_non_existent_generic_value_returns_null() {
            var model = ModelWithIdAndName.Create(1);
            var cacheKey = model.CreateUrn();
            var existingModel = this._cacheClient.Get<ModelWithIdAndName>(cacheKey);
            Assert.That(existingModel, Is.Null);
        }

        [Test]
        public void Get_non_existent_value_returns_null() {
            var model = ModelWithIdAndName.Create(1);
            var cacheKey = model.CreateUrn();
            var existingModel = this._cacheClient.Get<ModelWithIdAndName>(cacheKey);
            Assert.That(existingModel, Is.Null);
        }

    }

}
