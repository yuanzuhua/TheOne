using System;
using NUnit.Framework;
using TheOne.Redis.ClientManager;

namespace TheOne.Redis.Tests.Issues {

    internal sealed class RedisCharacterizationTests : RedisTestBase {

        private IRedisClientManager _db1ClientManager;
        private IRedisClientManager _db2ClientManager;

        [SetUp]
        public void SetUp() {
            foreach (var clientManager in new[] { this._db1ClientManager, this._db2ClientManager }) {
                if (clientManager != null) {
                    using (var cacheClient = clientManager.GetCacheClient()) {
                        cacheClient.Remove("key");
                    }
                }
            }
        }

        [Test]
        public void BasicRedisClientManager_WhenUsingADatabaseOnARedisConnectionString_CorrectDatabaseIsUsed() {
            this.TestForDatabaseOnConnectionString(connectionString => new BasicRedisClientManager(connectionString));
        }

        [Test]
        public void PooledRedisClientManager_WhenUsingADatabaseOnARedisConnectionString_CorrectDatabaseIsUsed() {
            this.TestForDatabaseOnConnectionString(connectionString => new PooledRedisClientManager(connectionString));
        }

        [Test]
        public void RedisManagerPool_WhenUsingADatabaseOnARedisConnectionString_CorrectDatabaseIsUsed() {
            this.TestForDatabaseOnConnectionString(connectionString => new RedisManagerPool(connectionString));
        }

        private void TestForDatabaseOnConnectionString(Func<string, IRedisClientManager> factory) {
            this._db1ClientManager = factory(Config.MasterHost + "?db=1");
            this._db2ClientManager = factory(Config.MasterHost + "?db=2");

            using (var cacheClient = this._db1ClientManager.GetCacheClient()) {
                cacheClient.Set("key", "value");
            }

            using (var cacheClient = this._db2ClientManager.GetCacheClient()) {
                Assert.Null(cacheClient.Get<string>("key"));
            }
        }

        [Test]
        public void WhenUsingAnInitialDatabase_CorrectDatabaseIsUsed() {
            this._db1ClientManager = new BasicRedisClientManager(1, Config.MasterHost);
            this._db2ClientManager = new BasicRedisClientManager(2, Config.MasterHost);

            using (var cacheClient = this._db1ClientManager.GetCacheClient()) {
                cacheClient.Set("key", "value");
            }

            using (var cacheClient = this._db2ClientManager.GetCacheClient()) {
                Assert.Null(cacheClient.Get<string>("key"));
            }
        }

    }

}
