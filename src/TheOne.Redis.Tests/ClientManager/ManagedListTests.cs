using System.Linq;
using NUnit.Framework;
using TheOne.Redis.ClientManager;

namespace TheOne.Redis.Tests.ClientManager {

    [TestFixture]
    internal sealed class ManagedListTests : RedisTestBase {

        private IRedisClientManager _redisManager;

        [SetUp]
        public void SetUp() {
            this._redisManager = new BasicRedisClientManager(Config.MasterHost);
            this._redisManager.Exec(r => r.FlushAll());
        }

        [TearDown]
        public void TearDown() {
            this._redisManager?.Dispose();
        }

        [Test]
        public void Can_Get_Managed_List() {
            ManagedList<string> managedList = this._redisManager.GetManagedList<string>("testkey");

            var testString = "simple Item to test";
            managedList.Add(testString);

            ManagedList<string> actualList = this._redisManager.GetManagedList<string>("testkey");

            Assert.AreEqual(managedList.First(), actualList.First());
        }

    }

}
