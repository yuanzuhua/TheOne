using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.ClientManager;

namespace TheOne.Redis.Tests.ClientManager {

    internal sealed class BasicRediscClientManagerTests : RedisTestBase {

        [Test]
        public void Can_select_db() {
            using (var redisManager = new BasicRedisClientManager(Config.MasterHost)) {
                using (IRedisClient client = redisManager.GetClient()) {
                    client.Db = 2;
                    client.Set("db", 2);
                }

                using (IRedisClient client = redisManager.GetClient()) {
                    client.Db = 3;
                    client.Set("db", 3);
                }

                using (IRedisClient client = redisManager.GetClient()) {
                    client.Db = 2;
                    // ((RedisClient)client).ChangeDb(2);
                    var db = client.Get<int>("db");
                    Assert.That(db, Is.EqualTo(2));
                }
            }

            using (var redisManager = new BasicRedisClientManager(Config.MasterHost + "?db=3")) {
                using (IRedisClient client = redisManager.GetClient()) {
                    var db = client.Get<int>("db");
                    Assert.That(db, Is.EqualTo(3));
                }
            }
        }

    }

}
