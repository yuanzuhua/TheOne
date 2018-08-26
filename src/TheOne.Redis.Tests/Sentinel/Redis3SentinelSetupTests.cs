using System;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.ClientManager;
using TheOne.Redis.Common;
using TheOne.Redis.Sentinel;

namespace TheOne.Redis.Tests.Sentinel {

    [TestFixture]
    internal sealed class Redis3SentinelSetupTests : RedisTestBase {

        [Test]
        public void Can_connect_directly_to_Redis_Instances() {
            using (var client = new RedisClient(Config.Sentinel6380)) {
                Console.WriteLine(Config.Sentinel6380);
                Console.WriteLine(client.Info.ToJson());
            }

            using (var sentinel = new RedisClient(Config.Sentinel26380)) {
                Console.WriteLine(Config.Sentinel26380);
                Console.WriteLine(sentinel.Info.ToJson());
            }
        }

        [Test]
        public void Can_connect_to_3SentinelSetup() {
            var sentinel = new RedisSentinel(Config.SentinelHosts, Config.SentinelMasterName);

            IRedisClientManager redisManager = sentinel.Start();

            using (IRedisClient client = redisManager.GetClient()) {
                Console.WriteLine("{0}:{1}", client.Host, client.Port);

                client.FlushAll();

                client.SetValue("Sentinel3Setup", "IntranetSentinel");

                var result = client.GetValue("Sentinel3Setup");
                Assert.That(result, Is.EqualTo("IntranetSentinel"));
            }

            using (IRedisClient readOnly = redisManager.GetReadOnlyClient()) {
                Console.WriteLine("{0}:{1}", readOnly.Host, readOnly.Port);

                var result = readOnly.GetValue("Sentinel3Setup");
                Assert.That(result, Is.EqualTo("IntranetSentinel"));
            }
        }

    }

}
