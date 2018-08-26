using System;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.ClientManager;
using TheOne.Redis.Sentinel;

namespace TheOne.Redis.Tests.ConsoleTests {

    [TestFixture]
    [Ignore("Hurts master")]
    internal sealed class ForceFailover : RedisTestBase {

        [Test]
        public void Execute() {

            RedisConfig.DisableVerboseLogging = true;

            var sentinel = new RedisSentinel(Config.SentinelHosts, Config.SentinelMasterName);

            IRedisClientManager redisManager = sentinel.Start();

            using (IRedisClient client = redisManager.GetClient()) {
                client.FlushAll();
            }

            using (IRedisClient client = redisManager.GetClient()) {
                Console.WriteLine(client.IncrementValue("counter").ToString());
            }

            Console.WriteLine("Force 'SENTINEL failover mymaster'...");

            try {
                using (IRedisClient client = redisManager.GetClient()) {
                    Console.WriteLine(client.IncrementValue("counter").ToString());
                }
            } catch (Exception ex) {
                Console.WriteLine(ex.Message);
            }

            try {
                using (IRedisClient client = redisManager.GetClient()) {
                    Console.WriteLine(client.IncrementValue("counter").ToString());
                }
            } catch (Exception ex) {
                Console.WriteLine(ex.Message);
            }

            try {
                using (IRedisClient client = redisManager.GetClient()) {
                    Console.WriteLine(client.IncrementValue("counter").ToString());
                }
            } catch (Exception ex) {
                Console.WriteLine(ex.Message);
            }
        }

    }

}
