using System;
using NUnit.Framework;
using TheOne.Redis.Sentinel;

namespace TheOne.Redis.Tests.ConsoleTests {

    [TestFixture]
    [Ignore("Hurts master")]
    internal sealed class ForceFailover : RedisTestBase {

        [Test]
        public void Execute() {
            var sentinel = new RedisSentinel(Config.SentinelHosts, Config.SentinelMasterName);

            var redisManager = sentinel.Start();

            using (var client = redisManager.GetClient()) {
                client.FlushAll();
            }

            using (var client = redisManager.GetClient()) {
                Console.WriteLine(client.IncrementValue("counter").ToString());
            }

            Console.WriteLine("Force 'SENTINEL failover mymaster'...");

            try {
                using (var client = redisManager.GetClient()) {
                    Console.WriteLine(client.IncrementValue("counter").ToString());
                }
            } catch (Exception ex) {
                Console.WriteLine(ex.Message);
            }

            try {
                using (var client = redisManager.GetClient()) {
                    Console.WriteLine(client.IncrementValue("counter").ToString());
                }
            } catch (Exception ex) {
                Console.WriteLine(ex.Message);
            }

            try {
                using (var client = redisManager.GetClient()) {
                    Console.WriteLine(client.IncrementValue("counter").ToString());
                }
            } catch (Exception ex) {
                Console.WriteLine(ex.Message);
            }
        }

    }

}
