using System;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.ClientManager;
using TheOne.Redis.Common;

namespace TheOne.Redis.Tests.ConsoleTests {

    [TestFixture]
    [Explicit]
    internal sealed class BlockingRemoveAfterReconnection {

        [Test]
        public void Execute() {
            // RedisConfig.AssumeServerVersion = 4000;
            RedisConfig.DefaultConnectTimeout = 20 * 1000;
            RedisConfig.DefaultRetryTimeout = 20 * 1000;
            var basicRedisClientManager = new RedisManagerPool(Config.MasterHost);
            using (IRedisClient client = basicRedisClientManager.GetClient()) {
                Console.WriteLine("Blocking...");
                var fromList = client.BlockingRemoveStartFromList("AnyQueue", TimeSpan.FromMinutes(20));
                Console.WriteLine($"Received: {fromList.ToJson()}");
            }

        }

    }

}
