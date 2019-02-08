using System;
using System.Threading;
using NUnit.Framework;
using TheOne.Redis.ClientManager;

namespace TheOne.Redis.Tests.ConsoleTests {

    [TestFixture]
    internal sealed class BlockingPop : RedisTestBase {

        [Test]
        public void Execute() {

            RedisConfig.DefaultConnectTimeout = 1 * 1000;
            RedisConfig.DefaultSendTimeout = 1 * 1000;
            RedisConfig.DefaultReceiveTimeout = 1 * 1000;

            RedisConfig.DefaultRetryTimeout = 10 * 1000;

            RedisConfig.DefaultIdleTimeoutSecs = 240;
            RedisConfig.BackOffMultiplier = 10;
            RedisConfig.BufferPoolMaxSize = 500000;
            RedisConfig.VerifyMasterConnections = true;
            RedisConfig.HostLookupTimeoutMs = 1000;
            RedisConfig.DeactivatedClientsExpiry = TimeSpan.FromSeconds(15);

            var redisManager = new RedisManagerPool(Config.MasterHost);

            // how many test items to create
            var items = 5;
            // how long to try popping
            var waitForSeconds = 5;
            // name of list
            var listId = "testlist";

            var startedAt = DateTime.Now;
            Console.WriteLine("--------------------------");
            Console.WriteLine("push {0} items to a list, then try pop for {1} seconds. repeat.", items, waitForSeconds);
            Console.WriteLine("--------------------------");

            using (var redis = redisManager.GetClient()) {
                do {
                    // add items to list
                    for (var i = 1; i <= items; i++) {
                        redis.PushItemToList(listId, string.Format("item {0}", i));
                    }

                    do {
                        var item = redis.BlockingPopItemFromList(listId, null);

                        // log the popped item.  if BRPOP timeout is null and list empty, I do not expect to print anything
                        Console.WriteLine("{0}", string.IsNullOrEmpty(item) ? " list empty " : item);

                        Thread.Sleep(1000);

                    } while (DateTime.Now - startedAt < TimeSpan.FromSeconds(waitForSeconds));

                    Console.WriteLine("--------------------------");
                    Console.WriteLine("completed first loop");
                    Console.WriteLine("--------------------------");

                } while (DateTime.Now - startedAt < TimeSpan.FromSeconds(2 * waitForSeconds));

                Console.WriteLine("--------------------------");
                Console.WriteLine("completed outer loop");
            }
        }

    }

}
