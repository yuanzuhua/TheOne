using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using TheOne.Redis.ClientManager;

namespace TheOne.Redis.Tests.ConsoleTests {

    [TestFixture]
    [Ignore("Restart Redis and press Enter")]
    internal sealed class BrPopAfterReconnection : RedisTestBase {

        [Test]
        public void Execute() {

            var basicRedisClientManager = new BasicRedisClientManager(Config.MasterHost);
            var queue = "FormSaved";

            Task.Run(() => {
                using (var client = basicRedisClientManager.GetReadOnlyClient()) {
                    Console.WriteLine($"Listening to {queue}");

                    var fromList = client.BlockingPopItemFromList(queue, TimeSpan.FromSeconds(60));

                    Console.WriteLine($"Received:{fromList}");
                }
            });
            Thread.Sleep(1000);

            Console.WriteLine("Enter something:");

            using (var client = basicRedisClientManager.GetClient()) {
                client.AddItemToList(queue, "something");
            }

            Console.WriteLine("Item added");

            Thread.Sleep(1000);
        }

    }

}
