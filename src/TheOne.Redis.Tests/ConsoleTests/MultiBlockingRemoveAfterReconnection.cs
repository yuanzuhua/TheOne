using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.ClientManager;
using TheOne.Redis.Common;

namespace TheOne.Redis.Tests.ConsoleTests {

    internal sealed class MultiBlockingRemoveAfterReconnection {

        private RedisManagerPool _redisManager;

        public void Execute() {
            RedisConfig.DefaultConnectTimeout = 20 * 1000;
            RedisConfig.DefaultRetryTimeout = 20 * 1000;

            this._redisManager = new RedisManagerPool(Config.MasterHost);

            this.MultipleBlocking(3);

            Console.ReadLine();
        }

        private void MultipleBlocking(int count) {
            for (var i = 0; i < count; i++) {
                var queue = $"Q{i + 1}";
                this.RunTask(() => this.BlockingRemoveStartFromList(queue), $"Receive from {queue}");
            }
        }

        private void BlockingRemoveStartFromList(string queue) {
            using (var client = this._redisManager.GetClient() as RedisClient) {
                Assert.NotNull(client);

                client.Ping();
                Console.WriteLine($"#{client.ClientId} Listening to {queue}");

                var fromList = client.BlockingRemoveStartFromList(queue, TimeSpan.FromHours(10));
                Console.WriteLine($"#{client.ClientId} Received: '{fromList.ToJson()}' from '{queue}'");
            }
        }

        private void RunTask(Action action, string name) {
            Task.Run(() => {

                while (true) {
                    try {
                        Console.WriteLine($"Invoking {name}");
                        action.Invoke();
                    } catch (Exception exception) {
                        Console.WriteLine($"Exception in {name}: {exception}");
                        //Thread.Sleep(5000);// Give redis some time to wake up!
                    }

                    Thread.Sleep(100);
                }
            });
        }

    }

}
