using System;
using System.Threading;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.ClientManager;

namespace TheOne.Redis.Tests.Issues {

    [TestFixture]
    [Ignore("Can't be included in Unit tests since it shutsdown redis server")]
    internal sealed class PooledRedisClientManagerIssues : RedisTestBase {

        private static PooledRedisClientManager _pool;

        public static void Stuff() {
            while (true) {
                RedisClient redisClient = null;
                try {
                    using (redisClient = (RedisClient)_pool.GetClient()) {
                        redisClient.Set("test", DateTime.Now);
                    }
                } catch (NotSupportedException nse) {
                    Console.WriteLine(redisClient.ToString());
                    Assert.Fail(nse.Message);
                } catch (Exception e) {
                    Console.WriteLine(e.Message);
                }

                Thread.Sleep(10);
            }
        }

        [Test]
        public void Cannot_add_unknown_client_back_to_pool_exception() {
            _pool = new PooledRedisClientManager(Config.MasterHost);
            try {
                var threads = new Thread[100];
                for (var i = 0; i < threads.Length; i++) {
                    threads[i] = new Thread(Stuff);
                    threads[i].Start();
                }

                Console.WriteLine("running, waiting 10secs..");
                Thread.Sleep(10000);
                using (var redisClient = (RedisClient)_pool.GetClient()) {
                    Console.WriteLine("shutdown Redis!");
                    redisClient.Shutdown();
                }
            } catch (NotSupportedException nse) {
                Assert.Fail(nse.Message);
            } catch (Exception e) {
                Console.WriteLine(e.Message);
            }

            Thread.Sleep(5000);
        }

    }

}
