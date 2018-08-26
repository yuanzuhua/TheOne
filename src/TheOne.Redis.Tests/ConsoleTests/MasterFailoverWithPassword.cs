using System;
using System.Threading;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.ClientManager;
using TheOne.Redis.Sentinel;

namespace TheOne.Redis.Tests.ConsoleTests {

    [TestFixture]
    [Ignore("not working")]
    internal sealed class MasterFailoverWithPassword : RedisTestBase {

        [Test]
        public void Execute() {
            var sentinel = new RedisSentinel(Config.SentinelHosts, Config.SentinelMasterName);
            sentinel.HostFilter = host => string.Format("password@{0}", host);
            IRedisClientManager manager = sentinel.Start();

            sentinel.OnWorkerError = Console.WriteLine;

            while (true) {
                try {
                    const string redisKey = "my Name";
                    using (IRedisClient client = manager.GetClient()) {
                        var result = client.Get<string>(redisKey);
                        Console.WriteLine("Redis Key: {0} \t Port: {1}", result, client.Port);
                    }
                } catch (Exception ex) {
                    Console.WriteLine("Error {0}", ex.Message);
                }

                Thread.Sleep(3000);
            }
        }

    }

}
