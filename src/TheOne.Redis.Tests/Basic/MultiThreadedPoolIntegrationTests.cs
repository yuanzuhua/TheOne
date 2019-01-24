using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using NUnit.Framework;
using TheOne.Redis.ClientManager;
using TheOne.Redis.Common;

namespace TheOne.Redis.Tests.Basic {

    [TestFixture]
    internal sealed class MultiThreadedPoolTests : RedisTestBase {

        private static Dictionary<string, int> _hostCountMap;

        private void RunSimultaneously(
            Func<IRedisClientManager> clientManagerFactory,
            Action<IRedisClientManager, int> useClientFn) {
            var before = Stopwatch.GetTimestamp();

            const int noOfConcurrentClients = 64;

            using (var manager = clientManagerFactory()) {
                var tasks = new List<Task>();
                for (var i = 0; i < noOfConcurrentClients; i++) {
                    var clientNo = i;
                    var action = (Action)(() => useClientFn(manager, clientNo));
                    tasks.Add(Task.Run(action));

                }

                Task.WaitAll(tasks.ToArray());
            }

            Console.WriteLine("Time Taken: {0}", (Stopwatch.GetTimestamp() - before) / 1000);
        }

        private static void CheckHostCountMap(Dictionary<string, int> hostCountMap) {
            Console.WriteLine(hostCountMap.ToJson());

            if (Config.SlaveHosts.Length <= 1) {
                return;
            }

            var hostCount = 0;
            foreach (var entry in hostCountMap) {
                if (entry.Value < 5) {
                    Console.WriteLine("ERROR: Host has unproportianate distrobution: " + entry.Value);
                }

                if (entry.Value > 60) {
                    Console.WriteLine("ERROR: Host has unproportianate distrobution: " + entry.Value);
                }

                hostCount += entry.Value;
            }

            if (hostCount != 64) {
                Console.WriteLine("ERROR: Invalid no of clients used");
            }
        }

        private static void UseClient(IRedisClientManager manager, int clientNo) {
            using (var client = manager.GetReadOnlyClient()) {
                lock (_hostCountMap) {
                    if (!_hostCountMap.TryGetValue(client.Host, out var hostCount)) {
                        hostCount = 0;
                    }

                    _hostCountMap[client.Host] = ++hostCount;
                }

                Console.WriteLine("Client '{0}' is using '{1}'", clientNo, client.Host);
            }
        }

        [SetUp]
        public void SetUp() {
            _hostCountMap = new Dictionary<string, int>();
        }

        [TearDown]
        public void TearDown() {
            CheckHostCountMap(_hostCountMap);
        }

        [Test]
        public void Basic_can_support_64_threads_using_the_client_simultaneously() {
            this.RunSimultaneously(() => new BasicRedisClientManager(Config.MasterHost), UseClient);
        }

        [Test]
        public void ManagerPool_can_support_64_threads_using_the_client_simultaneously() {
            this.RunSimultaneously(() => new RedisManagerPool(Config.MasterHost, new RedisPoolConfig { MaxPoolSize = 10 }), UseClient);
        }

        [Test]
        public void Pool_can_support_64_threads_using_the_client_simultaneously() {
            this.RunSimultaneously(() => (IRedisClientManager)new PooledRedisClientManager(new[] { Config.MasterHost },
                    new[] { Config.SlaveHost },
                    new RedisClientManagerConfig {
                        MaxWritePoolSize = 1,
                        MaxReadPoolSize = 1,
                        AutoStart = true
                    }),
                UseClient);
        }

    }

}
