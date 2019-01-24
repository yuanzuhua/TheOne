using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.ClientManager;
using TheOne.Redis.Common;

namespace TheOne.Redis.Tests.ClientManager {

    [TestFixture]
    internal sealed class RedisManagerPoolTests : RedisTestBase {

        private readonly string[] _hosts = {
            "readwrite1",
            "readwrite2:6000",
            "192.168.0.1",
            "localhost"
        };

        private readonly string[] _testReadOnlyHosts = {
            "read1",
            "read2:7000",
            "127.0.0.1"
        };

        private string _firstReadOnlyHost;

        private string _firstReadWriteHost;

        [OneTimeSetUp]
        public void OneTimeSetUp() {
            RedisConfig.VerifyMasterConnections = false;
        }

        [OneTimeTearDown]
        public void OneTimeTearDown() {
            RedisConfig.VerifyMasterConnections = true;
        }

        public RedisManagerPool CreateManager() {
            return new RedisManagerPool(this._hosts);
        }

        private static void AssertClientHasHost(IRedisClient client, string hostWithOptionalPort) {
            var parts = hostWithOptionalPort.Split(':');
            var port = parts.Length > 1 ? int.Parse(parts[1]) : RedisConfig.DefaultPort;

            Assert.That(client.Host, Is.EqualTo(parts[0]));
            Assert.That(client.Port, Is.EqualTo(port));
        }

        [SetUp]
        public void OnBeforeEachTest() {
            this._firstReadWriteHost = this._hosts[0];
            this._firstReadOnlyHost = this._testReadOnlyHosts[0];
        }

        [Test]
        public void Can_change_db_for_client() {
            using (var db1 = new RedisManagerPool(Config.MasterHost + "?db=1")) {
                using (var db2 = new RedisManagerPool(Config.MasterHost + "?db=2")) {
                    var val = Environment.TickCount;
                    var key = "test" + val;
                    var db1Client = db1.GetClient();
                    var db2Client = db2.GetClient();
                    try {
                        db1Client.Set(key, val);
                        Assert.That(db2Client.Get<int>(key), Is.EqualTo(0));
                        Assert.That(db1Client.Get<int>(key), Is.EqualTo(val));
                    } finally {
                        db1Client.Remove(key);
                    }
                }
            }
        }

        [Test]
        public void Can_get_ReadWrite_client() {
            using (var manager = this.CreateManager()) {
                var client = manager.GetClient();

                AssertClientHasHost(client, this._firstReadWriteHost);
            }
        }

        [Test]
        public void Can_have_different_pool_size_and_host_configurations() {
            var writeHosts = new[] { "readwrite1" };

            using (var manager = new RedisManagerPool(
                writeHosts,
                new RedisPoolConfig { MaxPoolSize = 4 })) {
                // A pool size of 4 will not block getting 4 clients
                using (var client1 = manager.GetClient()) {
                    using (var client2 = manager.GetClient()) {
                        using (var client3 = manager.GetClient()) {
                            using (var client4 = manager.GetClient()) {
                                AssertClientHasHost(client1, writeHosts[0]);
                                AssertClientHasHost(client2, writeHosts[0]);
                                AssertClientHasHost(client3, writeHosts[0]);
                                AssertClientHasHost(client4, writeHosts[0]);
                            }
                        }
                    }
                }
            }
        }

        [Test]
        public void Can_support_64_threads_using_the_client_simultaneously() {

            void UseClient(IRedisClientManager manager1, int clientNo1, Dictionary<string, int> hostCountMap1) {
                using (var client = manager1.GetClient()) {
                    lock (hostCountMap1) {
                        if (!hostCountMap1.TryGetValue(client.Host, out var hostCount1)) {
                            hostCount1 = 0;
                        }

                        hostCountMap1[client.Host] = ++hostCount1;
                    }

                    Console.WriteLine("Client '{0}' is using '{1}'", clientNo1, client.Host);
                }
            }

            const int noOfConcurrentClients = 64;
            var clientUsageMap = new Dictionary<string, int>();

            var tasks = new List<Task>();

            using (var manager = this.CreateManager()) {
                for (var i = 0; i < noOfConcurrentClients; i++) {
                    var clientNo = i;
                    var item = Task.Run(() => UseClient(manager, clientNo, clientUsageMap));
                    tasks.Add(item);
                }
            }

            Task.WaitAll(tasks.ToArray());

            Console.WriteLine(clientUsageMap.ToJson());

            var hostCount = 0;
            foreach (var entry in clientUsageMap) {
                Assert.That(entry.Value, Is.GreaterThanOrEqualTo(5), "Host has unproportianate distrobution: " + entry.Value);
                Assert.That(entry.Value, Is.LessThanOrEqualTo(30), "Host has unproportianate distrobution: " + entry.Value);
                hostCount += entry.Value;
            }

            Assert.That(hostCount, Is.EqualTo(noOfConcurrentClients), "Invalid no of clients used");
        }


        [Test]
        public void Does_loop_through_ReadWrite_hosts() {
            using (var manager = this.CreateManager()) {
                var client1 = manager.GetClient();
                client1.Dispose();
                var client2 = manager.GetClient();
                var client3 = manager.GetClient();
                var client4 = manager.GetClient();
                var client5 = manager.GetClient();

                AssertClientHasHost(client1, this._hosts[0]);
                AssertClientHasHost(client2, this._hosts[1]);
                AssertClientHasHost(client3, this._hosts[2]);
                AssertClientHasHost(client4, this._hosts[3]);
                AssertClientHasHost(client5, this._hosts[0]);
            }
        }

        [Test]
        public void Does_not_block_ReadWrite_clients_pool() {
            using (var manager = new RedisManagerPool(this._hosts,
                new RedisPoolConfig { MaxPoolSize = 4 })) {
                var delay = TimeSpan.FromSeconds(1);
                var client1 = manager.GetClient();
                var client2 = manager.GetClient();
                var client3 = manager.GetClient();
                var client4 = manager.GetClient();

                Assert.That(((RedisClient)client1).IsManagedClient, Is.True);
                Assert.That(((RedisClient)client2).IsManagedClient, Is.True);
                Assert.That(((RedisClient)client3).IsManagedClient, Is.True);
                Assert.That(((RedisClient)client4).IsManagedClient, Is.True);

                Task.Run(() => {
                    Thread.Sleep(delay + TimeSpan.FromSeconds(0.5));
                    client4.Dispose();
                });

                var start = DateTime.Now;

                var client5 = manager.GetClient();

                Assert.That(((RedisClient)client5).IsManagedClient, Is.False); // outside of pool

                Assert.That(DateTime.Now - start, Is.LessThan(delay));

                AssertClientHasHost(client1, this._hosts[0]);
                AssertClientHasHost(client2, this._hosts[1]);
                AssertClientHasHost(client3, this._hosts[2]);
                AssertClientHasHost(client4, this._hosts[3]);
                AssertClientHasHost(client5, this._hosts[0]);
            }
        }

    }

}
