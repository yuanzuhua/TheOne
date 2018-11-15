using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.ClientManager;
using TheOne.Redis.Common;

namespace TheOne.Redis.Tests.ClientManager {

    [TestFixture]
    internal sealed class PooledRedisClientManagerTests : RedisTestBase {

        private readonly string[] _testReadOnlyHosts = {
            "read1",
            "read2:7000",
            "127.0.0.1"
        };

        private readonly string[] _testReadWriteHosts = {
            "readwrite1",
            "readwrite2:6000",
            "192.168.0.1",
            "localhost"
        };

        private string _firstReadOnlyHost;
        private string _firstReadWriteHost;

        private PooledRedisClientManager CreateManager() {
            return new PooledRedisClientManager(this._testReadWriteHosts,
                this._testReadOnlyHosts,
                new RedisClientManagerConfig {
                    MaxWritePoolSize = this._testReadWriteHosts.Length,
                    MaxReadPoolSize = this._testReadOnlyHosts.Length,
                    AutoStart = false,
                    DefaultDb = null
                });
        }

        private PooledRedisClientManager CreateAndStartManager() {
            PooledRedisClientManager manager = this.CreateManager();
            manager.Start();
            return manager;
        }

        private static void AssertClientHasHost(IRedisClient client, string hostWithOptionalPort) {
            string[] parts = hostWithOptionalPort.Split(':');
            var port = parts.Length > 1 ? int.Parse(parts[1]) : RedisConfig.DefaultPort;

            Assert.That(client.Host, Is.EqualTo(parts[0]));
            Assert.That(client.Port, Is.EqualTo(port));
        }

        [OneTimeSetUp]
        public void OneTimeSetUp() {
            RedisConfig.VerifyMasterConnections = false;
        }

        [OneTimeTearDown]
        public void OneTimeTearDown() {
            RedisConfig.VerifyMasterConnections = true;
        }

        [SetUp]
        public void SetUp() {
            this._firstReadWriteHost = this._testReadWriteHosts[0];
            this._firstReadOnlyHost = this._testReadOnlyHosts[0];
        }

        [Test]
        public void Can_change_db_for_client_BasicRedisClientManager() {
            using (var db1 = new BasicRedisClientManager(1, Config.MasterHost)) {
                using (var db2 = new BasicRedisClientManager(2, Config.MasterHost)) {
                    var val = Environment.TickCount;
                    var key = "test" + val;
                    IRedisClient db1Client = db1.GetClient();
                    IRedisClient db2Client = db2.GetClient();
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
        public void Can_change_db_for_client_PooledRedisClientManager() {
            using (var db1 = new PooledRedisClientManager(1, Config.MasterHost)) {
                using (var db2 = new PooledRedisClientManager(2, Config.MasterHost)) {
                    var val = Environment.TickCount;
                    var key = "test" + val;
                    IRedisClient db1Client = db1.GetClient();
                    IRedisClient db2Client = db2.GetClient();
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
        public void Can_change_db_for_client_RedisManagerPool() {
            using (var db1 = new RedisManagerPool(Config.MasterHost + "?db=1")) {
                using (var db2 = new RedisManagerPool(Config.MasterHost + "?db=2")) {
                    var val = Environment.TickCount;
                    var key = "test" + val;
                    IRedisClient db1Client = db1.GetClient();
                    IRedisClient db2Client = db2.GetClient();
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
        public void Can_get_client_after_calling_Start() {
            using (PooledRedisClientManager manager = this.CreateManager()) {
                manager.Start();
                IRedisClient client = manager.GetClient();
                Console.WriteLine(client);
            }
        }

        [Test]
        public void Can_get_ReadOnly_client() {
            using (PooledRedisClientManager manager = this.CreateAndStartManager()) {
                IRedisClient client = manager.GetReadOnlyClient();

                AssertClientHasHost(client, this._firstReadOnlyHost);
            }
        }

        [Test]
        public void Can_get_ReadWrite_client() {
            using (PooledRedisClientManager manager = this.CreateAndStartManager()) {
                IRedisClient client = manager.GetClient();

                AssertClientHasHost(client, this._firstReadWriteHost);
            }
        }

        [Test]
        public void Can_have_different_pool_size_and_host_configurations() {
            var writeHosts = new[] { "readwrite1" };
            var readHosts = new[] { "read1", "read2" };

            const int poolSizeMultiplier = 4;

            using (var manager = new PooledRedisClientManager(writeHosts,
                    readHosts,
                    new RedisClientManagerConfig {
                        MaxWritePoolSize = writeHosts.Length * poolSizeMultiplier,
                        MaxReadPoolSize = readHosts.Length * poolSizeMultiplier,
                        AutoStart = true
                    }
                )
            ) {
                // A pool size of 4 will not block getting 4 clients
                using (IRedisClient client1 = manager.GetClient()) {
                    using (IRedisClient client2 = manager.GetClient()) {
                        using (IRedisClient client3 = manager.GetClient()) {
                            using (IRedisClient client4 = manager.GetClient()) {
                                AssertClientHasHost(client1, writeHosts[0]);
                                AssertClientHasHost(client2, writeHosts[0]);
                                AssertClientHasHost(client3, writeHosts[0]);
                                AssertClientHasHost(client4, writeHosts[0]);
                            }
                        }
                    }
                }

                // A pool size of 8 will not block getting 8 clients
                using (IRedisClient client1 = manager.GetReadOnlyClient()) {
                    using (IRedisClient client2 = manager.GetReadOnlyClient()) {
                        using (IRedisClient client3 = manager.GetReadOnlyClient()) {
                            using (IRedisClient client4 = manager.GetReadOnlyClient()) {
                                using (IRedisClient client5 = manager.GetReadOnlyClient()) {
                                    using (IRedisClient client6 = manager.GetReadOnlyClient()) {
                                        using (IRedisClient client7 = manager.GetReadOnlyClient()) {
                                            using (IRedisClient client8 = manager.GetReadOnlyClient()) {
                                                AssertClientHasHost(client1, readHosts[0]);
                                                AssertClientHasHost(client2, readHosts[1]);
                                                AssertClientHasHost(client3, readHosts[0]);
                                                AssertClientHasHost(client4, readHosts[1]);
                                                AssertClientHasHost(client5, readHosts[0]);
                                                AssertClientHasHost(client6, readHosts[1]);
                                                AssertClientHasHost(client7, readHosts[0]);
                                                AssertClientHasHost(client8, readHosts[1]);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        [Test]
        public void Can_support_64_threads_using_the_client_simultaneously() {

            void UseClient(IRedisClientManager manager1, int clientNo1, Dictionary<string, int> hostCountMap1) {
                using (IRedisClient client = manager1.GetClient()) {
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

            var clientAsyncResults = new List<Task>();
            using (PooledRedisClientManager manager = this.CreateAndStartManager()) {
                for (var i = 0; i < noOfConcurrentClients; i++) {
                    var clientNo = i;
                    Task item = Task.Run(() => UseClient(manager, clientNo, clientUsageMap));
                    clientAsyncResults.Add(item);
                }

                Task.WaitAll(clientAsyncResults.ToArray());
            }

            Console.WriteLine(RedisStats.ToDictionary());
            Console.WriteLine(clientUsageMap.ToJson());

            var hostCount = 0;
            foreach (KeyValuePair<string, int> entry in clientUsageMap) {
                Assert.That(entry.Value, Is.GreaterThanOrEqualTo(2), "Host has unproportianate distribution: " + entry.Value);
                Assert.That(entry.Value, Is.LessThanOrEqualTo(30), "Host has unproportianate distribution: " + entry.Value);
                hostCount += entry.Value;
            }

            Assert.That(hostCount, Is.EqualTo(noOfConcurrentClients), "Invalid no of clients used");
        }

        [Test]
        public void Cant_get_client_without_calling_Start() {
            using (PooledRedisClientManager manager = this.CreateManager()) {
                try {
                    IRedisClient client = manager.GetClient();
                    Console.WriteLine(client);
                } catch (InvalidOperationException) {
                    return;
                }

                Assert.Fail("Should throw");
            }
        }

        [Test]
        public void Does_block_ReadOnly_clients_pool() {
            TimeSpan delay = TimeSpan.FromSeconds(1);

            using (PooledRedisClientManager manager = this.CreateAndStartManager()) {
                IRedisClient client1 = manager.GetReadOnlyClient();
                IRedisClient client2 = manager.GetReadOnlyClient();
                IRedisClient client3 = manager.GetReadOnlyClient();

                Task.Run(() => {
                    Thread.Sleep(delay + TimeSpan.FromSeconds(0.5));
                    client3.Dispose();
                });

                DateTime start = DateTime.Now;

                IRedisClient client4 = manager.GetReadOnlyClient();

                Assert.That(DateTime.Now - start, Is.GreaterThanOrEqualTo(delay));

                AssertClientHasHost(client1, this._testReadOnlyHosts[0]);
                AssertClientHasHost(client2, this._testReadOnlyHosts[1]);
                AssertClientHasHost(client3, this._testReadOnlyHosts[2]);
                AssertClientHasHost(client4, this._testReadOnlyHosts[2]);
            }
        }

        [Test]
        public void Does_block_ReadWrite_clients_pool() {
            using (PooledRedisClientManager manager = this.CreateAndStartManager()) {
                TimeSpan delay = TimeSpan.FromSeconds(1);
                IRedisClient client1 = manager.GetClient();
                IRedisClient client2 = manager.GetClient();
                IRedisClient client3 = manager.GetClient();
                IRedisClient client4 = manager.GetClient();

                Task.Run(() => {
                    Thread.Sleep(delay + TimeSpan.FromSeconds(0.5));
                    client4.Dispose();
                });

                DateTime start = DateTime.Now;

                IRedisClient client5 = manager.GetClient();

                Assert.That(DateTime.Now - start, Is.GreaterThanOrEqualTo(delay));

                AssertClientHasHost(client1, this._testReadWriteHosts[0]);
                AssertClientHasHost(client2, this._testReadWriteHosts[1]);
                AssertClientHasHost(client3, this._testReadWriteHosts[2]);
                AssertClientHasHost(client4, this._testReadWriteHosts[3]);
                AssertClientHasHost(client5, this._testReadWriteHosts[3]);
            }
        }

        [Test]
        public void Does_loop_through_ReadOnly_hosts() {
            using (PooledRedisClientManager manager = this.CreateAndStartManager()) {
                IRedisClient client1 = manager.GetReadOnlyClient();
                client1.Dispose();
                IRedisClient client2 = manager.GetReadOnlyClient();
                client2.Dispose();
                IRedisClient client3 = manager.GetReadOnlyClient();
                IRedisClient client4 = manager.GetReadOnlyClient();
                IRedisClient client5 = manager.GetReadOnlyClient();

                AssertClientHasHost(client1, this._testReadOnlyHosts[0]);
                AssertClientHasHost(client2, this._testReadOnlyHosts[1]);
                AssertClientHasHost(client3, this._testReadOnlyHosts[2]);
                AssertClientHasHost(client4, this._testReadOnlyHosts[0]);
                AssertClientHasHost(client5, this._testReadOnlyHosts[1]);
            }
        }

        [Test]
        public void Does_loop_through_ReadWrite_hosts() {
            using (PooledRedisClientManager manager = this.CreateAndStartManager()) {
                IRedisClient client1 = manager.GetClient();
                client1.Dispose();
                IRedisClient client2 = manager.GetClient();
                IRedisClient client3 = manager.GetClient();
                IRedisClient client4 = manager.GetClient();
                IRedisClient client5 = manager.GetClient();

                AssertClientHasHost(client1, this._testReadWriteHosts[0]);
                AssertClientHasHost(client2, this._testReadWriteHosts[1]);
                AssertClientHasHost(client3, this._testReadWriteHosts[2]);
                AssertClientHasHost(client4, this._testReadWriteHosts[3]);
                AssertClientHasHost(client5, this._testReadWriteHosts[0]);
            }
        }

        [Test]
        public void Does_throw_TimeoutException_when_PoolTimeout_exceeded() {
            var config = new RedisClientManagerConfig {
                MaxWritePoolSize = 4,
                MaxReadPoolSize = 4,
                AutoStart = false
            };
            using (var manager = new PooledRedisClientManager(this._testReadWriteHosts, this._testReadOnlyHosts, config)) {
                manager.PoolTimeout = 100;
                manager.Start();

                List<IRedisClient> masters = Enumerable.Range(0, 4).Select(i => manager.GetClient()).ToList();
                Console.WriteLine(masters.Count);

                try {
                    manager.GetClient();
                    Assert.Fail("Should throw TimeoutException");
                } catch (TimeoutException ex) {
                    Assert.That(ex.Message, Does.StartWith("Redis Timeout expired."));
                }

                List<IRedisClient> slaves = Enumerable.Range(0, 4).Select(i => manager.GetReadOnlyClient()).ToList();
                Console.WriteLine(slaves.Count);

                try {
                    manager.GetReadOnlyClient();
                    Assert.Fail("Should throw TimeoutException");
                } catch (TimeoutException ex) {
                    Assert.That(ex.Message, Does.StartWith("Redis Timeout expired."));
                }
            }
        }

    }

}
