using System;
using System.Collections.Generic;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.ClientManager;
using TheOne.Redis.Common;

namespace TheOne.Redis.Tests.Sentinel {

    [TestFixture]
    internal sealed class RedisResolverTests : RedisTestBase {

        #region Models

        public class FixedResolver : IRedisResolver {

            private readonly RedisEndpoint _master;
            private readonly RedisEndpoint _slave;
            public int NewClientsInitialized;

            public FixedResolver(RedisEndpoint master, RedisEndpoint slave) {
                this._master = master;
                this._slave = slave;
                this.ClientFactory = RedisConfig.ClientFactory;
            }

            public Func<RedisEndpoint, RedisClient> ClientFactory { get; set; }

            public int ReadWriteHostsCount => 1;
            public int ReadOnlyHostsCount => 1;

            public void ResetMasters(IEnumerable<string> hosts) { }
            public void ResetSlaves(IEnumerable<string> hosts) { }

            public RedisClient CreateMasterClient(int desiredIndex) {
                return this.CreateRedisClient(this._master, true);
            }

            public RedisClient CreateSlaveClient(int desiredIndex) {
                return this.CreateRedisClient(this._slave, false);
            }

            public RedisClient CreateRedisClient(RedisEndpoint config, bool master) {
                this.NewClientsInitialized++;
                return this.ClientFactory(config);
            }

        }

        #endregion

        private static void InitializeEmptyRedisManagers(IRedisClientManager redisManager, string[] masters, string[] slaves) {
            var hasResolver = (IHasRedisResolver)redisManager;
            hasResolver.RedisResolver.ResetMasters(masters);
            hasResolver.RedisResolver.ResetSlaves(slaves);

            using (IRedisClient master = redisManager.GetClient()) {
                Assert.That(master.GetHostString(), Is.EqualTo(Config.Sentinel6380));
                master.SetValue("KEY", "1");
            }

            using (IRedisClient slave = redisManager.GetReadOnlyClient()) {
                Assert.That(slave.GetHostString(), Is.EqualTo(Config.Sentinel6380).Or.EqualTo(Config.Sentinel6381));
                Assert.That(slave.GetValue("KEY"), Is.EqualTo("1"));
            }
        }

        [Test]
        public void BasicRedisClientManager_alternates_hosts() {
            using (var redisManager = new BasicRedisClientManager(Config.MasterHosts, Config.SlaveHosts)) {
                using (IRedisClient master = redisManager.GetClient()) {
                    Assert.That(master.GetHostString(), Is.EqualTo(Config.Sentinel6380));
                    master.SetValue("KEY", "1");
                }

                using (IRedisClient master = redisManager.GetClient()) {
                    Assert.That(master.GetHostString(), Is.EqualTo(Config.Sentinel6380));
                    master.Increment("KEY", 1);
                }

                for (var i = 0; i < 5; i++) {
                    using (IRedisClient readOnly = redisManager.GetReadOnlyClient()) {
                        Assert.That(readOnly.GetHostString(), Is.EqualTo(Config.Sentinel6381).Or.EqualTo(Config.Sentinel6382));
                        Assert.That(readOnly.GetValue("KEY"), Is.EqualTo("2"));
                    }
                }

                using (ICacheClient cache = redisManager.GetCacheClient()) {
                    Assert.That(cache.Get<string>("KEY"), Is.EqualTo("2"));
                }
            }
        }

        [Test]
        public void Can_initialize_ClientManagers_with_no_hosts() {
            InitializeEmptyRedisManagers(new PooledRedisClientManager(), Config.MasterHosts, Config.SlaveHosts);
            InitializeEmptyRedisManagers(new RedisManagerPool(), Config.MasterHosts, Config.MasterHosts);
            InitializeEmptyRedisManagers(new BasicRedisClientManager(), Config.MasterHosts, Config.SlaveHosts);
        }

        [Test]
        public void PooledRedisClientManager_alternates_hosts() {
            using (var redisManager = new PooledRedisClientManager(Config.MasterHosts, Config.SlaveHosts)) {
                using (IRedisClient master = redisManager.GetClient()) {
                    Assert.That(master.GetHostString(), Is.EqualTo(Config.Sentinel6380));
                    master.SetValue("KEY", "1");
                }

                using (IRedisClient master = redisManager.GetClient()) {
                    Assert.That(master.GetHostString(), Is.EqualTo(Config.Sentinel6380));
                    master.Increment("KEY", 1);
                }

                for (var i = 0; i < 5; i++) {
                    using (IRedisClient readOnly = redisManager.GetReadOnlyClient()) {
                        Assert.That(readOnly.GetHostString(), Is.EqualTo(Config.Sentinel6381).Or.EqualTo(Config.Sentinel6382));
                        Assert.That(readOnly.GetValue("KEY"), Is.EqualTo("2"));
                    }
                }

                using (ICacheClient cache = redisManager.GetCacheClient()) {
                    Assert.That(cache.Get<string>("KEY"), Is.EqualTo("2"));
                }
            }
        }

        [Test]
        public void PooledRedisClientManager_can_execute_CustomResolver() {
            var resolver = new FixedResolver(RedisEndpoint.Create(Config.Sentinel6380), RedisEndpoint.Create(Config.Sentinel6381));
            using (var redisManager = new PooledRedisClientManager("127.0.0.1:8888") {
                RedisResolver = resolver
            }) {
                using (IRedisClient master = redisManager.GetClient()) {
                    Assert.That(master.GetHostString(), Is.EqualTo(Config.Sentinel6380));
                    master.SetValue("KEY", "1");
                }

                using (IRedisClient master = redisManager.GetClient()) {
                    Assert.That(master.GetHostString(), Is.EqualTo(Config.Sentinel6380));
                    master.Increment("KEY", 1);
                }

                Assert.That(resolver.NewClientsInitialized, Is.EqualTo(1));

                for (var i = 0; i < 5; i++) {
                    using (IRedisClient slave = redisManager.GetReadOnlyClient()) {
                        Assert.That(slave.GetHostString(), Is.EqualTo(Config.Sentinel6381));
                        Assert.That(slave.GetValue("KEY"), Is.EqualTo("2"));
                    }
                }

                Assert.That(resolver.NewClientsInitialized, Is.EqualTo(2));

                redisManager.FailoverTo("127.0.0.1:9999", "127.0.0.1:9999");

                for (var i = 0; i < 5; i++) {
                    using (IRedisClient master = redisManager.GetClient()) {
                        Assert.That(master.GetHostString(), Is.EqualTo(Config.Sentinel6380));
                        Assert.That(master.GetValue("KEY"), Is.EqualTo("2"));
                    }

                    using (IRedisClient slave = redisManager.GetReadOnlyClient()) {
                        Assert.That(slave.GetHostString(), Is.EqualTo(Config.Sentinel6381));
                        Assert.That(slave.GetValue("KEY"), Is.EqualTo("2"));
                    }
                }

                Assert.That(resolver.NewClientsInitialized, Is.EqualTo(4));
            }
        }

        [Test]
        public void RedisManagerPool_alternates_hosts() {
            using (var redisManager = new RedisManagerPool(Config.MasterHosts)) {
                using (IRedisClient master = redisManager.GetClient()) {
                    Assert.That(master.GetHostString(), Is.EqualTo(Config.Sentinel6380));
                    master.SetValue("KEY", "1");
                }

                using (IRedisClient master = redisManager.GetClient()) {
                    Assert.That(master.GetHostString(), Is.EqualTo(Config.Sentinel6380));
                    master.Increment("KEY", 1);
                }

                for (var i = 0; i < 5; i++) {
                    using (IRedisClient readOnly = redisManager.GetReadOnlyClient()) {
                        Assert.That(readOnly.GetHostString(), Is.EqualTo(Config.Sentinel6380));
                        Assert.That(readOnly.GetValue("KEY"), Is.EqualTo("2"));
                    }
                }

                using (ICacheClient cache = redisManager.GetCacheClient()) {
                    Assert.That(cache.Get<string>("KEY"), Is.EqualTo("2"));
                }
            }
        }

        [Test]
        public void RedisManagerPool_can_execute_CustomResolver() {
            var resolver = new FixedResolver(RedisEndpoint.Create(Config.Sentinel6380), RedisEndpoint.Create(Config.Sentinel6381));
            using (var redisManager = new RedisManagerPool("127.0.0.1:8888") {
                RedisResolver = resolver
            }) {
                using (IRedisClient master = redisManager.GetClient()) {
                    Assert.That(master.GetHostString(), Is.EqualTo(Config.Sentinel6380));
                    master.SetValue("KEY", "1");
                }

                using (IRedisClient master = redisManager.GetClient()) {
                    Assert.That(master.GetHostString(), Is.EqualTo(Config.Sentinel6380));
                    master.Increment("KEY", 1);
                }

                Assert.That(resolver.NewClientsInitialized, Is.EqualTo(1));

                for (var i = 0; i < 5; i++) {
                    using (IRedisClient slave = redisManager.GetReadOnlyClient()) {
                        Assert.That(slave.GetHostString(), Is.EqualTo(Config.Sentinel6380));
                        Assert.That(slave.GetValue("KEY"), Is.EqualTo("2"));
                    }
                }

                Assert.That(resolver.NewClientsInitialized, Is.EqualTo(1));

                redisManager.FailoverTo("127.0.0.1:9999", "127.0.0.1:9999");

                for (var i = 0; i < 5; i++) {
                    using (IRedisClient master = redisManager.GetClient()) {
                        Assert.That(master.GetHostString(), Is.EqualTo(Config.Sentinel6380));
                        Assert.That(master.GetValue("KEY"), Is.EqualTo("2"));
                    }

                    using (IRedisClient slave = redisManager.GetReadOnlyClient()) {
                        Assert.That(slave.GetHostString(), Is.EqualTo(Config.Sentinel6380));
                        Assert.That(slave.GetValue("KEY"), Is.EqualTo("2"));
                    }
                }

                Assert.That(resolver.NewClientsInitialized, Is.EqualTo(2));
            }
        }

        [Test]
        public void RedisResolver_does_reset_when_detects_invalid_master() {
            string[] invalidMaster = { Config.Sentinel6381 };
            string[] invalidSlaves = { Config.Sentinel6380, Config.Sentinel6382 };

            using (var redisManager = new PooledRedisClientManager(invalidMaster, invalidSlaves)) {
                var resolver = (RedisResolver)redisManager.RedisResolver;

                using (IRedisClient master = redisManager.GetClient()) {
                    master.SetValue("KEY", "1");
                    Assert.That(master.GetHostString(), Is.EqualTo(Config.Sentinel6380));
                }

                using (IRedisClient master = redisManager.GetClient()) {
                    master.Increment("KEY", 1);
                    Assert.That(master.GetHostString(), Is.EqualTo(Config.Sentinel6380));
                }

                Console.WriteLine("Masters:");
                Console.WriteLine(resolver.Masters.ToJson());
                Console.WriteLine("Slaves:");
                Console.WriteLine(resolver.Slaves.ToJson());
            }
        }

    }

}
