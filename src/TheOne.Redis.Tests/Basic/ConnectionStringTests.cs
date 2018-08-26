using System.Collections.Generic;
using System.Linq;
using System.Net;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.ClientManager;

namespace TheOne.Redis.Tests.Basic {

    [TestFixture]
    internal sealed class ConnectionStringTests : RedisTestBase {

        [OneTimeSetUp]
        public void OneTimeSetUp() {
            RedisConfig.VerifyMasterConnections = false;
        }

        [OneTimeTearDown]
        public void OneTimeTearDown() {
            RedisConfig.VerifyMasterConnections = true;
        }

        private static void AssertClientManager(IRedisClientManager redisManager, RedisEndpoint expected) {
            using (var readWrite = (RedisClient)redisManager.GetClient()) {
                using (var readOnly = (RedisClient)redisManager.GetReadOnlyClient()) {
                    using (var cacheClientWrapper = (RedisClientManagerCacheClient)redisManager.GetCacheClient()) {
                        AssertClient(readWrite, expected);
                        AssertClient(readOnly, expected);

                        using (var cacheClient = (RedisClient)cacheClientWrapper.GetClient()) {
                            AssertClient(cacheClient, expected);
                        }
                    }
                }
            }
        }

        private static void AssertClient(RedisClient redis, RedisEndpoint expected) {
            Assert.That(redis.Host, Is.EqualTo(expected.Host));
            Assert.That(redis.Port, Is.EqualTo(expected.Port));
            Assert.That(redis.Ssl, Is.EqualTo(expected.Ssl));
            Assert.That(redis.Client, Is.EqualTo(expected.Client));
            Assert.That(redis.Password, Is.EqualTo(expected.Password));
            Assert.That(redis.Db, Is.EqualTo(expected.Db));
            Assert.That(redis.ConnectTimeout, Is.EqualTo(expected.ConnectTimeout));
            Assert.That(redis.SendTimeout, Is.EqualTo(expected.SendTimeout));
            Assert.That(redis.ReceiveTimeout, Is.EqualTo(expected.ReceiveTimeout));
            Assert.That(redis.RetryTimeout, Is.EqualTo(expected.RetryTimeout));
            Assert.That(redis.IdleTimeOutSecs, Is.EqualTo(expected.IdleTimeOutSecs));
            Assert.That(redis.NamespacePrefix, Is.EqualTo(expected.NamespacePrefix));
        }

        [Test]
        public void Can_connect_to_redis_with_password_with_equals() {
            var connString = Config.Localhost + "?password=" + WebUtility.UrlEncode(Config.LocalhostPassword);
            var redisManager = new PooledRedisClientManager(connString);
            using (IRedisClient redis = redisManager.GetClient()) {
                Assert.That(redis.Password, Is.EqualTo(Config.LocalhostPassword));
            }
        }

        [Test]
        public void Can_connect_to_Slaves_and_Masters_with_Password() {
            var factory = new PooledRedisClientManager(new[] { Config.MasterHost }, new[] { Config.SlaveHost });

            using (IRedisClient readWrite = factory.GetClient()) {
                using (IRedisClient readOnly = factory.GetReadOnlyClient()) {
                    readWrite.SetValue("Foo", "Bar");
                    var value = readOnly.GetValue("Foo");

                    Assert.That(value, Is.EqualTo("Bar"));
                }
            }
        }

        [Test]
        public void Can_Parse_Host() {
            var hosts = new[] { "pass@host.com:6123" };
            List<RedisEndpoint> endPoints = RedisEndpoint.Create(hosts);

            Assert.AreEqual(1, endPoints.Count);
            RedisEndpoint ep = endPoints[0];

            Assert.AreEqual("host.com", ep.Host);
            Assert.AreEqual(6123, ep.Port);
            Assert.AreEqual("pass", ep.Password);
        }

        [Test]
        public void Can_use_password_with_equals() {
            var connString = "127.0.0.1?password=" + WebUtility.UrlEncode("p@55w0rd=");

            RedisEndpoint config = RedisEndpoint.Create(connString);
            Assert.That(config.Password, Is.EqualTo("p@55w0rd="));
        }

        [Test]
        public void Does_encode_values_when_serializing_to_ConnectionString() {
            var config = new RedisEndpoint {
                Host = "host",
                Port = 1,
                Password = "p@55W0rd="
            };

            var connString = config.ToString();
            Assert.That(connString, Is.EqualTo("host:1?Password=p%4055W0rd%3D"));

            RedisEndpoint fromConfig = RedisEndpoint.Create(connString);
            Assert.That(fromConfig.Host, Is.EqualTo(config.Host));
            Assert.That(fromConfig.Port, Is.EqualTo(config.Port));
            Assert.That(fromConfig.Password, Is.EqualTo(config.Password));
        }

        [Test]
        public void Does_retry_failed_commands_auth() {
            RedisStats.Reset();

            var redisCtrl = new RedisClient(Config.MasterHost);
            redisCtrl.FlushAll();
            redisCtrl.SetClient("redisCtrl");

            var redis = new RedisClient(Config.MasterHost);
            redis.SetClient("redisRetry");

            List<Dictionary<string, string>> clientInfo = redisCtrl.GetClientsInfo();
            var redisId = clientInfo.First(m => m["name"] == "redisRetry")["id"];
            Assert.That(redisId.Length, Is.GreaterThan(0));

            Assert.That(redis.IncrementValue("retryCounter"), Is.EqualTo(1));

            redis.OnBeforeFlush = () => {
                redisCtrl.KillClients(withId: redisId);
            };

            Assert.That(redis.IncrementValue("retryCounter"), Is.EqualTo(2));
            Assert.That(redis.Get<int>("retryCounter"), Is.EqualTo(2));

            Assert.That(RedisStats.TotalRetryCount, Is.EqualTo(1));
            Assert.That(RedisStats.TotalRetrySuccess, Is.EqualTo(1));
            Assert.That(RedisStats.TotalRetryTimedout, Is.EqualTo(0));
        }

        [Test]
        [TestCase("host", "host:6379")]
        [TestCase("redis://host", "host:6379")]
        [TestCase("host:1", "host:1")]
        [TestCase("pass@host:1", "host:1?Password=pass")]
        [TestCase("nunit:pass@host:1", "host:1?Client=nunit&Password=pass")]
        [TestCase("host:1?password=pass&client=nunit", "host:1?Client=nunit&Password=pass")]
        [TestCase("host:1?db=2", "host:1?Db=2")]
        [TestCase("host?ssl=true", "host:6380?Ssl=true")]
        [TestCase("host:1?ssl=true", "host:1?Ssl=true")]
        [TestCase("host:1?connectTimeout=1&sendtimeout=2&receiveTimeout=3&idletimeoutsecs=4",
            "host:1?ConnectTimeout=1&SendTimeout=2&ReceiveTimeout=3&IdleTimeOutSecs=4")]
        [TestCase(
            "redis://nunit:pass@host:1?ssl=true&db=1&connectTimeout=2&sendtimeout=3&receiveTimeout=4&idletimeoutsecs=5&NamespacePrefix=prefix.",
            "host:1?Client=nunit&Password=pass&Db=1&Ssl=true&ConnectTimeout=2&SendTimeout=3&ReceiveTimeout=4&IdleTimeOutSecs=5&NamespacePrefix=prefix.")]
        public void Does_Serialize_RedisEndpoint(string connString, string expectedString) {
            RedisEndpoint actual = RedisEndpoint.Create(connString);
            Assert.That(actual.ToString(), Is.EqualTo(expectedString));
        }

        [Test]
        public void Does_set_all_properties_on_Client_using_ClientsManagers() {
            var connStr =
                "redis://nunit:pass@host:1?ssl=true&db=0&connectTimeout=2&sendtimeout=3&receiveTimeout=4&idletimeoutsecs=5&NamespacePrefix=prefix.";
            var expected = new RedisEndpoint {
                Host = "host",
                Port = 1,
                Ssl = true,
                Client = "nunit",
                Password = "pass",
                Db = 0,
                ConnectTimeout = 2,
                SendTimeout = 3,
                ReceiveTimeout = 4,
                IdleTimeOutSecs = 5,
                NamespacePrefix = "prefix."
            };

            using (var pooledManager = new RedisManagerPool(connStr)) {
                AssertClientManager(pooledManager, expected);
            }

            using (var pooledManager = new PooledRedisClientManager(connStr)) {
                AssertClientManager(pooledManager, expected);
            }

            using (var basicManager = new BasicRedisClientManager(connStr)) {
                AssertClientManager(basicManager, expected);
            }
        }

        [Test]
        public void Does_set_Client_name_on_Connection() {
            using (var redis = new RedisClient(Config.MasterHost + "?Client=nunit")) {
                var clientName = redis.GetClient();

                Assert.That(clientName, Is.EqualTo("nunit"));
            }
        }

        [Test]
        public void Does_set_Client_on_Pooled_Connection() {
            using (var redisManager = new PooledRedisClientManager(Config.MasterHost + "?Client=nunit")) {
                using (IRedisClient redis = redisManager.GetClient()) {
                    var clientName = redis.GetClient();

                    Assert.That(clientName, Is.EqualTo("nunit"));
                }
            }
        }

        [Test]
        public void Host_May_Contain_AtChar() {
            var hosts = new[] { "@pa1@ss@localhost:6123" };
            List<RedisEndpoint> endPoints = RedisEndpoint.Create(hosts);

            Assert.AreEqual(1, endPoints.Count);
            RedisEndpoint ep = endPoints[0];

            Assert.AreEqual("@pa1@ss", ep.Password);
            Assert.AreEqual("localhost", ep.Host);
            Assert.AreEqual(6123, ep.Port);
        }

        [Test]
        public void Passwords_are_not_leaked_in_exception_messages() {
            const string password = "yesterdayspassword";

            void TestDelegate() {
                try {
                    // redis will throw when using password and it's not configured
                    var factory = new PooledRedisClientManager(password + "@" + Config.MasterHost);
                    using (IRedisClient redis = factory.GetClient()) {
                        redis.SetValue("Foo", "Bar");
                    }
                } catch (RedisResponseException ex) {
                    Assert.That(ex.Message, Is.Not.Contains(password));
                    throw;
                }
            }

            Assert.Throws<RedisResponseException>(TestDelegate,
                "Expected an exception after Redis AUTH command; try using a password that doesn't match.");
        }

    }

}
