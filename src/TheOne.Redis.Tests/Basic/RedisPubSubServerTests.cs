using System;
using System.Linq;
using System.Threading;
using NUnit.Framework;
using TheOne.Redis.ClientManager;
using TheOne.Redis.PubSub;

namespace TheOne.Redis.Tests.Basic {

    [TestFixture]
    internal sealed class RedisPubSubServerTests : RedisTestBase {

        private RedisPubSubServer CreatePubSubServer(int intervalSecs = 1, int timeoutSecs = 3) {
            var clientManager = new PooledRedisClientManager(Config.MasterHost);
            using (var redis = clientManager.GetClient()) {
                redis.FlushAll();
            }

            var pubSub = new RedisPubSubServer(
                clientManager,
                "topic:test") {
                HeartbeatInterval = TimeSpan.FromSeconds(intervalSecs),
                HeartbeatTimeout = TimeSpan.FromSeconds(timeoutSecs)
            };

            return pubSub;
        }

        [Test]
        public void Does_restart_when_Heartbeat_Timeout_exceeded() {
            // This auto restarts 2 times before letting connection to stay alive

            var pulseCount = 0;
            var startCount = 0;
            var stopCount = 0;

            using (var pubSub = this.CreatePubSubServer()) {
                pubSub.OnStart = () => Console.WriteLine("start #{0}", ++startCount);
                pubSub.OnStop = () => Console.WriteLine("stop #{0}", ++stopCount);
                pubSub.OnHeartbeatReceived = () => Console.WriteLine("pulse #{0}", ++pulseCount);

                // pause longer than heartbeat timeout so auto reconnects
                pubSub.OnControlCommand = op => {
                    if (op == "PULSE" && stopCount < 2) {
                        Thread.Sleep(4000);
                    }
                };

                pubSub.Start();

                Thread.Sleep(33 * 1000);

                Assert.That(pulseCount, Is.GreaterThan(3));
                Assert.That(startCount, Is.EqualTo(3));
                Assert.That(stopCount, Is.EqualTo(2));
            }
        }

        [Test]
        public void Does_send_heartbeat_pulses() {
            var pulseCount = 0;
            using (var pubSub = this.CreatePubSubServer(1, 3)) {
                pubSub.OnHeartbeatReceived = () => {
                    Console.WriteLine("pulse #{0}", pulseCount++);
                };
                pubSub.Start();

                Thread.Sleep(5200);

                Assert.That(pulseCount, Is.GreaterThan(2));
            }
        }

        [Test]
        public void Does_send_heartbeat_pulses_to_multiple_PubSubServers() {
            var count = 15;

            var pulseCount = 0;
            var pubSubs = Enumerable.Range(0, count).Select(i => {
                var pubSub = this.CreatePubSubServer(3, 30);
                pubSub.OnHeartbeatReceived = () => {
                    Console.WriteLine("{0}: pulse #{1}", i, pulseCount++);
                };
                pubSub.Start();
                return pubSub;
            }).ToList();

            Thread.Sleep(11000);

            Console.WriteLine("pulseCount = {0}", pulseCount);

            Assert.That(pulseCount, Is.GreaterThan(2 * count));
            Assert.That(pulseCount, Is.LessThan(8 * count));

            foreach (var value in pubSubs) {
                value?.Dispose();
            }
        }

    }

}
