using System;
using System.Threading;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Common;
using TheOne.Redis.Sentinel;

namespace TheOne.Redis.Tests.ConsoleTests {

    [TestFixture]
    [Ignore("Hurts master")]
    internal sealed class RedisSentinelFailoverTests : RedisTestBase {

        [Test]
        public void Execute() {
            RedisConfig.DefaultReceiveTimeout = 10000;

            using (var sentinel = new RedisSentinel(Config.SentinelHosts)) {
                // if (this._useRedisManagerPool) {
                //     sentinel.RedisManagerFactory = (masters, slaves) => new RedisManagerPool(masters);

                var redisManager = sentinel.Start();

                var i = 0;

                Timer clientTimer = null;

                void OnClientTimerOnElapsed(object state) {
                    Console.WriteLine();
                    Console.WriteLine("clientTimer.Elapsed: " + i++);

                    try {
                        string key = null;
                        using (var master = (RedisClient)redisManager.GetClient()) {
                            var counter = master.Increment("key", 1);
                            key = "key" + counter;
                            Console.WriteLine("Set key {0} in read/write client #{1}@{2}", key, master.Id, master.GetHostString());
                            master.SetValue(key, "value" + 1);
                        }

                        using (var readOnly = (RedisClient)redisManager.GetReadOnlyClient()) {
                            Console.WriteLine("Get key {0} in read-only client #{1}@{2}", key, readOnly.Id, readOnly.GetHostString());
                            var value = readOnly.GetValue(key);
                            Console.WriteLine("{0} = {1}", key, value);
                        }
                    } catch (ObjectDisposedException) {
                        Console.WriteLine("ObjectDisposedException detected, disposing timer...");
                        clientTimer?.Dispose();
                    } catch (Exception ex) {
                        Console.WriteLine("Error in Timer, {0}", ex);
                    }

                    if (i % 10 == 0) {
                        Console.WriteLine(RedisStats.ToDictionary().ToJson());
                    }
                }

                clientTimer = new Timer(OnClientTimerOnElapsed, null, 0, 1000);

                Console.WriteLine("Sleeping for 5000ms...");
                Thread.Sleep(5000);

                Console.WriteLine("Failing over master...");
                sentinel.ForceMasterFailover();
                Console.WriteLine("master was failed over");

                Console.WriteLine("Sleeping for 20000ms...");
                Thread.Sleep(20000);

                try {
                    var debugConfig = sentinel.GetMaster();
                    using (var master = new RedisClient(debugConfig)) {
                        Console.WriteLine("Putting master '{0}' to sleep for 35 seconds...", master.GetHostString());
                        master.DebugSleep(35);
                    }
                } catch (Exception ex) {
                    Console.WriteLine("Error retrieving master for DebugSleep(), {0}", ex);
                }

                Console.WriteLine("After DEBUG SLEEP... Sleeping for 5000ms...");
                Thread.Sleep(5000);

                Console.WriteLine("RedisStats:");
                Console.WriteLine(RedisStats.ToDictionary().ToJson());
            }
        }

    }

}
