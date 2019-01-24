using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using TheOne.Redis.ClientManager;
using TheOne.Redis.PubSub;

namespace TheOne.Redis.Tests.ConsoleTests {

    [TestFixture]
    [Ignore("too long")]
    internal sealed class LongRunningRedisPubSubServer : RedisTestBase {

        private const string _channel = "longrunningtest";
        private static DateTime _startedAt;
        private static long _messagesSent;
        private static long _heartbeatsSent;
        private static long _heartbeatsReceived;
        private static long _startCount;
        private static long _stopCount;
        private static long _disposeCount;
        private static long _errorCount;
        private static long _failoverCount;
        private static long _unSubscribeCount;
        private static RedisManagerPool _manager;
        private static RedisPubSubServer _pubSubServer;

        private static void OnInterval(object state) {
            Task.Factory.StartNew(PublishMessage);
        }

        private static void PublishMessage() {
            try {
                var message = "MSG: #" + Interlocked.Increment(ref _messagesSent);
                Console.WriteLine("PublishMessage(): " + message);
                using (var redis = _manager.GetClient()) {
                    redis.PublishMessage(_channel, message);
                }
            } catch (Exception ex) {
                Console.WriteLine("ERROR PublishMessage: " + ex);
            }
        }

        [Test]
        public void Execute() {
            _manager = new RedisManagerPool(Config.MasterHost);
            _startedAt = DateTime.Now;

            var q = new Timer(OnInterval, null, 0, 1000);
            Console.WriteLine(q);

            using (_pubSubServer = new RedisPubSubServer(_manager, _channel) {
                OnStart = () => {
                    Console.WriteLine("OnStart: #" + Interlocked.Increment(ref _startCount));
                },
                OnHeartbeatSent = () => {
                    Console.WriteLine("OnHeartbeatSent: #" + Interlocked.Increment(ref _heartbeatsSent));
                },
                OnHeartbeatReceived = () => {
                    Console.WriteLine("OnHeartbeatReceived: #" + Interlocked.Increment(ref _heartbeatsReceived));
                },
                OnMessage = (channel, msg) => {
                    Console.WriteLine("OnMessage: @" + channel + ": " + msg);
                },
                OnStop = () => {
                    Console.WriteLine("OnStop: #" + Interlocked.Increment(ref _stopCount));
                },
                OnError = ex => {
                    Console.WriteLine("OnError: #" + Interlocked.Increment(ref _errorCount) + " ERROR: " + ex);
                },
                OnFailover = server => {
                    Console.WriteLine("OnFailover: #" + Interlocked.Increment(ref _failoverCount));
                },
                OnDispose = () => {
                    Console.WriteLine("OnDispose: #" + Interlocked.Increment(ref _disposeCount));
                },
                OnUnSubscribe = channel => {
                    Console.WriteLine("OnUnSubscribe: #" + Interlocked.Increment(ref _unSubscribeCount) + " channel: " + channel);
                }
            }) {
                Console.WriteLine("PubSubServer StartedAt: " + _startedAt.ToLongTimeString());
                _pubSubServer.Start();

                Thread.Sleep(TimeSpan.FromSeconds(10000));

                Console.WriteLine("PubSubServer EndedAt: " + DateTime.Now.ToLongTimeString());
                Console.WriteLine("PubSubServer TimeTaken: " + (DateTime.Now - _startedAt).TotalSeconds + "s");
            }
        }

    }

}
