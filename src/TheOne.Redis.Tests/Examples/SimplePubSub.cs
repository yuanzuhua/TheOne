using System;
using System.Threading;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.PubSub;

namespace TheOne.Redis.Tests.Examples {

    [TestFixture]
    internal sealed class SimplePubSub {

        private const string _channelName = "SimplePubSubCHANNEL";
        private const string _messagePrefix = "MESSAGE ";
        private const int _publishMessageCount = 5;

        [OneTimeSetUp]
        public void OneTimeSetUp() {
            using (var redis = new RedisClient(Config.MasterHost)) {
                redis.FlushAll();
            }
        }

        [Test]
        public void Publish_5_messages_to_3_clients() {
            const int noOfClients = 3;

            for (var i = 1; i <= noOfClients; i++) {
                var clientNo = i;
                ThreadPool.QueueUserWorkItem(x => {
                    using (var redisConsumer = new RedisClient(Config.MasterHost)) {
                        using (IRedisSubscription subscription = redisConsumer.CreateSubscription()) {
                            var messagesReceived = 0;
                            subscription.OnSubscribe = channel => {
                                Console.WriteLine("Client #{0} Subscribed to '{1}'", clientNo, channel);
                            };
                            subscription.OnUnSubscribe = channel => {
                                Console.WriteLine("Client #{0} UnSubscribed from '{1}'", clientNo, channel);
                            };
                            subscription.OnMessage = (channel, msg) => {
                                Console.WriteLine("Client #{0} Received '{1}' from channel '{2}'", clientNo, msg, channel);

                                if (++messagesReceived == _publishMessageCount) {
                                    subscription.UnSubscribeFromAllChannels();
                                }
                            };

                            Console.WriteLine("Client #{0} started Listening On '{1}'", clientNo, _channelName);
                            subscription.SubscribeToChannels(_channelName); // blocking
                        }
                    }

                    Console.WriteLine("Client #{0} EOF", clientNo);
                });
            }

            using (var redisClient = new RedisClient(Config.MasterHost)) {
                Thread.Sleep(500);
                Console.WriteLine("Begin publishing messages...");

                for (var i = 1; i <= _publishMessageCount; i++) {
                    var message = _messagePrefix + i;
                    Console.WriteLine("Publishing '{0}' to '{1}'", message, _channelName);
                    redisClient.PublishMessage(_channelName, message);
                }
            }

            Thread.Sleep(500);
        }

        [Test]
        public void Publish_and_receive_5_messages() {
            var messagesReceived = 0;

            using (var redisConsumer = new RedisClient(Config.MasterHost)) {
                using (IRedisSubscription subscription = redisConsumer.CreateSubscription()) {
                    subscription.OnSubscribe = channel => {
                        Console.WriteLine("Subscribed to '{0}'", channel);
                    };
                    subscription.OnUnSubscribe = channel => {
                        Console.WriteLine("UnSubscribed from '{0}'", channel);
                    };
                    subscription.OnMessage = (channel, msg) => {
                        Console.WriteLine("Received '{0}' from channel '{1}'", msg, channel);

                        // As soon as we've received all 5 messages, disconnect by unsubscribing to all channels
                        if (++messagesReceived == _publishMessageCount) {
                            subscription.UnSubscribeFromAllChannels();
                        }
                    };

                    ThreadPool.QueueUserWorkItem(x => {
                        Thread.Sleep(200);
                        Console.WriteLine("Begin publishing messages...");

                        using (var redisPublisher = new RedisClient(Config.MasterHost)) {
                            for (var i = 1; i <= _publishMessageCount; i++) {
                                var message = _messagePrefix + i;
                                Console.WriteLine("Publishing '{0}' to '{1}'", message, _channelName);
                                redisPublisher.PublishMessage(_channelName, message);
                            }
                        }
                    });

                    Console.WriteLine("Started Listening On '{0}'", _channelName);
                    subscription.SubscribeToChannels(_channelName); // blocking
                }
            }

            Console.WriteLine("EOF");
        }

    }

}
