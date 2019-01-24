using System;
using System.Collections.Generic;
using System.Threading;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Common;

namespace TheOne.Redis.Tests.Basic {

    [TestFixture]
    internal sealed class RedisPubSubTests : RedisClientTestsBase {

        public override void SetUp() {
            base.SetUp();
            this.Redis.NamespacePrefix = "RedisPubSubTests";
        }

        [Test]
        public void Can_Subscribe_and_Publish_message_to_multiple_channels() {
            var channelPrefix = this.PrefixedKey("CHANNEL3 ");
            const string message = "MESSAGE";
            const int publishChannelCount = 5;
            var key = this.PrefixedKey("Can_Subscribe_and_Publish_message_to_multiple_channels");

            var channels = new List<string>();
            for (var i = 0; i < publishChannelCount; i++) {
                channels.Add(channelPrefix + i);
            }

            var messagesReceived = 0;
            var channelsSubscribed = 0;
            var channelsUnSubscribed = 0;

            this.Redis.IncrementValue(key);

            using (var subscription = this.Redis.CreateSubscription()) {
                subscription.OnSubscribe = channel => {
                    Console.WriteLine("Subscribed to '{0}'", channel);
                    Assert.That(channel, Is.EqualTo(channelPrefix + channelsSubscribed++));
                };
                subscription.OnUnSubscribe = channel => {
                    Console.WriteLine("UnSubscribed from '{0}'", channel);
                    Assert.That(channel, Is.EqualTo(channelPrefix + channelsUnSubscribed++));
                };
                subscription.OnMessage = (channel, msg) => {
                    Console.WriteLine("Received '{0}' from channel '{1}'", msg, channel);
                    Assert.That(channel, Is.EqualTo(channelPrefix + messagesReceived++));
                    Assert.That(msg, Is.EqualTo(message));

                    subscription.UnSubscribeFromChannels(channel);
                };

                ThreadPool.QueueUserWorkItem(x => {
                    Thread.Sleep(100); // to be sure that we have subscribers

                    using (var redisClient = new RedisClient(Config.MasterHost)) {
                        foreach (var channel in channels) {
                            Console.WriteLine("Publishing '{0}' to '{1}'", message, channel);
                            redisClient.PublishMessage(channel, message);
                        }
                    }
                });

                Console.WriteLine("Start Listening On");
                subscription.SubscribeToChannels(channels.ToArray()); // blocking
            }

            Console.WriteLine("Using as normal client again...");
            this.Redis.IncrementValue(key);
            Assert.That(this.Redis.Get<int>(key), Is.EqualTo(2));

            Assert.That(messagesReceived, Is.EqualTo(publishChannelCount));
            Assert.That(channelsSubscribed, Is.EqualTo(publishChannelCount));
            Assert.That(channelsUnSubscribed, Is.EqualTo(publishChannelCount));
        }

        [Test]
        public void Can_Subscribe_and_Publish_multiple_message() {
            var channelName = this.PrefixedKey("CHANNEL2");
            const string messagePrefix = "MESSAGE ";
            var key = this.PrefixedKey("Can_Subscribe_and_Publish_multiple_message");
            const int publishMessageCount = 5;
            var messagesReceived = 0;

            this.Redis.IncrementValue(key);

            using (var subscription = this.Redis.CreateSubscription()) {
                subscription.OnSubscribe = channel => {
                    Console.WriteLine("Subscribed to '{0}'", channel);
                    Assert.That(channel, Is.EqualTo(channelName));
                };
                subscription.OnUnSubscribe = channel => {
                    Console.WriteLine("UnSubscribed from '{0}'", channel);
                    Assert.That(channel, Is.EqualTo(channelName));
                };
                subscription.OnMessage = (channel, msg) => {
                    Console.WriteLine("Received '{0}' from channel '{1}'", msg, channel);
                    Assert.That(channel, Is.EqualTo(channelName));
                    Assert.That(msg, Is.EqualTo(messagePrefix + messagesReceived++));

                    if (messagesReceived == publishMessageCount) {
                        subscription.UnSubscribeFromAllChannels();
                    }
                };

                ThreadPool.QueueUserWorkItem(x => {
                    Thread.Sleep(100); // to be sure that we have subscribers

                    using (var redisClient = new RedisClient(Config.MasterHost)) {
                        for (var i = 0; i < publishMessageCount; i++) {
                            var message = messagePrefix + i;
                            Console.WriteLine("Publishing '{0}' to '{1}'", message, channelName);
                            redisClient.PublishMessage(channelName, message);
                        }
                    }
                });

                Console.WriteLine("Start Listening On");
                subscription.SubscribeToChannels(channelName); // blocking
            }

            Console.WriteLine("Using as normal client again...");
            this.Redis.IncrementValue(key);
            Assert.That(this.Redis.Get<int>(key), Is.EqualTo(2));

            Assert.That(messagesReceived, Is.EqualTo(publishMessageCount));
        }

        [Test]
        public void Can_Subscribe_and_Publish_single_message() {
            var channelName = this.PrefixedKey("CHANNEL1");
            const string message = "Hello, World!";
            var key = this.PrefixedKey("Can_Subscribe_and_Publish_single_message");

            this.Redis.IncrementValue(key);

            using (var subscription = this.Redis.CreateSubscription()) {
                subscription.OnSubscribe = channel => {
                    Console.WriteLine("Subscribed to '{0}'", channel);
                    Assert.That(channel, Is.EqualTo(channelName));
                };
                subscription.OnUnSubscribe = channel => {
                    Console.WriteLine("UnSubscribed from '{0}'", channel);
                    Assert.That(channel, Is.EqualTo(channelName));
                };
                subscription.OnMessage = (channel, msg) => {
                    Console.WriteLine("Received '{0}' from channel '{1}'", msg, channel);
                    Assert.That(channel, Is.EqualTo(channelName));
                    Assert.That(msg, Is.EqualTo(message));
                    subscription.UnSubscribeFromAllChannels();
                };

                ThreadPool.QueueUserWorkItem(x => {
                    Thread.Sleep(100); // to be sure that we have subscribers
                    using (var redisClient = new RedisClient(Config.MasterHost)) {
                        Console.WriteLine("Publishing '{0}' to '{1}'", message, channelName);
                        redisClient.PublishMessage(channelName, message);
                    }
                });

                Console.WriteLine("Start Listening On " + channelName);
                subscription.SubscribeToChannels(channelName); // blocking
            }

            Console.WriteLine("Using as normal client again...");
            this.Redis.IncrementValue(key);
            Assert.That(this.Redis.Get<int>(key), Is.EqualTo(2));
        }

        [Test]
        public void Can_Subscribe_and_Publish_single_message_using_wildcard() {
            var channelWildcard = this.PrefixedKey("CHANNEL.*");
            var channelName = this.PrefixedKey("CHANNEL.1");
            const string message = "Hello, World!";
            var key = this.PrefixedKey("Can_Subscribe_and_Publish_single_message");

            this.Redis.IncrementValue(key);

            using (var subscription = this.Redis.CreateSubscription()) {
                subscription.OnSubscribe = channel => {
                    Console.WriteLine("Subscribed to '{0}'", channelWildcard);
                    Assert.That(channel, Is.EqualTo(channelWildcard));
                };
                subscription.OnUnSubscribe = channel => {
                    Console.WriteLine("UnSubscribed from '{0}'", channelWildcard);
                    Assert.That(channel, Is.EqualTo(channelWildcard));
                };
                subscription.OnMessage = (channel, msg) => {
                    Console.WriteLine("Received '{0}' from channel '{1}'", msg, channel);
                    Assert.That(channel, Is.EqualTo(channelName));
                    Assert.That(msg, Is.EqualTo(message), "we should get the message, not the channel");
                    subscription.UnSubscribeFromChannelsMatching();
                };

                ThreadPool.QueueUserWorkItem(x => {
                    Thread.Sleep(100); // to be sure that we have subscribers
                    using (var redisClient = new RedisClient(Config.MasterHost)) {
                        Console.WriteLine("Publishing '{0}' to '{1}'", message, channelName);
                        redisClient.PublishMessage(channelName, message);
                    }
                });

                Console.WriteLine("Start Listening On " + channelName);
                subscription.SubscribeToChannelsMatching(channelWildcard); // blocking
            }

            Console.WriteLine("Using as normal client again...");
            this.Redis.IncrementValue(key);
            Assert.That(this.Redis.Get<int>(key), Is.EqualTo(2));
        }

        [Test]
        public void Can_Subscribe_to_channel_pattern() {
            var msgs = 0;
            using (var subscription = this.Redis.CreateSubscription()) {
                subscription.OnMessage = (channel, msg) => {
                    Console.WriteLine("{0}: {1}", channel, msg + msgs++);
                    subscription.UnSubscribeFromChannelsMatching(this.PrefixedKey("CHANNEL4:TITLE*"));
                };

                ThreadPool.QueueUserWorkItem(x => {
                    Thread.Sleep(100); // to be sure that we have subscribers

                    using (var redisClient = new RedisClient(Config.MasterHost)) {
                        Console.WriteLine("Publishing msg...");
                        redisClient.Publish(this.PrefixedKey("CHANNEL4:TITLE1"), "hello".ToUtf8Bytes());
                    }
                });

                Console.WriteLine("Start Listening On");
                subscription.SubscribeToChannelsMatching(this.PrefixedKey("CHANNEL4:TITLE*"));
            }
        }

        [Test]
        public void Can_Subscribe_to_MultipleChannel_pattern() {
            string[] channels = { this.PrefixedKey("CHANNEL5:TITLE*"), this.PrefixedKey("CHANNEL5:BODY*") };
            var msgs = 0;
            using (var subscription = this.Redis.CreateSubscription()) {
                subscription.OnMessage = (channel, msg) => {
                    Console.WriteLine("{0}: {1}", channel, msg + msgs++);
                    subscription.UnSubscribeFromChannelsMatching(channels);
                };

                ThreadPool.QueueUserWorkItem(x => {
                    Thread.Sleep(100); // to be sure that we have subscribers

                    using (var redisClient = new RedisClient(Config.MasterHost)) {
                        Console.WriteLine("Publishing msg...");
                        redisClient.Publish(this.PrefixedKey("CHANNEL5:BODY"), "hello".ToUtf8Bytes());
                    }
                });

                Console.WriteLine("Start Listening On");
                subscription.SubscribeToChannelsMatching(channels);
            }
        }

    }

}
