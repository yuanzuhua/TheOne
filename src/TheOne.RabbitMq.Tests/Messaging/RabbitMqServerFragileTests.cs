using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using NUnit.Framework;
using RabbitMQ.Client;
using TheOne.RabbitMq.Extensions;
using TheOne.RabbitMq.Messaging;
using TheOne.RabbitMq.Models;
using TheOne.RabbitMq.Tests.Messaging.Models;

namespace TheOne.RabbitMq.Tests.Messaging {

    [TestFixture]
    internal sealed class RabbitMqServerFragileTests {

        [OneTimeTearDown]
        public void TestFixtureTearDown() {
            RabbitMqConfig.UsingChannel(channel => {
                channel.ExchangeDelete(MqQueueNames.Exchange);
                channel.ExchangeDelete(MqQueueNames.ExchangeDlq);
                channel.ExchangeDelete(MqQueueNames.ExchangeTopic);
                channel.DeleteQueue<Reverse>();
                channel.DeleteQueue<ReverseResponse>();
                channel.DeleteQueue<NothingHere>();
                channel.DeleteQueue<NothingHereResponse>();
                channel.DeleteQueue<AlwaysThrows>();
            });
        }

        [Test]
        [SuppressMessage("ReSharper", "AccessToDisposedClosure", Justification = "intentionally")]
        public void Does_process_all_messages_and_Starts_Stops_correctly_with_multiple_threads_racing() {
            using (var mqServer = RabbitMqServerTests.CreateMqServer()) {
                void Action0(IModel channel) {
                    channel.PurgeQueue<Reverse>();
                    channel.PurgeQueue<NothingHere>();
                }

                RabbitMqConfig.UsingChannel(mqServer.ConnectionFactory, Action0);

                var reverseCalled = 0;
                var nothingHereCalled = 0;

                mqServer.RegisterHandler<Reverse>(x => {
                    Console.WriteLine("Processing Reverse {0}...", Interlocked.Increment(ref reverseCalled));
                    return new ReverseResponse { Value = string.Join(",", x.GetBody().Value.Reverse()) };
                });
                mqServer.RegisterHandler<NothingHere>(x => {
                    Console.WriteLine("Processing NothingHere {0}...", Interlocked.Increment(ref nothingHereCalled));
                    return new NothingHereResponse { Value = x.GetBody().Value };
                });

                using (var mqClient = mqServer.CreateMessageQueueClient()) {
                    mqClient.Publish(new Reverse { Value = "Hello" });
                    mqClient.Publish(new Reverse { Value = "World" });
                    mqClient.Publish(new NothingHere { Value = "HelloWorld" });

                    mqServer.Start();

                    Thread.Sleep(1500);
                    Assert.That(mqServer.GetStatus(), Is.EqualTo("Started"));
                    Assert.That(mqServer.GetStats().TotalMessagesProcessed, Is.EqualTo(3));

                    mqClient.Publish(new Reverse { Value = "Foo" });
                    mqClient.Publish(new NothingHere { Value = "Bar" });

                    ExecUtils.ExecMultiThreading(10, () => mqServer.Start());
                    Assert.That(mqServer.GetStatus(), Is.EqualTo("Started"));

                    ExecUtils.ExecMultiThreading(5, () => mqServer.Stop());
                    Assert.That(mqServer.GetStatus(), Is.EqualTo("Stopped"));

                    ExecUtils.ExecMultiThreading(10, () => mqServer.Start());
                    Assert.That(mqServer.GetStatus(), Is.EqualTo("Started"));

                    Console.WriteLine("\n" + mqServer.GetStats());

                    Assert.That(mqServer.GetStats().TotalMessagesProcessed, Is.GreaterThanOrEqualTo(5));
                    Assert.That(reverseCalled, Is.EqualTo(3));
                    Assert.That(nothingHereCalled, Is.EqualTo(2));
                }
            }
        }

        [Test]
        [Explicit]
        public void Does_process_messages_sent_before_it_was_started() {
            var reverseCalled = 0;

            using (var mqServer = RabbitMqServerTests.CreateMqServer()) {
                void Action0(IModel channel) {
                    channel.PurgeQueue<Reverse>();
                    channel.PurgeQueue<ReverseResponse>();
                }

                RabbitMqConfig.UsingChannel(mqServer.ConnectionFactory, Action0);

                mqServer.RegisterHandler<Reverse>(x => {
                    Interlocked.Increment(ref reverseCalled);
                    return new ReverseResponse { Value = string.Join(",", x.GetBody().Value.Reverse()) };
                });

                using (var mqClient = mqServer.CreateMessageQueueClient()) {
                    RabbitMqServerTests.Publish_4_messages(mqClient);

                    mqServer.Start();

                    Thread.Sleep(2000);
                    Assert.That(mqServer.GetStats().TotalMessagesProcessed, Is.EqualTo(4));
                    Assert.That(reverseCalled, Is.EqualTo(4));
                }
            }
        }

        [Test]
        [Explicit]
        public void Does_retry_messages_with_errors_by_RetryCount() {
            var retryCount = 1;
            // in total, inc. first try
            var totalRetries = 1 + retryCount;

            using (var mqServer = RabbitMqServerTests.CreateMqServer(retryCount)) {
                void Action0(IModel channel) {
                    channel.PurgeQueue<Reverse>();
                    channel.PurgeQueue<NothingHere>();
                    channel.PurgeQueue<AlwaysThrows>();
                }

                RabbitMqConfig.UsingChannel(mqServer.ConnectionFactory, Action0);

                var reverseCalled = 0;
                var nothingHereCalled = 0;

                mqServer.RegisterHandler<Reverse>(x => {
                    Interlocked.Increment(ref reverseCalled);
                    return new ReverseResponse { Value = string.Join(",", x.GetBody().Value.Reverse()) };
                });
                mqServer.RegisterHandler<NothingHere>(x => {
                    Interlocked.Increment(ref nothingHereCalled);
                    return new NothingHereResponse { Value = x.GetBody().Value };
                });
                mqServer.RegisterHandler<AlwaysThrows>(x => throw new Exception("Always Throwing! " + x.GetBody().Value));

                mqServer.Start();

                using (var mqClient = mqServer.CreateMessageQueueClient()) {
                    mqClient.Publish(new AlwaysThrows { Value = "1st" });
                    mqClient.Publish(new Reverse { Value = "Hello" });
                    mqClient.Publish(new Reverse { Value = "World" });
                    mqClient.Publish(new NothingHere { Value = "TheOne" });

                    Thread.Sleep(1000);

                    Assert.That(mqServer.GetStats().TotalMessagesFailed, Is.EqualTo(1 * totalRetries));
                    Assert.That(mqServer.GetStats().TotalMessagesProcessed, Is.EqualTo(2 + 1));

                    for (var i = 0; i < 5; i++) {
                        mqClient.Publish(new AlwaysThrows { Value = "#" + i });
                    }

                    mqClient.Publish(new Reverse { Value = "Hello" });
                    mqClient.Publish(new Reverse { Value = "World" });
                    mqClient.Publish(new NothingHere { Value = "TheOne" });
                }

                Console.WriteLine(mqServer.GetStatsDescription());

                Thread.Sleep(1000);
                Assert.That(mqServer.GetStats().TotalMessagesFailed, Is.EqualTo((1 + 5) * totalRetries));
                Assert.That(mqServer.GetStats().TotalMessagesProcessed, Is.EqualTo(6));

                Assert.That(reverseCalled, Is.EqualTo(2 + 2));
                Assert.That(nothingHereCalled, Is.EqualTo(1 + 1));
            }
        }

    }

}
