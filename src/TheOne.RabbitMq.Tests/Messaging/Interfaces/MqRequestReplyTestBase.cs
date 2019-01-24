using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using NUnit.Framework;
using TheOne.RabbitMq.Interfaces;
using TheOne.RabbitMq.Messaging;
using TheOne.RabbitMq.Models;
using TheOne.RabbitMq.Tests.Messaging.Models;

namespace TheOne.RabbitMq.Tests.Messaging.Interfaces {

    [TestFixture]
    public abstract class MqRequestReplyTestBase {

        protected abstract IMqMessageService CreateMqServer(int retryCount = 1);

        [Test]
        [SuppressMessage("ReSharper", "AccessToDisposedClosure", Justification = "intentionally")]
        public void Can_handle_multiple_rpc_clients() {
            const int noOfClients = 10;
            const int timeMs = 5000;

            var errors = new ConcurrentDictionary<long, string>();
            using (var mqServer = this.CreateMqServer()) {
                mqServer.RegisterHandler<Incr>(m =>
                    new IncrResponse { Result = m.GetBody().Value + 1 });
                mqServer.Start();

                var counter = 0;
                var activeClients = 0;
                var activeClientsLock = new object();

                void CallBack(object _) {
                    using (var mqClient = mqServer.CreateMessageQueueClient()) {
                        var sw = Stopwatch.StartNew();
                        var clientId = Interlocked.Increment(ref activeClients);
                        while (sw.ElapsedMilliseconds < timeMs) {
                            var next = Interlocked.Increment(ref counter);
                            try {
                                var replyToMq = mqClient.GetTempQueueName();
                                mqClient.Publish(new MqMessage<Incr>(new Incr { Value = next }) {
                                    ReplyTo = replyToMq
                                });

                                var responseMsg =
                                    mqClient.Get<IncrResponse>(replyToMq, TimeSpan.FromMilliseconds(timeMs));
                                mqClient.Ack(responseMsg);

                                var actual = responseMsg.GetBody().Result;
                                var expected = next + 1;
                                if (actual != expected) {
                                    errors[next] = string.Format("Actual: {1}, Expected: {0}", actual, expected);
                                }

                            } catch (Exception ex) {
                                errors[next] = ex.Message + "\nStackTrace:\n" + ex.StackTrace;
                            }
                        }

                        Console.WriteLine("Client {0} finished", clientId);
                        if (Interlocked.Decrement(ref activeClients) == 0) {
                            Console.WriteLine("All Clients Finished");
                            lock (activeClientsLock) {
                                Monitor.Pulse(activeClientsLock);
                            }
                        }
                    }
                }

                for (var i = 0; i < noOfClients; i++) {
                    ThreadPool.QueueUserWorkItem(CallBack);
                }

                lock (activeClientsLock) {
                    Monitor.Wait(activeClientsLock);
                }

                Console.WriteLine("Stopping Server...");
                Console.WriteLine("Requests: {0}", counter);
            }

            Assert.That(errors.Count, Is.EqualTo(0));
        }

        [Test]
        public void Can_publish_messages_to_the_ReplyTo_temporary_queue() {
            using (var mqServer = this.CreateMqServer()) {
                mqServer.RegisterHandler<HelloIntro>(m =>
                    new HelloIntroResponse { Result = string.Format("Hello, {0}!", m.GetBody().Name) });
                mqServer.Start();

                using (var mqClient = mqServer.CreateMessageQueueClient()) {
                    var replyToMq = mqClient.GetTempQueueName();
                    mqClient.Publish(new MqMessage<HelloIntro>(new HelloIntro { Name = "World" }) {
                        ReplyTo = replyToMq
                    });

                    var responseMsg = mqClient.Get<HelloIntroResponse>(replyToMq);
                    mqClient.Ack(responseMsg);
                    Assert.That(responseMsg.GetBody().Result, Is.EqualTo("Hello, World!"));
                }
            }
        }

        [Test]
        public void Can_send_message_with_custom_Header() {
            using (var mqServer = this.CreateMqServer()) {
                mqServer.RegisterHandler<HelloIntro>(m =>
                    new MqMessage<HelloIntroResponse>(new HelloIntroResponse { Result = string.Format("Hello, {0}!", m.GetBody().Name) }) {
                        Meta = m.Meta
                    });
                mqServer.Start();

                using (var mqClient = mqServer.CreateMessageQueueClient()) {
                    var replyToMq = mqClient.GetTempQueueName();
                    mqClient.Publish(new MqMessage<HelloIntro>(new HelloIntro { Name = "World" }) {
                        ReplyTo = replyToMq,
                        Meta = new Dictionary<string, string> { { "Custom", "Header" } }
                    });

                    var responseMsg = mqClient.Get<HelloIntroResponse>(replyToMq);
                    mqClient.Ack(responseMsg);
                    Assert.That(responseMsg.GetBody().Result, Is.EqualTo("Hello, World!"));
                    Assert.That(responseMsg.Meta["Custom"], Is.EqualTo("Header"));
                }
            }
        }

        [Test]
        public void Can_send_message_with_custom_Tag() {
            using (var mqServer = this.CreateMqServer()) {
                if (mqServer is RabbitMqServer) {
                    return; // Uses DeliveryTag for Tag
                }

                mqServer.RegisterHandler<HelloIntro>(m =>
                    new MqMessage<HelloIntroResponse>(new HelloIntroResponse {
                        Result = string.Format("Hello, {0}!", m.GetBody().Name)
                    }) { Tag = m.Tag });
                mqServer.Start();

                using (var mqClient = mqServer.CreateMessageQueueClient()) {
                    var replyToMq = mqClient.GetTempQueueName();
                    mqClient.Publish(new MqMessage<HelloIntro>(new HelloIntro { Name = "World" }) {
                        ReplyTo = replyToMq,
                        Tag = "Custom"
                    });

                    var responseMsg = mqClient.Get<HelloIntroResponse>(replyToMq);
                    mqClient.Ack(responseMsg);
                    Assert.That(responseMsg.GetBody().Result, Is.EqualTo("Hello, World!"));
                    Assert.That(responseMsg.Tag, Is.EqualTo("Custom"));
                }
            }
        }

    }

}
