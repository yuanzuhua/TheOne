using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using NUnit.Framework;
using RabbitMQ.Client;
using TheOne.RabbitMq.Extensions;
using TheOne.RabbitMq.Interfaces;
using TheOne.RabbitMq.Messaging;
using TheOne.RabbitMq.Models;
using TheOne.RabbitMq.Tests.Messaging.Models;

namespace TheOne.RabbitMq.Tests.Messaging {

    [TestFixture]
    internal sealed class RabbitMqServerTests {

        internal static RabbitMqServer CreateMqServer(int retryCount = 1) {
            return new RabbitMqServer(RabbitMqConfig.RabbitMqHostName) { RetryCount = retryCount };
        }

        internal static void Publish_4_messages(IMqMessageQueueClient mqClient) {
            mqClient.Publish(new Reverse { Value = "Hello" });
            mqClient.Publish(new Reverse { Value = "World" });
            mqClient.Publish(new Reverse { Value = "TheOne" });
            mqClient.Publish(new Reverse { Value = "Redis" });
        }

        private static void Publish_4_NothingHere_messages(IMqMessageQueueClient mqClient) {
            mqClient.Publish(new NothingHere { Value = "Hello" });
            mqClient.Publish(new NothingHere { Value = "World" });
            mqClient.Publish(new NothingHere { Value = "TheOne" });
            mqClient.Publish(new NothingHere { Value = "Redis" });
        }

        private static void RunHandlerOnMultipleThreads(int noOfThreads, int msgs) {
            using (RabbitMqServer mqServer = CreateMqServer()) {
                var timesCalled = 0;
                RabbitMqConfig.UsingChannel(mqServer.ConnectionFactory, channel => channel.PurgeQueue<Wait>());

                object WaitProcessMessageFn(IMqMessage<Wait> m) {
                    Interlocked.Increment(ref timesCalled);
                    Thread.Sleep(m.GetBody().ForMs);
                    return null;
                }

                mqServer.RegisterHandler<Wait>(WaitProcessMessageFn, noOfThreads);
                mqServer.Start();

                using (IMqMessageQueueClient mqClient = mqServer.CreateMessageQueueClient()) {
                    var dto = new Wait { ForMs = 100 };
                    for (var i1 = 0; i1 < msgs; i1++) {
                        mqClient.Publish(dto);
                    }

                    Thread.Sleep(1000);
                    Assert.That(timesCalled, Is.EqualTo(msgs));
                }
            }
        }

        [OneTimeTearDown]
        public void TestFixtureTearDown() {
            RabbitMqConfig.UsingChannel(channel => {
                channel.ExchangeDelete(MqQueueNames.Exchange);
                channel.ExchangeDelete(MqQueueNames.ExchangeDlq);
                channel.ExchangeDelete(MqQueueNames.ExchangeTopic);
                channel.DeleteQueue<Hello>();
                channel.DeleteQueue<HelloResponse>();
                channel.DeleteQueue<HelloNull>();
                channel.DeleteQueue<Incr>();
                channel.DeleteQueue<IncrResponse>();
                channel.DeleteQueue<Reverse>();
                channel.DeleteQueue<ReverseResponse>();
                channel.DeleteQueue<NothingHere>();
                channel.DeleteQueue<NothingHereResponse>();
                channel.DeleteQueue<Wait>();
            });
        }

        [Test]
        public void Can_filter_published_and_received_messages() {
            string receivedMsgApp = null;
            string receivedMsgType = null;

            RabbitMqExtensions.CreateQueueFilter = (s, args) => {
                if (s == MqQueueNames<Hello>.Direct) {
                    args.Remove(RabbitMqExtensions.XMaxPriority);
                }
            };
            using (RabbitMqServer mqServer = CreateMqServer()) {
                mqServer.PublishMessageFilter = (queueName, properties, msg) => {
                    properties.AppId = $"app:{queueName}";
                };
                mqServer.GetMessageFilter = (queueName, basicMsg) => {
                    IBasicProperties props = basicMsg.BasicProperties;
                    // automatically added by RabbitMqProducer
                    receivedMsgType = props.Type;
                    receivedMsgApp = props.AppId;
                };
                mqServer.RequestFilter = message => {
                    Console.WriteLine(message);
                    return message;
                };
                mqServer.ResponseFilter = o => {
                    Console.WriteLine(o);
                    return o;
                };

                mqServer.RegisterHandler<Hello>(m => {
                    return new HelloResponse { Result = string.Format("Hello, {0}!", m.GetBody().Name) };
                });

                mqServer.Start();

                using (IMqMessageQueueClient mqClient = mqServer.CreateMessageQueueClient()) {
                    mqClient.Publish(new Hello { Name = "Bugs Bunny" });
                }

                Thread.Sleep(100);

                Assert.That(receivedMsgApp, Is.EqualTo(string.Format("app:{0}", MqQueueNames<Hello>.Direct)));
                Assert.That(receivedMsgType, Is.EqualTo(typeof(Hello).Name));

                RabbitMqConfig.UsingChannel(channel => {
                    var queueName = MqQueueNames<HelloResponse>.Direct;
                    channel.RegisterQueue(queueName);

                    BasicGetResult basicMsg = channel.BasicGet(queueName, true);
                    IBasicProperties props = basicMsg.BasicProperties;

                    Assert.That(props.Type, Is.EqualTo(typeof(HelloResponse).Name));
                    Assert.That(props.AppId, Is.EqualTo($"app:{queueName}"));

                    IMqMessage<HelloResponse> msg = basicMsg.ToMessage<HelloResponse>();
                    Assert.That(msg.GetBody().Result, Is.EqualTo("Hello, Bugs Bunny!"));
                });
            }
        }

        [Test]
        public void Can_handle_requests_concurrently_in_4_threads() {
            RunHandlerOnMultipleThreads(4, 10);
        }

        [Test]
        public void Can_publish_and_receive_messages_with_MessageFactory() {
            using (var mqFactory = new RabbitMqMessageFactory(RabbitMqConfig.RabbitMqHostName)) {
                using (IMqMessageQueueClient mqClient = mqFactory.CreateMessageQueueClient()) {
                    mqClient.Publish(new Hello { Name = "Foo" });
                    IMqMessage<Hello> msg = mqClient.Get<Hello>(MqQueueNames<Hello>.Direct);

                    Assert.That(msg.GetBody().Name, Is.EqualTo("Foo"));
                }
            }
        }

        [Test]
        public void Can_receive_and_process_same_reply_responses() {
            var called = 0;
            using (RabbitMqServer mqServer = CreateMqServer()) {
                RabbitMqConfig.UsingChannel(mqServer.ConnectionFactory, channel => channel.PurgeQueue<Incr>());

                Incr ProcessMessageFn(IMqMessage<Incr> m) {
                    Console.WriteLine("In Incr #" + m.GetBody().Value);
                    Interlocked.Increment(ref called);
                    return m.GetBody().Value > 0 ? new Incr { Value = m.GetBody().Value - 1 } : null;
                }

                mqServer.RegisterHandler<Incr>(ProcessMessageFn);

                mqServer.Start();

                var incr = new Incr { Value = 5 };
                using (IMqMessageQueueClient mqClient = mqServer.CreateMessageQueueClient()) {
                    mqClient.Publish(incr);
                }

                Thread.Sleep(1000);
                Assert.That(called, Is.EqualTo(1 + incr.Value));
            }
        }

        [Test]
        public void Can_receive_and_process_standard_request_reply_combo() {
            using (RabbitMqServer mqServer = CreateMqServer()) {
                void Action0(IModel channel) {
                    channel.PurgeQueue<Hello>();
                    channel.PurgeQueue<HelloResponse>();
                }

                RabbitMqConfig.UsingChannel(mqServer.ConnectionFactory, Action0);

                string messageReceived = null;

                mqServer.RegisterHandler<Hello>(m =>
                    new HelloResponse { Result = "Hello, " + m.GetBody().Name });

                mqServer.RegisterHandler<HelloResponse>(m => {
                    messageReceived = m.GetBody().Result;
                    return null;
                });

                mqServer.Start();

                using (IMqMessageQueueClient mqClient = mqServer.CreateMessageQueueClient()) {
                    var dto = new Hello { Name = "TheOne" };
                    mqClient.Publish(dto);

                    Thread.Sleep(1000);
                    Assert.That(messageReceived, Is.EqualTo("Hello, TheOne"));
                }
            }
        }

        [Test]
        public void Cannot_Start_a_Disposed_MqServer() {
            RabbitMqServer mqServer = CreateMqServer();

            mqServer.Dispose();

            try {
                mqServer.Start();
                Assert.Fail("Should throw ObjectDisposedException");
                // ReSharper disable once UncatchableException
            } catch (ObjectDisposedException) {
                //
            }
        }

        [Test]
        public void Cannot_Stop_a_Disposed_MqServer() {
            RabbitMqServer mqServer = CreateMqServer();

            mqServer.Start();

            Thread.Sleep(100);

            mqServer.Dispose();

            try {
                mqServer.Stop();
                Assert.Fail("Should throw ObjectDisposedException");
                // ReSharper disable once UncatchableException
            } catch (ObjectDisposedException) {
                //
            }
        }

        [Test]
        public void Handler_with_null_Response_Messages_is_ignored() {
            var msgsReceived = 0;
            using (RabbitMqServer mqServer = CreateMqServer()) {
                mqServer.RegisterHandler<HelloNull>(m => {
                    Interlocked.Increment(ref msgsReceived);
                    return null;
                });

                mqServer.Start();

                using (IMqMessageQueueClient mqClient = mqServer.CreateMessageQueueClient()) {
                    mqClient.Publish(new HelloNull { Name = "Into the Void" });

                    Thread.Sleep(100);

                    Assert.That(msgsReceived, Is.EqualTo(1));
                }
            }
        }

        [Test]
        public void Messages_has_ReplayTo_with_null_Response_is_ignored() {
            var msgsReceived = 0;
            using (RabbitMqServer mqServer = CreateMqServer()) {
                mqServer.RegisterHandler<HelloNull>(m => {
                    Interlocked.Increment(ref msgsReceived);
                    return null;
                });

                mqServer.Start();

                using (IMqMessageQueueClient mqClient = mqServer.CreateMessageQueueClient()) {
                    var replyMq = mqClient.GetTempQueueName();
                    mqClient.Publish(new MqMessage<HelloNull>(new HelloNull { Name = "Into the Void" }) {
                        ReplyTo = replyMq
                    });

                    IMqMessage<HelloNull> msg = mqClient.Get<HelloNull>(replyMq, TimeSpan.FromSeconds(5));

                    Assert.Null(msg);

                    Thread.Sleep(100);

                    Assert.That(msgsReceived, Is.EqualTo(1));
                }
            }
        }

        [Test]
        public void Messages_notify() {
            using (RabbitMqServer mqServer = CreateMqServer()) {
                mqServer.Start();

                using (IMqMessageQueueClient mqClient = mqServer.CreateMessageQueueClient()) {
                    var name = new HelloNull { Name = "Into the Void" };
                    mqClient.Notify(MqQueueNames<HelloNull>.Topic, new MqMessage<HelloNull> { Body = name });
                    IMqMessage<HelloNull> msg = mqClient.Get<HelloNull>(MqQueueNames<HelloNull>.Topic, TimeSpan.FromSeconds(5));
                    Assert.AreEqual(name.Name, msg.GetBody().Name);
                    Thread.Sleep(100);
                }
            }
        }

        [Test]
        public void Misc_publish_NothingHere_messages() {
            using (var mqServer = new RabbitMqServer(RabbitMqConfig.RabbitMqHostName)) {
                using (IMqMessageQueueClient mqClient = mqServer.CreateMessageQueueClient()) {
                    Publish_4_NothingHere_messages(mqClient);
                }
            }
        }

        [Test]
        public void Misc_publish_Reverse_messages() {
            using (var mqServer = new RabbitMqServer(RabbitMqConfig.RabbitMqHostName)) {
                using (IMqMessageQueueClient mqClient = mqServer.CreateMessageQueueClient()) {
                    Publish_4_messages(mqClient);
                }
            }
        }

        [Test]
        [SuppressMessage("ReSharper", "AccessToDisposedClosure", Justification = "intentionally")]
        public void Only_allows_1_BgThread_to_run_at_a_time() {
            using (RabbitMqServer mqServer = CreateMqServer()) {

                mqServer.RegisterHandler<Reverse>(x => {
                    return new ReverseResponse { Value = string.Join(",", x.GetBody().Value.Reverse()) };
                });
                mqServer.RegisterHandler<NothingHere>(x => {
                    return new NothingHereResponse { Value = x.GetBody().Value };
                });

                ExecUtils.ExecMultiThreading(5, () => mqServer.Start());
                Assert.That(mqServer.GetStatus(), Is.EqualTo("Started"));
                Assert.That(mqServer.BgThreadCount, Is.EqualTo(1));

                ExecUtils.ExecMultiThreading(10, () => mqServer.Stop());
                Assert.That(mqServer.GetStatus(), Is.EqualTo("Stopped"));

                ExecUtils.ExecMultiThreading(1, () => mqServer.Start());
                Assert.That(mqServer.GetStatus(), Is.EqualTo("Started"));
                Assert.That(mqServer.BgThreadCount, Is.EqualTo(2));

                Console.WriteLine(mqServer.GetStats());
            }
        }

    }

}
