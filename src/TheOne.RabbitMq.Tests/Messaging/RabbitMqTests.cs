using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using NUnit.Framework;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using TheOne.RabbitMq.Extensions;
using TheOne.RabbitMq.Messaging;
using TheOne.RabbitMq.Models;
using TheOne.RabbitMq.Tests.Messaging.Models;

namespace TheOne.RabbitMq.Tests.Messaging {

    [TestFixture]
    internal sealed class RabbitMqTests {

        private const string _exchange = "mq:tests";
        private const string _exchangeDlq = "mq:tests.dlq";
        private const string _exchangeTopic = "mq:tests.topic";
        private const string _exchangeFanout = "mq:tests.fanout";

        [OneTimeSetUp]
        public void TestFixtureSetUp() {
            RabbitMqConfig.UsingChannel(channel => {
                channel.RegisterDirectExchange(_exchange);
                channel.RegisterDlqExchange(_exchangeDlq);
                channel.RegisterTopicExchange(_exchangeTopic);

                RegisterQueue(channel, MqQueueNames<HelloRabbit>.Direct);
                RegisterDlq(channel, MqQueueNames<HelloRabbit>.Dlq);
                RegisterTopic(channel, MqQueueNames<HelloRabbit>.Topic);
                RegisterQueue(channel, MqQueueNames<HelloRabbit>.Direct, _exchangeTopic);

                channel.PurgeQueue<HelloRabbit>();
            });
        }

        [OneTimeTearDown]
        public void TestFixtureTearDown() {
            var exchangeNames = new[] {
                _exchange,
                _exchangeDlq,
                _exchangeTopic,
                _exchangeFanout,
                MqQueueNames.Exchange,
                MqQueueNames.ExchangeDlq,
                MqQueueNames.ExchangeTopic
            };

            RabbitMqConfig.UsingChannel(channel => {
                foreach (var value in exchangeNames) {
                    channel.ExchangeDelete(value);
                }

                channel.DeleteQueue<AlwaysThrows>();
                channel.DeleteQueue<HelloRabbit>();
                channel.DeleteQueue<Incr>();
                channel.DeleteQueue<Reverse>();
                channel.DeleteQueue<NothingHere>();
            });
        }

        private static void RegisterQueue(IModel channel, string queueName, string exchange = _exchange) {
            var args = new Dictionary<string, object> {
                { RabbitMqExtensions.XDeadLetterExchange, _exchangeDlq },
                { RabbitMqExtensions.XDeadLetterRoutingKey, queueName.Replace(MqQueueNames.PostfixDirect, MqQueueNames.PostfixDlq) },
                { RabbitMqExtensions.XMaxPriority, 10 }
            };
            channel.QueueDeclare(queueName, true, false, false, args);
            channel.QueueBind(queueName, exchange, queueName);
        }

        private static void RegisterTopic(IModel channel, string queueName) {
            channel.QueueDeclare(queueName, false, false, false, null);
            channel.QueueBind(queueName, _exchangeTopic, queueName);
        }

        private static void RegisterDlq(IModel channel, string queueName) {
            channel.QueueDeclare(queueName, true, false, false, null);
            channel.QueueBind(queueName, _exchangeDlq, queueName);
        }

        private static void PublishHelloRabbit(IModel channel, string text = "World!") {
            byte[] payload = MqMessageExtensions.ToJsonBytes(new HelloRabbit { Name = text });
            IBasicProperties props = channel.CreateBasicProperties();
            props.Persistent = true;
            channel.BasicPublish(_exchange, MqQueueNames<HelloRabbit>.Direct, props, payload);
        }

        [Test]
        public void Can_consume_messages_from_RabbitMQ_with_BasicConsume() {
            RabbitMqConfig.UsingChannel(channel => {
                var consumer = new RabbitMqBasicConsumer(channel);
                channel.BasicConsume(MqQueueNames<HelloRabbit>.Direct, false, consumer);
                string recvMsg = null;

                ThreadPool.QueueUserWorkItem(_ => {
                    Thread.Sleep(100);
                    PublishHelloRabbit(channel);
                });

                while (true) {
                    try {
                        BasicGetResult e = consumer.Queue.Dequeue();
                        Console.WriteLine("Dequeued");
                        recvMsg = e.Body.FromUtf8Bytes();
                        // ... process the message
                        Console.WriteLine(recvMsg);

                        channel.BasicAck(e.DeliveryTag, false);
                        break;
                    } catch (OperationInterruptedException) {
                        // The consumer was removed, either through
                        // channel or connection closure, or through the
                        // action of IModel.BasicCancel().
                        Console.WriteLine("End of the road...");
                        break;
                    }
                }

                Assert.That(recvMsg, Is.Not.Null);
            });
        }

        [Test]
        public void Can_consume_messages_from_RabbitMQ_with_BasicGet() {
            RabbitMqConfig.UsingChannel(channel => {
                PublishHelloRabbit(channel);

                while (true) {
                    BasicGetResult basicGetMsg = channel.BasicGet(MqQueueNames<HelloRabbit>.Direct, false);

                    if (basicGetMsg == null) {
                        Console.WriteLine("End of the road...");
                        return;
                    }

                    var msg = MqMessageExtensions.FromJsonBytes<HelloRabbit>(basicGetMsg.Body);

                    Console.WriteLine(msg);

                    Thread.Sleep(1000);

                    channel.BasicAck(basicGetMsg.DeliveryTag, false);
                }
            });
        }

        [Test]
        public void Can_consume_messages_with_BasicConsumer() {
            RabbitMqConfig.UsingChannel(channel => {
                OperationInterruptedException lastEx = null;

                channel.Close();

                ThreadPool.QueueUserWorkItem(_ => {
                    try {
                        PublishHelloRabbit(channel);
                    } catch (Exception ex) {
                        lastEx = ex as OperationInterruptedException;
                        Console.WriteLine("Caught {0}: {1}", ex.GetType().Name, ex);
                    }
                });

                Thread.Sleep(1000);

                Assert.That(lastEx, Is.Not.Null);
            });
        }

        [Test]
        public void Can_interrupt_BasicConsumer_in_bgthread_by_closing_channel() {
            RabbitMqConfig.UsingChannel(channel => {
                string recvMsg = null;
                EndOfStreamException lastEx = null;

                var bgThread = new Thread(() => {
                    try {
                        var consumer = new RabbitMqBasicConsumer(channel);
                        channel.BasicConsume(MqQueueNames<HelloRabbit>.Direct, false, consumer);

                        while (true) {
                            try {
                                BasicGetResult e = consumer.Queue.Dequeue();
                                recvMsg = e.Body.FromUtf8Bytes();
                            } catch (EndOfStreamException ex) {
                                // The consumer was cancelled, the model closed, or the
                                // connection went away.
                                Console.WriteLine("EndOfStreamException in bgthread: {0}", ex.Message);
                                lastEx = ex;
                                return;
                            } catch (Exception ex) {
                                Assert.Fail("Unexpected exception in bgthread: " + ex.Message);
                            }
                        }
                    } catch (Exception ex) {
                        Console.WriteLine("Exception in bgthread: {0}: {1}", ex.GetType().Name, ex.Message);
                    }
                }) {
                    Name = "Closing Channel Test",
                    IsBackground = true
                };
                bgThread.Start();

                PublishHelloRabbit(channel);
                Thread.Sleep(100);

                // closing either throws EndOfStreamException in bgthread
                channel.Close();
                // connection.Close();

                Thread.Sleep(2000);

                Assert.That(recvMsg, Is.Not.Null);
                Assert.That(lastEx, Is.Not.Null);
            });
        }

        [Test]
        public void Can_publish_messages_to_RabbitMQ() {
            RabbitMqConfig.UsingChannel(channel => {
                for (var i1 = 0; i1 < 5; i1++) {
                    byte[] payload = MqMessageExtensions.ToJsonBytes(new HelloRabbit { Name = $"World! #{i1}" });
                    IBasicProperties props = channel.CreateBasicProperties();
                    props.Persistent = true;

                    channel.BasicPublish(_exchange, MqQueueNames<HelloRabbit>.Direct, props, payload);

                    Console.WriteLine("Sent Message " + i1);
                    Thread.Sleep(1000);
                }
            });
        }

        [Test]
        public void Does_publish_to_dead_letter_exchange() {
            RabbitMqConfig.UsingChannel(channel => {

                PublishHelloRabbit(channel);

                BasicGetResult basicGetMsg = channel.BasicGet(MqQueueNames<HelloRabbit>.Direct, true);
                BasicGetResult dlqBasicMsg = channel.BasicGet(MqQueueNames<HelloRabbit>.Dlq, true);
                Assert.That(basicGetMsg, Is.Not.Null);
                Assert.That(dlqBasicMsg, Is.Null);

                PublishHelloRabbit(channel);

                basicGetMsg = channel.BasicGet(MqQueueNames<HelloRabbit>.Direct, false);
                Thread.Sleep(500);
                dlqBasicMsg = channel.BasicGet(MqQueueNames<HelloRabbit>.Dlq, false);
                Assert.That(basicGetMsg, Is.Not.Null);
                Assert.That(dlqBasicMsg, Is.Null);

                channel.BasicNack(basicGetMsg.DeliveryTag, false, false);

                Thread.Sleep(500);
                dlqBasicMsg = channel.BasicGet(MqQueueNames<HelloRabbit>.Dlq, true);
                Assert.That(dlqBasicMsg, Is.Not.Null);
            });
        }

        [Test]
        public void Publishing_message_to_fanout_exchange_publishes_to_all_queues() {
            RabbitMqConfig.UsingChannel(channel => {
                channel.RegisterFanoutExchange(_exchangeFanout);

                RegisterQueue(channel, MqQueueNames<HelloRabbit>.Direct, _exchangeFanout);

                byte[] body = MqMessageExtensions.ToJsonBytes(new HelloRabbit { Name = "World!" });
                IBasicProperties props = channel.CreateBasicProperties();
                props.Persistent = true;

                channel.BasicPublish(_exchangeFanout, MqQueueNames<HelloRabbit>.Direct, props, body);

                BasicGetResult basicGetMsg = channel.BasicGet(MqQueueNames<HelloRabbit>.Direct, true);
                Assert.That(basicGetMsg, Is.Not.Null);
            });
        }

        [Test]
        public void Publishing_message_with_routingKey_sends_only_to_registered_queue() {
            RabbitMqConfig.UsingChannel(channel => {
                PublishHelloRabbit(channel);

                BasicGetResult basicGetMsg = channel.BasicGet(MqQueueNames<HelloRabbit>.Direct, true);
                Assert.That(basicGetMsg, Is.Not.Null);
            });
        }
    }
}
