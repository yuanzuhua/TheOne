using System;
using System.Threading;
using NUnit.Framework;
using TheOne.RabbitMq.Interfaces;
using TheOne.RabbitMq.Messaging;
using TheOne.RabbitMq.Models;
using TheOne.RabbitMq.Tests.Messaging.Models;

namespace TheOne.RabbitMq.Tests.Messaging.Interfaces {

    [TestFixture]
    public abstract class MqServerIntroTestBase {

        public abstract IMqMessageService CreateMqServer(int retryCount = 1);

        public const string ReplyToMq = "mq:Hello.replyto";

        [Test]
        public void Message_with_exceptions_are_retried_then_published_to_Request_dlq() {
            using (IMqMessageService mqServer = this.CreateMqServer()) {
                var called = 0;
                mqServer.RegisterHandler<HelloIntro>(m => {
                    Interlocked.Increment(ref called);
                    throw new ArgumentException("Name");
                });
                mqServer.Start();

                using (IMqMessageQueueClient mqClient = mqServer.CreateMessageQueueClient()) {
                    mqClient.Publish(new HelloIntro { Name = "World" });

                    IMqMessage<HelloIntro> dlqMsg = mqClient.Get<HelloIntro>(MqQueueNames<HelloIntro>.Dlq);
                    mqClient.Ack(dlqMsg);

                    Assert.That(called, Is.EqualTo(2));
                    Assert.That(dlqMsg.GetBody().Name, Is.EqualTo("World"));
                    Assert.That(dlqMsg.Error.ErrorCode, Is.EqualTo(typeof(ArgumentException).Name));
                    Assert.That(dlqMsg.Error.Message, Is.EqualTo("Name"));
                }
            }
        }

        [Test]
        public void Message_with_ReplyTo_are_published_to_the_ReplyTo_queue() {
            using (IMqMessageService mqServer = this.CreateMqServer()) {
                mqServer.RegisterHandler<HelloIntro>(m =>
                    new HelloIntroResponse { Result = string.Format("Hello, {0}!", m.GetBody().Name) });
                mqServer.Start();

                using (IMqMessageQueueClient mqClient = mqServer.CreateMessageQueueClient()) {
                    mqClient.Publish(new MqMessage<HelloIntro>(new HelloIntro { Name = "World" }) {
                        ReplyTo = ReplyToMq
                    });

                    IMqMessage<HelloIntroResponse> responseMsg = mqClient.Get<HelloIntroResponse>(ReplyToMq);
                    mqClient.Ack(responseMsg);
                    Assert.That(responseMsg.GetBody().Result, Is.EqualTo("Hello, World!"));
                }
            }
        }

        [Test]
        public void Message_with_ReplyTo_that_throw_exceptions_are_retried_then_published_to_Request_dlq() {
            using (IMqMessageService mqServer = this.CreateMqServer()) {
                var called = 0;
                mqServer.RegisterHandler<HelloIntro>(m => {
                    Interlocked.Increment(ref called);
                    throw new ArgumentException("Name");
                });
                mqServer.Start();

                using (IMqMessageQueueClient mqClient = mqServer.CreateMessageQueueClient()) {
                    mqClient.Publish(new MqMessage<HelloIntro>(new HelloIntro { Name = "World" }) {
                        ReplyTo = ReplyToMq
                    });

                    IMqMessage<HelloIntro> dlqMsg = mqClient.Get<HelloIntro>(MqQueueNames<HelloIntro>.Dlq);
                    mqClient.Ack(dlqMsg);

                    Assert.That(called, Is.EqualTo(2));
                    Assert.That(dlqMsg.GetBody().Name, Is.EqualTo("World"));
                    Assert.That(dlqMsg.Error.ErrorCode, Is.EqualTo(typeof(ArgumentException).Name));
                    Assert.That(dlqMsg.Error.Message, Is.EqualTo("Name"));
                }
            }
        }

        [Test]
        public void Message_with_response_are_published_to_Response_inq() {
            using (IMqMessageService mqServer = this.CreateMqServer()) {
                mqServer.RegisterHandler<HelloIntro>(m =>
                    new HelloIntroResponse { Result = string.Format("Hello, {0}!", m.GetBody().Name) });
                mqServer.Start();

                using (IMqMessageQueueClient mqClient = mqServer.CreateMessageQueueClient()) {
                    mqClient.Publish(new HelloIntro { Name = "World" });

                    IMqMessage<HelloIntroResponse> responseMsg = mqClient.Get<HelloIntroResponse>(MqQueueNames<HelloIntroResponse>.Direct);
                    mqClient.Ack(responseMsg);
                    Assert.That(responseMsg.GetBody().Result, Is.EqualTo("Hello, World!"));
                }
            }
        }

        [Test]
        public void Messages_with_no_responses_are_not_published() {
            using (IMqMessageService mqServer = this.CreateMqServer()) {
                mqServer.RegisterHandler<HelloIntro>(m => {
                    Console.WriteLine("Hello, {0}!", m.GetBody().Name);
                    return null;
                });
                mqServer.Start();

                using (IMqMessageQueueClient mqClient = mqServer.CreateMessageQueueClient()) {
                    mqClient.Publish(new HelloIntro { Name = "World" });
                }
            }
        }
    }
}
