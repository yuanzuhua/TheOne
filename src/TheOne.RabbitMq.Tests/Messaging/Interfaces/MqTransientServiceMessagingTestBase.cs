using NUnit.Framework;
using TheOne.RabbitMq.InMemoryMq;
using TheOne.RabbitMq.Interfaces;
using TheOne.RabbitMq.Models;
using TheOne.RabbitMq.Tests.Messaging.Models;

namespace TheOne.RabbitMq.Tests.Messaging.Interfaces {

    internal abstract class MqTransientServiceMessagingTestBase : MqMessagingHostTestBase {

        [Test]
        public void AlwaysFailsService_ends_up_in_dlq_after_3_attempts() {
            var service = new AlwaysFailService();
            var request = new AlwaysFail { Name = "World!" };
            using (MqTransientMessageServiceBase serviceHost = this.CreateMessagingService()) {
                using (IMqMessageQueueClient client = serviceHost.MessageFactory.CreateMessageQueueClient()) {
                    client.Publish(request);
                }

                serviceHost.RegisterHandler<AlwaysFail>(m => service.Any(m.GetBody()));
                serviceHost.Start();

                Assert.That(service.Result, Is.Null);
                Assert.That(service.TimesCalled, Is.EqualTo(3));

                using (IMqMessageQueueClient client = serviceHost.MessageFactory.CreateMessageQueueClient()) {
                    IMqMessage<AlwaysFail> dlqMessage = client.GetAsync<AlwaysFail>(MqQueueNames<AlwaysFail>.Dlq);
                    client.Ack(dlqMessage);

                    Assert.That(dlqMessage, Is.Not.Null);
                    Assert.That(dlqMessage.GetBody().Name, Is.EqualTo(request.Name));
                }
            }
        }

        [Test]
        public void Normal_GreetService_client_and_server_example() {
            var service = new GreetService();
            using (MqTransientMessageServiceBase serviceHost = this.CreateMessagingService()) {
                serviceHost.RegisterHandler<Greet>(m => service.Any(m.GetBody()));

                serviceHost.Start();

                using (IMqMessageQueueClient client = serviceHost.MessageFactory.CreateMessageQueueClient()) {
                    client.Publish(new Greet { Name = "World!" });
                }

                Assert.That(service.Result, Is.EqualTo("Hello, World!"));
                Assert.That(service.TimesCalled, Is.EqualTo(1));
            }
        }

        [Test]
        public void Publish_before_starting_host_GreetService_client_and_server_example() {
            var service = new GreetService();
            using (MqTransientMessageServiceBase serviceHost = this.CreateMessagingService()) {
                using (IMqMessageQueueClient client = serviceHost.MessageFactory.CreateMessageQueueClient()) {
                    client.Publish(new Greet { Name = "World!" });
                }

                serviceHost.RegisterHandler<Greet>(m => service.Any(m.GetBody()));
                serviceHost.Start();

                Assert.That(service.Result, Is.EqualTo("Hello, World!"));
                Assert.That(service.TimesCalled, Is.EqualTo(1));
            }
        }

        [Test]
        public void UnRetryableFailService_ends_up_in_dlq_after_1_attempt() {
            var service = new UnRetryableFailService();

            var request = new UnRetryableFail { Name = "World!" };
            using (MqTransientMessageServiceBase serviceHost = this.CreateMessagingService()) {
                using (IMqMessageQueueClient client = serviceHost.MessageFactory.CreateMessageQueueClient()) {
                    client.Publish(request);
                }

                serviceHost.RegisterHandler<UnRetryableFail>(m => service.Any(m.GetBody()));
                serviceHost.Start();

                Assert.That(service.Result, Is.Null);
                Assert.That(service.TimesCalled, Is.EqualTo(1));

                using (IMqMessageQueueClient client = serviceHost.MessageFactory.CreateMessageQueueClient()) {
                    IMqMessage<UnRetryableFail> dlqMessage = client.GetAsync<UnRetryableFail>(MqQueueNames<UnRetryableFail>.Dlq);
                    client.Ack(dlqMessage);

                    Assert.That(dlqMessage, Is.Not.Null);
                    Assert.That(dlqMessage.GetBody().Name, Is.EqualTo(request.Name));
                }
            }
        }

    }

}
