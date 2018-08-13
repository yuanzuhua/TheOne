using NUnit.Framework;
using TheOne.RabbitMq.Interfaces;
using TheOne.RabbitMq.Messaging;
using TheOne.RabbitMq.Models;
using TheOne.RabbitMq.Tests.Messaging.Models;

namespace TheOne.RabbitMq.Tests.Messaging.Interfaces {

    internal abstract class MqServerQueueNameTestBase {

        public const string QueuePrefix = "site1.";

        public abstract IMqMessageService CreateMqServer(int retryCount = 1);

        [SetUp]
        public void SetUp() {
            MqQueueNames.SetQueuePrefix(QueuePrefix);
        }

        [TearDown]
        public void TearDown() {
            MqQueueNames.SetQueuePrefix("");
        }

        [Test]
        public void Can_Send_and_Receive_messages_using_QueueNamePrefix() {

            using (IMqMessageService mqServer = this.CreateMqServer()) {
                mqServer.RegisterHandler<HelloIntro>(m =>
                    new HelloIntroResponse { Result = $"Hello, {m.GetBody().Name}!" });
                mqServer.Start();

                using (IMqMessageQueueClient mqClient = mqServer.CreateMessageQueueClient()) {
                    var request = new HelloIntro { Name = "World" };
                    var requestInq = MqMessageFactory.Create(request).ToInQueueName();
                    Assert.That(requestInq, Is.EqualTo("site1.theone:mq.HelloIntro.direct"));

                    mqClient.Publish(request);

                    var responseInq = MqQueueNames<HelloIntroResponse>.Direct;
                    Assert.That(responseInq, Is.EqualTo("site1.theone:mq.HelloIntroResponse.direct"));

                    IMqMessage<HelloIntroResponse> responseMsg = mqClient.Get<HelloIntroResponse>(responseInq);
                    mqClient.Ack(responseMsg);
                    Assert.That(responseMsg.GetBody().Result, Is.EqualTo("Hello, World!"));
                }
            }
        }

    }

}
