using NUnit.Framework;
using RabbitMQ.Client;
using TheOne.RabbitMq.Interfaces;
using TheOne.RabbitMq.Models;
using TheOne.RabbitMq.Tests.Messaging.Interfaces;
using TheOne.RabbitMq.Tests.Messaging.Models;

namespace TheOne.RabbitMq.Tests.Messaging {

    internal sealed class RabbitMqRequestReplyTests : MqRequestReplyTestBase {

        protected override IMqMessageService CreateMqServer(int retryCount = 1) {
            return new RabbitMqServer(RabbitMqConfig.RabbitMqHostName) { RetryCount = retryCount };
        }

        [OneTimeTearDown]
        public void TestFixtureTearDown() {
            RabbitMqConfig.UsingChannel(channel => {
                channel.ExchangeDelete(MqQueueNames.Exchange);
                channel.ExchangeDelete(MqQueueNames.ExchangeDlq);
                channel.ExchangeDelete(MqQueueNames.ExchangeTopic);
                channel.DeleteQueue<Incr>();
                channel.DeleteQueue<IncrResponse>();
                channel.DeleteQueue<HelloIntro>();
                channel.DeleteQueue<HelloIntroResponse>();
            });
        }

    }

}
