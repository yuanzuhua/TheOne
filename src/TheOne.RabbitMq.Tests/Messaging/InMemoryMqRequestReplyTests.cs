using TheOne.RabbitMq.InMemoryMq;
using TheOne.RabbitMq.Interfaces;
using TheOne.RabbitMq.Tests.Messaging.Interfaces;

namespace TheOne.RabbitMq.Tests.Messaging {

    internal sealed class InMemoryMqRequestReplyTests : MqRequestReplyTestBase {

        protected override IMqMessageService CreateMqServer(int retryCount = 1) {
            return new InMemoryMqTransientMessageService { RetryCount = retryCount };
        }
    }
}
