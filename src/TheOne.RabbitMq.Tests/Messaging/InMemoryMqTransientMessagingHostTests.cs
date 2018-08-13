using TheOne.RabbitMq.InMemoryMq;
using TheOne.RabbitMq.Interfaces;
using TheOne.RabbitMq.Tests.Messaging.Interfaces;

namespace TheOne.RabbitMq.Tests.Messaging {

    internal sealed class InMemoryMqTransientMessagingHostTests : MqTransientServiceMessagingTestBase {

        private InMemoryMqTransientMessageService _messageService;

        protected override IMqMessageFactory CreateMessageFactory() {
            this._messageService = new InMemoryMqTransientMessageService();
            return new InMemoryMqTransientMessageFactory(this._messageService);
        }

        protected override MqTransientMessageServiceBase CreateMessagingService() {
            return this._messageService;
        }

    }

}
