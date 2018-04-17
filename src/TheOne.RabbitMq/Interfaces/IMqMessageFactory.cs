using System;

namespace TheOne.RabbitMq.Interfaces {

    public interface IMqMessageFactory : IMqMessageQueueClientFactory {

        IMqMessageProducer CreateMessageProducer();
    }

    public interface IMqMessageQueueClientFactory : IDisposable {

        IMqMessageQueueClient CreateMessageQueueClient();
    }
}
