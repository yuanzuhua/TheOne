using TheOne.RabbitMq.Interfaces;

namespace TheOne.RabbitMq.InMemoryMq {

    /// <inheritdoc />
    internal partial class InMemoryMqTransientMessageFactory : IMqMessageFactory {

        private readonly InMemoryMqTransientMessageService _transientMessageService;

        public InMemoryMqTransientMessageFactory() : this(null) { }

        public InMemoryMqTransientMessageFactory(InMemoryMqTransientMessageService transientMessageService) {
            this._transientMessageService = transientMessageService ?? new InMemoryMqTransientMessageService();
            this.MqFactory = new InMemoryMqMessageQueueClientFactory();
        }

        internal InMemoryMqMessageQueueClientFactory MqFactory { get; set; }

        /// <inheritdoc />
        public IMqMessageProducer CreateMessageProducer() {
            return new InMemoryMqMessageProducer(this);
        }

        /// <inheritdoc />
        public IMqMessageQueueClient CreateMessageQueueClient() {
            return new InMemoryMqMessageQueueClient(this.MqFactory);
        }

        /// <inheritdoc />
        public void Dispose() { }

        public IMqMessageService CreateMessageService() {
            return this._transientMessageService;
        }

    }

}
