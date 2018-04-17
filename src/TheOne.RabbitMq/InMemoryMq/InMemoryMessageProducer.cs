using TheOne.RabbitMq.Interfaces;
using TheOne.RabbitMq.Messaging;
using TheOne.RabbitMq.Models;

namespace TheOne.RabbitMq.InMemoryMq {

    internal partial class InMemoryMqTransientMessageFactory {

        /// <inheritdoc />
        internal class InMemoryMqMessageProducer : IMqMessageProducer {

            private readonly InMemoryMqTransientMessageFactory _parent;

            public InMemoryMqMessageProducer(InMemoryMqTransientMessageFactory parent) {
                this._parent = parent;
            }

            /// <inheritdoc />
            public void Publish<T>(T messageBody) {
                if (messageBody is IMqMessage message) {
                    this.Publish(message.ToInQueueName(), message);
                } else {
                    this.Publish(new MqMessage<T>(messageBody));
                }
            }

            /// <inheritdoc />
            public void Publish<T>(IMqMessage<T> message) {
                this.Publish(MqQueueNames<T>.Direct, message);
            }

            /// <inheritdoc />
            public void Dispose() { }

            public void Publish(string queueName, IMqMessage message) {
                this._parent._transientMessageService.MessageQueueFactory
                    .PublishMessage(queueName, message.ToJsonBytes());
            }
        }
    }
}
