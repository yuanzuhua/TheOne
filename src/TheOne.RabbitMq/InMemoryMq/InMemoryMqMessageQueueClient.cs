using System;
using TheOne.RabbitMq.Interfaces;
using TheOne.RabbitMq.Messaging;
using TheOne.RabbitMq.Models;

namespace TheOne.RabbitMq.InMemoryMq {

    /// <inheritdoc />
    internal class InMemoryMqMessageQueueClient : IMqMessageQueueClient {

        private readonly InMemoryMqMessageQueueClientFactory _factory;

        public InMemoryMqMessageQueueClient(InMemoryMqMessageQueueClientFactory factory) {
            this._factory = factory;
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
            this._factory.PublishMessage(MqQueueNames<T>.Direct, message);
        }

        /// <inheritdoc />
        public void Publish(string queueName, IMqMessage message) {
            var messageBytes = message.ToJsonBytes();
            this._factory.PublishMessage(queueName, messageBytes);
        }

        /// <inheritdoc />
        public void Notify(string queueName, IMqMessage message) {
            var messageBytes = message.ToJsonBytes();
            this._factory.PublishMessage(queueName, messageBytes);
        }

        public IMqMessage<T> Get<T>(string queueName, TimeSpan? timeout = null) {
            var startedAt = DateTime.Now.Ticks; // No Stopwatch in Silverlight
            var timeoutMs = timeout == null ? -1 : (long)timeout.Value.TotalMilliseconds;
            while (timeoutMs == -1 || timeoutMs >= new TimeSpan(DateTime.Now.Ticks - startedAt).TotalMilliseconds) {
                var msg = this.GetAsync<T>(queueName);
                if (msg != null) {
                    return msg;
                }
            }

            throw new TimeoutException($"Exceeded elapsed time of {timeoutMs}ms");
        }

        /// <inheritdoc />
        public IMqMessage<T> GetAsync<T>(string queueName) {
            var bytes = this._factory.GetMessageAsync(queueName);
            return MqMessageExtensions.ToMessage<T>(bytes);
        }

        /// <inheritdoc />
        public void Ack(IMqMessage message) { }

        /// <inheritdoc />
        public void Nak(IMqMessage message, bool requeue, Exception exception = null) {
            var queueName = requeue
                ? message.ToInQueueName()
                : message.ToDlqQueueName();

            this.Publish(queueName, message);
        }

        /// <inheritdoc />
        public IMqMessage<T> CreateMessage<T>(object mqResponse) {
            return (IMqMessage<T>)mqResponse;
        }


        /// <inheritdoc />
        public string GetTempQueueName() {
            return MqQueueNames.GetTempQueueName();
        }

        /// <inheritdoc />
        public void Dispose() { }

    }

}
