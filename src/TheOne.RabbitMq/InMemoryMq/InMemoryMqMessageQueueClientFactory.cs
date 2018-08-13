using System;
using System.Collections.Generic;
using TheOne.RabbitMq.Interfaces;
using TheOne.RabbitMq.Messaging;

namespace TheOne.RabbitMq.InMemoryMq {

    /// <inheritdoc />
    internal class InMemoryMqMessageQueueClientFactory : IMqMessageQueueClientFactory {

        private readonly Dictionary<string, Queue<byte[]>> _queueMessageBytesMap = new Dictionary<string, Queue<byte[]>>();
        private readonly object _syncLock = new object();

        /// <inheritdoc />
        public IMqMessageQueueClient CreateMessageQueueClient() {
            return new InMemoryMqMessageQueueClient(this);
        }

        /// <inheritdoc />
        public void Dispose() { }

        public event EventHandler<EventArgs> MessageReceived;

        private void InvokeMessageReceived(EventArgs e) {
            EventHandler<EventArgs> received = this.MessageReceived;
            received?.Invoke(this, e);
        }

        /// <summary>
        ///     Returns the next message from queueName or null if no message
        /// </summary>
        public byte[] GetMessageAsync(string queueName) {
            lock (this._syncLock) {
                if (!this._queueMessageBytesMap.TryGetValue(queueName, out Queue<byte[]> bytesQueue)) {
                    return null;
                }

                if (bytesQueue.Count == 0) {
                    return null;
                }

                byte[] messageBytes = bytesQueue.Dequeue();
                return messageBytes;
            }
        }

        public void PublishMessage<T>(string queueName, IMqMessage<T> message) {
            this.PublishMessage(queueName, message.ToJsonBytes());
        }

        public void PublishMessage(string queueName, byte[] messageBytes) {
            lock (this._syncLock) {
                if (!this._queueMessageBytesMap.TryGetValue(queueName, out Queue<byte[]> bytesQueue)) {
                    bytesQueue = new Queue<byte[]>();
                    this._queueMessageBytesMap[queueName] = bytesQueue;
                }

                bytesQueue.Enqueue(messageBytes);
            }

            this.InvokeMessageReceived(new EventArgs());
        }

    }

}
