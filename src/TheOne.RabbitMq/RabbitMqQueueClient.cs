using System;
using System.Threading;
using RabbitMQ.Client;
using TheOne.RabbitMq.Interfaces;
using TheOne.RabbitMq.Messaging;
using TheOne.RabbitMq.Models;

namespace TheOne.RabbitMq {

    /// <inheritdoc cref="RabbitMqProducer" />
    public class RabbitMqQueueClient : RabbitMqProducer, IMqMessageQueueClient {

        public RabbitMqQueueClient(RabbitMqMessageFactory msgFactory) : base(msgFactory) { }

        /// <inheritdoc />
        public virtual void Notify(string queueName, IMqMessage message) {
            this.Publish(queueName, MqQueueNames.ExchangeTopic, message);
        }

        /// <inheritdoc />
        public virtual IMqMessage<T> Get<T>(string queueName, TimeSpan? timeout = null) {
            var now = DateTime.Now;

            while (timeout == null || DateTime.Now - now < timeout.Value) {
                var basicMsg = this.GetMessage(queueName, false);
                if (basicMsg != null) {
                    return basicMsg.ToMessage<T>();
                }

                Thread.Sleep(100);
            }

            return null;
        }

        /// <inheritdoc />
        public virtual IMqMessage<T> GetAsync<T>(string queueName) {
            var basicMsg = this.GetMessage(queueName, false);
            return basicMsg.ToMessage<T>();
        }

        /// <inheritdoc />
        public virtual void Ack(IMqMessage message) {
            var deliveryTag = ulong.Parse(message.Tag);
            this.Channel.BasicAck(deliveryTag, false);
        }

        /// <inheritdoc />
        public virtual void Nak(IMqMessage message, bool requeue, Exception exception = null) {
            try {
                if (requeue) {
                    var deliveryTag = ulong.Parse(message.Tag);
                    this.Channel.BasicNack(deliveryTag, false, true);
                } else {
                    this.Publish(message.ToDlqQueueName(), MqQueueNames.ExchangeDlq, message);
                    this.Ack(message);
                }
            } catch (Exception) {
                var deliveryTag = ulong.Parse(message.Tag);
                this.Channel.BasicNack(deliveryTag, false, requeue);
            }
        }

        /// <inheritdoc />
        public virtual IMqMessage<T> CreateMessage<T>(object mqResponse) {
            if (mqResponse is BasicGetResult msgResult) {
                return msgResult.ToMessage<T>();
            }

            return (IMqMessage<T>)mqResponse;
        }

        /// <inheritdoc />
        public virtual string GetTempQueueName() {
            var anonMq = this.Channel.QueueDeclare(MqQueueNames.GetTempQueueName(), false, true, true, null);
            return anonMq.QueueName;
        }

        public virtual void Notify(IMqMessage message) {
            this.Notify(new MqQueueNames(message.Body.GetType()).Topic, message);
        }

        public virtual void Notify<T>(T body) {
            this.Notify((IMqMessage)new MqMessage { Body = body });
        }

    }

}
