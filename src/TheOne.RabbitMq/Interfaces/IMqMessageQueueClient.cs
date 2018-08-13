using System;

namespace TheOne.RabbitMq.Interfaces {

    public interface IMqMessageProducer : IDisposable {

        void Publish<T>(T messageBody);
        void Publish<T>(IMqMessage<T> message);

    }

    public interface IMqMessageQueueClient : IMqMessageProducer {

        /// <summary>
        ///     Publish the specified message into the durable queue @queueName
        /// </summary>
        void Publish(string queueName, IMqMessage message);

        /// <summary>
        ///     Publish the specified message into the transient queue @queueName
        /// </summary>
        void Notify(string queueName, IMqMessage message);

        /// <summary>
        ///     Synchronous blocking get.
        /// </summary>
        IMqMessage<T> Get<T>(string queueName, TimeSpan? timeout = null);

        /// <summary>
        ///     Non blocking get message
        /// </summary>
        IMqMessage<T> GetAsync<T>(string queueName);

        /// <summary>
        ///     Acknowledge the message has been successfully received or processed
        /// </summary>
        void Ack(IMqMessage message);

        /// <summary>
        ///     Negative acknowledgement the message was not processed correctly
        /// </summary>
        void Nak(IMqMessage message, bool requeue, Exception exception = null);

        /// <summary>
        ///     Create a typed message from a raw MQ Response artefact
        /// </summary>
        IMqMessage<T> CreateMessage<T>(object mqResponse);

        /// <summary>
        ///     Create a temporary Queue for Request / Reply
        /// </summary>
        string GetTempQueueName();

    }

}
