using System;
using System.Collections.Generic;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using TheOne.Logging;
using TheOne.RabbitMq.Interfaces;
using TheOne.RabbitMq.Messaging;
using TheOne.RabbitMq.Models;

namespace TheOne.RabbitMq {

    /// <inheritdoc />
    public class RabbitMqProducer : IMqMessageProducer {

        private static readonly ILog _logger = LogProvider.GetCurrentClassLogger();
        private static HashSet<string> _queues = new HashSet<string>();
        protected readonly RabbitMqMessageFactory MsgFactory;
        private IModel _channel;
        private IConnection _connection;

        public RabbitMqProducer(RabbitMqMessageFactory msgFactory) {
            this.MsgFactory = msgFactory;
        }

        public int RetryCount { get; set; }
        public Action<string, IBasicProperties, IMqMessage> PublishMessageFilter { get; set; }
        public Action<string, BasicGetResult> GetMessageFilter { get; set; }

        /// <summary>
        ///     http://www.rabbitmq.com/blog/2012/04/25/rabbitmq-performance-measurements-part-2/
        ///     http://www.rabbitmq.com/amqp-0-9-1-reference.html
        /// </summary>
        public ushort PrefetchCount { get; set; } = 20;

        public IConnection Connection => this._connection ?? (this._connection = this.MsgFactory.ConnectionFactory.CreateConnection());

        public IModel Channel {
            get {
                if (this._channel?.IsOpen == true) {
                    return this._channel;
                }

                this._channel = this.Connection.OpenChannel();
                // prefetch size is no supported by RabbitMQ
                // http://www.rabbitmq.com/specification.html#method-status-basic.qos
                this._channel.BasicQos(0, this.PrefetchCount, false);

                return this._channel;
            }
        }

        /// <inheritdoc />
        public virtual void Publish<T>(T messageBody) {
            if (messageBody is IMqMessage message) {
                this.Publish(message.ToInQueueName(), message);
            } else {
                this.Publish(new MqMessage<T>(messageBody));
            }
        }

        /// <inheritdoc />
        public virtual void Publish<T>(IMqMessage<T> message) {
            this.Publish(MqQueueNames<T>.Direct, message);
        }

        /// <inheritdoc />
        public virtual void Dispose() {
            if (this._channel != null) {
                try {
                    this._channel.Dispose();
                } catch (Exception ex) {
                    _logger.Error(ex, "Error trying to dispose RabbitMqProducer model.");
                }

                this._channel = null;
            }

            if (this._connection != null) {
                try {
                    this._connection.Dispose();
                } catch (Exception ex) {
                    _logger.Error(ex, "Error trying to dispose RabbitMqProducer connection.");
                }

                this._connection = null;
            }
        }

        public virtual void Publish(string queueName, IMqMessage message) {
            this.Publish(queueName, MqQueueNames.Exchange, message);
        }

        public virtual void Publish(string queueName, string exchange, IMqMessage message) {
            IBasicProperties props = this.Channel.CreateBasicProperties();
            props.Persistent = true;
            props.PopulateFromMessage(message);

            this.PublishMessageFilter?.Invoke(queueName, props, message);

            byte[] messageBytes = MqMessageExtensions.ToJsonBytes(message.Body);

            this.PublishMessage(exchange ?? MqQueueNames.Exchange, queueName, props, messageBytes);
        }

        public virtual void PublishMessage(string exchange, string routingKey, IBasicProperties basicProperties, byte[] body) {
            try {
                // In case of server named queues (client declared queue with channel.declare()),
                // assume queue already exists (redeclaration would result in error anyway since queue was marked as exclusive)
                // and publish to default exchange
                if (RabbitMqExtensions.IsServerNamedQueue(routingKey)) {
                    this.Channel.BasicPublish("", routingKey, basicProperties, body);
                } else {
                    if (!_queues.Contains(routingKey)) {
                        this.Channel.RegisterQueueByName(routingKey);
                        _queues = new HashSet<string>(_queues) { routingKey };
                    }

                    this.Channel.BasicPublish(exchange, routingKey, basicProperties, body);
                }
            } catch (OperationInterruptedException ex) {
                if (ex.Is404()) {
                    // In case of server named queues (client declared queue with channel.declare()),
                    // assume queue already exists (redeclaration would result in error anyway since queue was marked as exclusive)
                    // and publish to default exchange
                    if (RabbitMqExtensions.IsServerNamedQueue(routingKey)) {
                        this.Channel.BasicPublish("", routingKey, basicProperties, body);
                    } else {
                        this.Channel.RegisterExchangeByName(exchange);
                        this.Channel.BasicPublish(exchange, routingKey, basicProperties, body);
                    }
                }

                throw;
            }
        }

        public virtual BasicGetResult GetMessage(string queueName, bool noAck) {
            try {
                if (!_queues.Contains(queueName)) {
                    this.Channel.RegisterQueueByName(queueName);
                    _queues = new HashSet<string>(_queues) { queueName };
                }

                BasicGetResult basicMsg = this.Channel.BasicGet(queueName, noAck);
                this.GetMessageFilter?.Invoke(queueName, basicMsg);
                return basicMsg;
            } catch (OperationInterruptedException ex) {
                if (ex.Is404()) {
                    this.Channel.RegisterQueueByName(queueName);
                    return this.Channel.BasicGet(queueName, noAck);
                }

                throw;
            }
        }
    }
}
