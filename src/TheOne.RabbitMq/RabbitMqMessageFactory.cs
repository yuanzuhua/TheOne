using System;
using RabbitMQ.Client;
using TheOne.RabbitMq.Interfaces;

namespace TheOne.RabbitMq {

    /// <inheritdoc />
    public class RabbitMqMessageFactory : IMqMessageFactory {

        private int _retryCount;

        /// <inheritdoc />
        public RabbitMqMessageFactory(string hostName = "localhost")
            : this(new RabbitMqConnectionModel { HostName = hostName }) {
            //
        }

        /// <inheritdoc />
        public RabbitMqMessageFactory(RabbitMqConnectionModel model) {
            if (model == null) {
                throw new ArgumentNullException(nameof(model));
            }

            this.ConnectionFactory = RabbitMqConnectionModel.CreateConnectionFactory(model);
            this.ConnectionFactory.RequestedHeartbeat = 10;
        }

        /// <inheritdoc />
        public RabbitMqMessageFactory(ConnectionFactory connectionFactory) {
            this.ConnectionFactory = connectionFactory;
        }

        public ConnectionFactory ConnectionFactory { get; }
        public Action<string, IBasicProperties, IMqMessage> PublishMessageFilter { get; set; }
        public Action<string, BasicGetResult> GetMessageFilter { get; set; }

        /// <summary>
        ///     Rabbit MQ RetryCount must be 0-1
        /// </summary>
        public int RetryCount {
            get => this._retryCount;
            set {
                if (value < 0 || value > 1) {
                    throw new ArgumentOutOfRangeException(nameof(this.RetryCount),
                        "Rabbit MQ RetryCount must be 0-1");
                }

                this._retryCount = value;
            }
        }

        public bool UsePolling { get; set; }

        /// <inheritdoc />
        public virtual IMqMessageQueueClient CreateMessageQueueClient() {
            return new RabbitMqQueueClient(this) {
                RetryCount = this.RetryCount,
                PublishMessageFilter = this.PublishMessageFilter,
                GetMessageFilter = this.GetMessageFilter
            };
        }

        /// <inheritdoc />
        public virtual IMqMessageProducer CreateMessageProducer() {
            return new RabbitMqProducer(this) {
                RetryCount = this.RetryCount,
                PublishMessageFilter = this.PublishMessageFilter,
                GetMessageFilter = this.GetMessageFilter
            };
        }

        /// <inheritdoc />
        public virtual void Dispose() { }

    }

}
