using RabbitMQ.Client;
using RabbitMQ.Util;

namespace TheOne.RabbitMq {

    /// <inheritdoc />
    public class RabbitMqBasicConsumer : DefaultBasicConsumer {

        public RabbitMqBasicConsumer(IModel model) : this(model, new SharedQueue<BasicGetResult>()) { }

        public RabbitMqBasicConsumer(IModel model, SharedQueue<BasicGetResult> queue) : base(model) {
            this.Queue = queue;
        }

        public SharedQueue<BasicGetResult> Queue { get; }

        public override void OnCancel() {
            this.Queue.Close();
            base.OnCancel();
        }

        public override void HandleBasicDeliver(
            string consumerTag, ulong deliveryTag, bool redelivered, string exchange,
            string routingKey, IBasicProperties properties, byte[] body) {
            var msgResult = new BasicGetResult(
                deliveryTag,
                redelivered,
                exchange,
                routingKey,
                0, // Not available, received by RabbitMQ when declaring queue
                properties,
                body);

            this.Queue.Enqueue(msgResult);
        }
    }
}
