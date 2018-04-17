using System;
using TheOne.RabbitMq.Interfaces;

namespace TheOne.RabbitMq.InMemoryMq {

    internal class InMemoryMqTransientMessageService : MqTransientMessageServiceBase {

        public InMemoryMqTransientMessageService() : this(null) { }

        public InMemoryMqTransientMessageService(InMemoryMqTransientMessageFactory factory) {
            this.Factory = factory ?? new InMemoryMqTransientMessageFactory(this);
            this.Factory.MqFactory.MessageReceived += this.factory_MessageReceived;
        }

        internal InMemoryMqTransientMessageFactory Factory { get; set; }

        /// <inheritdoc />
        public override IMqMessageFactory MessageFactory => this.Factory;

        public InMemoryMqMessageQueueClientFactory MessageQueueFactory => this.Factory.MqFactory;

        private void factory_MessageReceived(object sender, EventArgs e) {
            this.Start();
        }
    }
}
