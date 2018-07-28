using NUnit.Framework;
using TheOne.RabbitMq.InMemoryMq;
using TheOne.RabbitMq.Interfaces;

namespace TheOne.RabbitMq.Tests.Messaging.Interfaces {

    [TestFixture]
    internal abstract class MqMessagingHostTestBase {

        [SetUp]
        public virtual void OnBeforeEachTest() {
            this.CreateMessageFactory();
        }

        protected abstract IMqMessageFactory CreateMessageFactory();

        protected abstract MqTransientMessageServiceBase CreateMessagingService();
    }
}
