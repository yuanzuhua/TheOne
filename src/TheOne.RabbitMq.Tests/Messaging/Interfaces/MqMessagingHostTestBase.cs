using NUnit.Framework;
using TheOne.RabbitMq.InMemoryMq;
using TheOne.RabbitMq.Interfaces;

namespace TheOne.RabbitMq.Tests.Messaging.Interfaces {

    [TestFixture]
    internal abstract class MqMessagingHostTestBase {

        protected abstract IMqMessageFactory CreateMessageFactory();

        protected abstract MqTransientMessageServiceBase CreateMessagingService();

        [SetUp]
        public virtual void OnBeforeEachTest() {
            this.CreateMessageFactory();
        }

    }

}
