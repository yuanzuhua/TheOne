using NUnit.Framework;
using TheOne.RabbitMq.Interfaces;
using TheOne.RabbitMq.Messaging;
using TheOne.RabbitMq.Models;
using TheOne.RabbitMq.Tests.Messaging.Models;

namespace TheOne.RabbitMq.Tests.Messaging {

    [TestFixture]
    internal sealed class MqMessagingTests {

        [Test]
        public void Can_serialize_IMqMessage_into_typed_Message() {
            var dto = new Incr { Value = 1 };
            IMqMessage iMsg = MqMessageFactory.Create(dto);
            byte[] jsonBytes = MqMessageExtensions.ToJsonBytes((object)iMsg);
            IMqMessage<Incr> typedMessage = MqMessageExtensions.FromJsonBytes<MqMessage<Incr>>(jsonBytes);
            Assert.That(typedMessage.GetBody().Value, Is.EqualTo(dto.Value));
        }

    }

}
