using System;
using NUnit.Framework;
using TheOne.RabbitMq.Extensions;

namespace TheOne.RabbitMq.Tests.Extensions {

    [TestFixture]
    internal sealed class DateTimeExtensionsTests {

        [Test]
        public void Can_serialize_Unix_DateTime() {
            var maxDate = DateTime.MaxValue.ToUnixTime();
            var minDate = DateTime.MinValue.ToUnixTime();
            Console.WriteLine(maxDate);
            Console.WriteLine(minDate);
        }
    }
}
