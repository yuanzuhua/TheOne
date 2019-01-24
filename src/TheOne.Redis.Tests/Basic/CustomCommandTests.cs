using System;
using System.Linq;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Common;
using TheOne.Redis.Tests.Shared;

namespace TheOne.Redis.Tests.Basic {

    [TestFixture]
    internal sealed class CustomCommandTests : RedisClientTestsBase {

        [Test]
        public void Can_send_complex_types_in_Custom_Commands() {
            var ret = this.Redis.Custom("SET", "foo", new Poco { Name = "Bar" });
            Assert.That(ret.Text, Is.EqualTo("OK"));

            ret = this.Redis.Custom("GET", "foo");
            var dto = ret.GetResult<Poco>();
            Assert.That(dto.Name, Is.EqualTo("Bar"));

            Enum.GetNames(typeof(DayOfWeek)).ToList()
                .ForEach(x => this.Redis.Custom("RPUSH", "DaysOfWeek", new Poco { Name = x }));

            ret = this.Redis.Custom("LRANGE", "DaysOfWeek", 1, -2);
            var weekDays = ret.GetResults<Poco>();

            Assert.That(weekDays.First().Name, Is.EqualTo("Monday"));

            Console.WriteLine(ret.ToJson());
        }

        [Test]
        public void Can_send_custom_commands() {
            var ret = this.Redis.Custom("SET", "foo", 1);
            Assert.That(ret.Text, Is.EqualTo("OK"));
            ret = this.Redis.Custom(Commands.Set, "bar", "b");
            Assert.That(ret.Text, Is.EqualTo("OK"));

            ret = this.Redis.Custom("GET", "foo");
            Assert.That(ret.Text, Is.EqualTo("1"));
            ret = this.Redis.Custom(Commands.Get, "bar");
            Assert.That(ret.Text, Is.EqualTo("b"));

            ret = this.Redis.Custom(Commands.Keys, "*");
            var keys = ret.GetResults();
            Assert.That(keys, Is.EquivalentTo(new[] { "foo", "bar" }));

            ret = this.Redis.Custom("MGET", "foo", "bar");
            var values = ret.GetResults();
            Assert.That(values, Is.EquivalentTo(new[] { "1", "b" }));

            Enum.GetNames(typeof(DayOfWeek)).ToList()
                .ForEach(x => this.Redis.Custom("RPUSH", "DaysOfWeek", x));

            ret = this.Redis.Custom("LRANGE", "DaysOfWeek", 1, -2);

            var weekDays = ret.GetResults();
            Assert.That(weekDays,
                Is.EquivalentTo(
                    new[] { "Monday", "Tuesday", "Wednesday", "Thursday", "Friday" }));

            Console.WriteLine(ret.ToJson());
        }

    }

}
