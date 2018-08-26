using NUnit.Framework;
using TheOne.Redis.Client;

namespace TheOne.Redis.Tests.Basic {

    [TestFixture]
    internal sealed class TwemproxyTests : RedisTestBase {

        [Test]
        public void Can_connect_to_twemproxy() {
            var redis = new RedisClient(Config.MasterHost);
            redis.SetValue("foo", "bar");
            var foo = redis.GetValue("foo");
            Assert.That(foo, Is.EqualTo("bar"));
        }

    }

}
