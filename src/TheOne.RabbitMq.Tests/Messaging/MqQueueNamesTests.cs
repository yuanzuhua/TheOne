using NUnit.Framework;
using TheOne.RabbitMq.Models;
using TheOne.RabbitMq.Tests.Messaging.Models;

namespace TheOne.RabbitMq.Tests.Messaging {

    [TestFixture]
    internal sealed class MqQueueNamesTests {

        #region Models

        private sealed class TestFilter { }

        private sealed class TestPrefix { }

        #endregion

        [Test]
        public void Can_determine_TempQueue() {
            var tmpName = MqQueueNames.GetTempQueueName();
            Assert.That(MqQueueNames.IsTempQueue(tmpName), Is.True);
        }

        [Test]
        public void Can_determine_TempQueue_with_Custom_QueueNameFm() {
            MqQueueNames.ResolveQueueNameFn = (typeName, suffix) =>
                string.Format("SITE.{0}{1}", typeName, suffix.ToUpper());

            var tmpName = MqQueueNames.GetTempQueueName();
            Assert.That(MqQueueNames.IsTempQueue(tmpName), Is.True);

            MqQueueNames.ResolveQueueNameFn = MqQueueNames.ResolveQueueName;
        }

        [Test]
        public void Can_determine_TempQueue_with_Custom_QueuePrefix() {
            MqQueueNames.SetQueuePrefix("site1.");

            var tmpName = MqQueueNames.GetTempQueueName();
            Assert.That(MqQueueNames.IsTempQueue(tmpName), Is.True);

            MqQueueNames.SetQueuePrefix("");
        }

        [Test]
        public void Does_resolve_QueueNames_using_Custom_Filter() {
            MqQueueNames.ResolveQueueNameFn = (typeName, suffix) =>
                string.Format("SITE.{0}{1}", typeName, suffix.ToUpper());

            Assert.That(new MqQueueNames(typeof(TestFilter)).Direct, Is.EqualTo("SITE.TestFilter.DIRECT"));
            Assert.That(MqQueueNames<TestFilter>.Direct, Is.EqualTo("SITE.TestFilter.DIRECT"));

            MqQueueNames.ResolveQueueNameFn = MqQueueNames.ResolveQueueName;
        }

        [Test]
        public void Does_resolve_QueueNames_using_QueuePrefix() {
            MqQueueNames.SetQueuePrefix("site1.");

            Assert.That(new MqQueueNames(typeof(TestPrefix)).Direct, Is.EqualTo("site1.theone:mq.TestPrefix.direct"));
            Assert.That(MqQueueNames<TestPrefix>.Direct, Is.EqualTo("site1.theone:mq.TestPrefix.direct"));

            MqQueueNames.SetQueuePrefix("");
        }

        [Test]
        public void Does_resolve_the_same_default_QueueNames() {
            Assert.That(new MqQueueNames(typeof(HelloIntro)).Direct, Is.EqualTo("theone:mq.HelloIntro.direct"));
            Assert.That(MqQueueNames<HelloIntro>.Direct, Is.EqualTo("theone:mq.HelloIntro.direct"));
        }

        [Test]
        public void Does_serialize_to_correct_MQ_name() {
            const string expected = "theone:mq.Greet.direct";
            Assert.That(MqQueueNames<Greet>.Direct, Is.EqualTo(expected));
        }

    }

}
