using NUnit.Framework;
using TheOne.Redis.Queue;

namespace TheOne.Redis.Tests.Queue {

    [TestFixture]
    internal sealed class ObjectSerializerTests {

        [Test]
        public void Can_serialize_object_with_default_serializer() {
            var ser = new ObjectSerializer();
            var test = "test";
            var serialized = ser.Serialize(test);
            Assert.AreEqual(test, ser.Deserialize(serialized));
        }

        [Test]
        public void Can_serialize_object_with_optimized_serializer() {
            var ser = new OptimizedObjectSerializer();
            var test = "test";
            var serialized = ser.Serialize(test);
            Assert.AreEqual(test, ser.Deserialize(serialized));

            var testFloat = 320.0f;
            serialized = ser.Serialize(testFloat);
            Assert.AreEqual(testFloat, ser.Deserialize(serialized));
        }

    }

}
