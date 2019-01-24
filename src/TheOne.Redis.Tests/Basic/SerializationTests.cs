using System;
using System.Text;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using TheOne.Redis.Common;
using TheOne.Redis.Tests.Shared;

namespace TheOne.Redis.Tests.Basic {

    [TestFixture]
    internal sealed class SerializationTests : RedisTestBase {

        #region Models

        public class Tuple {

            public Tuple() { }

            public Tuple(Type type, object value) {
                this.Type = type;
                this.Value = value;
            }

            public Type Type { get; set; }
            public object Value { get; set; }

        }

        #endregion

        [Test]
        public void Can_Serialize_type_with_object() {
            var obj = new CustomType { CustomId = 1, CustomName = "Name" };
            var typeWithObject = new Tuple(obj.GetType(), obj);
            var bytes = Encoding.UTF8.GetBytes(typeWithObject.ToJson());

            var bytesStr = Encoding.UTF8.GetString(bytes);
            var fromTypeWithObject = bytesStr.FromJson<Tuple>();
            var newObj = ((JObject)fromTypeWithObject.Value).ToObject<CustomType>();
            Assert.NotNull(newObj);
            Assert.That(newObj.CustomId, Is.EqualTo(obj.CustomId));
            Assert.That(newObj.CustomName, Is.EqualTo(obj.CustomName));
        }

    }

}
