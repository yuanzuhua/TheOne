using System.Collections.Generic;
using NUnit.Framework;
using TheOne.RabbitMq.Extensions;

namespace TheOne.RabbitMq.Tests.Extensions {

    [TestFixture]
    internal sealed class TypeNameExtensionsTests {

        #region Models

        private sealed class Poco { }

        private sealed class Root {

            public sealed class Nested { }

        }

        #endregion

        [Test]
        public void Can_expand_generic_List() {
            var genericName = typeof(List<Poco>).ExpandTypeName();
            Assert.That(genericName, Is.EqualTo("List<TypeNameExtensionsTests.Poco>"));
        }

        [Test]
        public void Can_expand_generic_List_and_Dictionary() {
            var genericName = typeof(Dictionary<string, List<Poco>>).ExpandTypeName();
            Assert.That(genericName, Is.EqualTo("Dictionary<String,List<TypeNameExtensionsTests.Poco>>"));
        }

        [Test]
        public void Can_expand_Nullable_array() {
            var a = new byte?[0];
            var name = a.GetType().ExpandTypeName();
            Assert.That(name, Is.EqualTo("Nullable<Byte>[]"));
        }

        [Test]
        public void Can_expand_type_name() {
            Assert.That(typeof(Root).ExpandTypeName(), Is.EqualTo("TypeNameExtensionsTests.Root"));
            Assert.That(typeof(Root.Nested).ExpandTypeName(), Is.EqualTo("TypeNameExtensionsTests.Root.Nested"));
        }

    }

}
