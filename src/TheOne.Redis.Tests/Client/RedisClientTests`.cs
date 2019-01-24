using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using TheOne.Redis.Common;

namespace TheOne.Redis.Tests.Client {

    [TestFixture]
    internal sealed class GenericRedisClientTests : RedisClientTestsBase {

        #region Models

        public class Dummy {

            public int Id { get; set; }
            public string Name { get; set; }

        }

        #endregion

        public override void SetUp() {
            base.SetUp();
            this.Redis.NamespacePrefix = "GenericRedisClientTests";
        }

        [Test]
        public void Can_Delete() {
            var dto = new Dummy { Id = 1, Name = "Name" };

            this.Redis.Store(dto);

            Assert.That(this.Redis.GetAllItemsFromSet(this.PrefixedKey("ids:Dummy")).ToArray()[0], Is.EqualTo("1"));
            Assert.That(this.Redis.GetById<Dummy>(1), Is.Not.Null);

            this.Redis.Delete(dto);

            Assert.That(this.Redis.GetAllItemsFromSet(this.PrefixedKey("ids:Dummy")).Count, Is.EqualTo(0));
            Assert.That(this.Redis.GetById<Dummy>(1), Is.Null);
        }

        [Test]
        public void Can_DeleteById() {
            var dto = new Dummy { Id = 1, Name = "Name" };
            this.Redis.Store(dto);

            Assert.That(this.Redis.GetAllItemsFromSet(this.PrefixedKey("ids:Dummy")).ToArray()[0], Is.EqualTo("1"));
            Assert.That(this.Redis.GetById<Dummy>(1), Is.Not.Null);

            this.Redis.DeleteById<Dummy>(dto.Id);

            Assert.That(this.Redis.GetAllItemsFromSet(this.PrefixedKey("ids:Dummy")).Count, Is.EqualTo(0));
            Assert.That(this.Redis.GetById<Dummy>(1), Is.Null);
        }

        [Test]
        public void Can_save_via_string() {
            var dtos = Enumerable.Range(0, 10).Select(i => new Dummy { Id = i, Name = "Name" + i }).ToList();

            this.Redis.SetValue("dummy:strings", dtos.ToJson());

            var fromDtos = this.Redis.GetValue("dummy:strings").FromJson<List<Dummy>>();

            Assert.That(fromDtos.Count, Is.EqualTo(10));
        }

        [Test]
        public void Can_save_via_types() {
            var dtos = Enumerable.Range(0, 10).Select(i => new Dummy { Id = i, Name = "Name" + i }).ToList();

            this.Redis.Set("dummy:strings", dtos);

            var fromDtos = this.Redis.Get<List<Dummy>>("dummy:strings");

            Assert.That(fromDtos.Count, Is.EqualTo(10));
        }

        [Test]
        public void Can_Set_and_Get_key_with_all_byte_values() {
            const string key = "bytesKey";

            var value = new byte[256];
            for (var i = 0; i < value.Length; i++) {
                value[i] = (byte)i;
            }

            var redis = this.Redis.As<byte[]>();

            redis.SetValue(key, value);
            var resultValue = redis.GetValue(key);

            Assert.That(resultValue, Is.EquivalentTo(value));
        }

        [Test]
        public void Can_Set_and_Get_string() {
            const string value = "value";
            this.Redis.SetValue("key", value);
            var valueString = this.Redis.GetValue("key");

            Assert.That(valueString, Is.EqualTo(value));
        }

        [Test]
        public void Can_SetBit_And_GetBit_And_BitCount() {
            const string key = "BitKey";
            const int offset = 100;
            this.Redis.SetBit(key, offset, 1);
            Assert.AreEqual(1, this.Redis.GetBit(key, offset));
            Assert.AreEqual(1, this.Redis.BitCount(key));
        }

    }

}
