using System.Collections.Generic;
using NUnit.Framework;
using TheOne.Redis.ClientManager;
using TheOne.Redis.Tests.Extensions;
using TheOne.Redis.Tests.Shared;

namespace TheOne.Redis.Tests.ClientManager {

    internal sealed class RedisClientManagerExtensionsTests : RedisTestBase {

        private IRedisClientManager _redisManager;

        [SetUp]
        public void SetUp() {
            this._redisManager = new BasicRedisClientManager(Config.MasterHost);
            this._redisManager.Exec(r => r.FlushAll());
        }

        [TearDown]
        public void TearDown() {
            this._redisManager?.Dispose();
            this._redisManager = null;
        }

        [Test]
        public void Can_Exec_Action() {
            this._redisManager.Exec(r => {
                r.Increment("key", 1);
                Assert.That(r.Get<int>("key"), Is.EqualTo(1));
            });
        }

        [Test]
        public void Can_Exec_Func_bool() {
            var value = this._redisManager.Exec(r => {
                r.AddItemToSet("set", "item");
                return r.SetContainsItem("set", "item");
            });

            Assert.That(value, Is.True);
        }

        [Test]
        public void Can_Exec_Func_double() {
            var value = this._redisManager.Exec(r => {
                r.AddItemToSortedSet("zset", "value", 1.1d);
                return r.GetItemScoreInSortedSet("zset", "value");
            });

            Assert.That(value, Is.EqualTo(1.1d));
        }

        [Test]
        public void Can_Exec_Func_int() {
            var value = this._redisManager.Exec(r => {
                r.AddItemToList("list", "value");
                return r.GetListCount("list");
            });
            Assert.That(value, Is.EqualTo(1));
        }

        [Test]
        public void Can_Exec_Func_long() {
            var value = this._redisManager.Exec(r => r.Increment("key", 1));
            Assert.That(value, Is.EqualTo(1));
        }

        [Test]
        public void Can_Exec_Func_string() {
            var value = this._redisManager.Exec(r => {
                r.SetValue("key", "value");
                return r.GetValue("key");
            });
            Assert.That(value, Is.EqualTo("value"));
        }

        [Test]
        public void Can_Exec_Transaction_Action() {
            var value = false;
            this._redisManager.ExecTrans(trans => {
                trans.QueueCommand(r => r.AddItemToSet("set", "item"));
                trans.QueueCommand(r => r.SetContainsItem("set", "item"), x => value = x);
            });

            Assert.That(value, Is.True);
        }

        [Test]
        public void Can_ExecAs_ModelWithIdAndName_Action() {
            var expected = ModelWithIdAndName.Create(1);
            this._redisManager.ExecAs<ModelWithIdAndName>(m => {
                m.Store(expected);
                var actual = m.GetById(expected.Id);
                Assert.That(actual, Is.EqualTo(expected));
            });
        }

        [Test]
        public void Can_ExecAs_ModelWithIdAndName_Func() {
            var expected = ModelWithIdAndName.Create(1);
            var actual = this._redisManager.ExecAs<ModelWithIdAndName>(m => {
                m.Store(expected);
                return m.GetById(expected.Id);
            });
            Assert.That(actual, Is.EqualTo(expected));
        }

        [Test]
        public void Can_ExecAs_ModelWithIdAndName_Func_IList() {
            ModelWithIdAndName[] expected = {
                ModelWithIdAndName.Create(1),
                ModelWithIdAndName.Create(2),
                ModelWithIdAndName.Create(3)
            };
            var actual = this._redisManager.ExecAs<ModelWithIdAndName>(m => {
                var list = m.Lists["typed-list"];
                list.AddRange(expected);
                return (IList<ModelWithIdAndName>)list.GetAll();
            });
            Assert.That(actual.EquivalentTo(expected));
        }

        [Test]
        public void Can_ExecAs_ModelWithIdAndName_Func_List() {
            ModelWithIdAndName[] expected = {
                ModelWithIdAndName.Create(1),
                ModelWithIdAndName.Create(2),
                ModelWithIdAndName.Create(3)
            };
            var actual = this._redisManager.ExecAs<ModelWithIdAndName>(m => {
                var list = m.Lists["typed-list"];
                list.AddRange(expected);
                return list.GetAll();
            });
            Assert.That(actual.EquivalentTo(expected));
        }

    }

}
