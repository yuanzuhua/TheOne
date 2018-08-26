using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Common;
using TheOne.Redis.Tests.Shared;

namespace TheOne.Redis.Tests.Client {

    [TestFixture(typeof(ModelWithFieldsOfDifferentTypes), typeof(ModelWithFieldsOfDifferentTypesFactory))]
    [TestFixture(typeof(Shipper), typeof(ShipperFactory))]
    [TestFixture(typeof(string), typeof(BuiltInsFactory))]
    internal sealed class PersistenceProviderTests<T, TFactory> : RedisTestBase where TFactory : class, IModelFactory<T>, new() {

        private readonly IModelFactory<T> _factory = new TFactory();
        private RedisClient _client;
        private IRedisTypedClient<T> _redis;

        [SetUp]
        public void SetUp() {
            this._client?.Dispose();
            this._client = null;
            this._client = new RedisClient(Config.MasterHost);
            this._client.FlushAll();
            this._redis = this._client.As<T>();
        }

        [Test]
        public void Can_Delete_ModelWithIdAndName() {
            List<T> tos = this._factory.CreateList();
            List<string> ids = tos.ConvertAll(x => x.GetId().ToString());

            this._redis.StoreAll(tos);

            string[] deleteIds = { ids[1], ids[3] };

            this._redis.DeleteByIds(deleteIds);

            IList<T> froms = this._redis.GetByIds(ids);
            List<string> fromIds = froms.Select(x => x.GetId().ToString()).ToList();

            List<string> expectedIds = ids.Where(x => !deleteIds.Contains(x))
                                          .ToList().ConvertAll(x => x.ToString());

            Assert.That(fromIds, Is.EquivalentTo(expectedIds));
        }

        [Test]
        public void Can_DeleteAll() {
            List<T> tos = this._factory.CreateList();
            this._redis.StoreAll(tos);

            IList<T> all = this._redis.GetAll();

            Assert.That(all.Count, Is.EqualTo(tos.Count));

            this._redis.DeleteAll();

            all = this._redis.GetAll();

            Assert.That(all.Count, Is.EqualTo(0));
        }

        [Test]
        public void Can_Store_and_GetById_ModelWithIdAndName() {
            const int modelId = 1;
            T to = this._factory.CreateInstance(modelId);
            this._redis.Store(to);

            T from = this._redis.GetById(to.GetId().ToString());

            this._factory.AssertIsEqual(to, from);
        }

        [Test]
        public void Can_StoreAll_and_GetByIds_ModelWithIdAndName() {
            List<T> tos = this._factory.CreateList();
            List<string> ids = tos.ConvertAll(x => x.GetId().ToString());

            this._redis.StoreAll(tos);

            IList<T> froms = this._redis.GetByIds(ids);
            List<string> fromIds = froms.Select(x => x.GetId().ToString()).ToList();

            Assert.That(fromIds, Is.EquivalentTo(ids));
        }

    }

}
