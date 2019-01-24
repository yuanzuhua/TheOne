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
            var tos = this._factory.CreateList();
            var ids = tos.ConvertAll(x => x.GetId().ToString());

            this._redis.StoreAll(tos);

            string[] deleteIds = { ids[1], ids[3] };

            this._redis.DeleteByIds(deleteIds);

            var froms = this._redis.GetByIds(ids);
            var fromIds = froms.Select(x => x.GetId().ToString()).ToList();

            var expectedIds = ids.Where(x => !deleteIds.Contains(x))
                                 .ToList().ConvertAll(x => x.ToString());

            Assert.That(fromIds, Is.EquivalentTo(expectedIds));
        }

        [Test]
        public void Can_DeleteAll() {
            var tos = this._factory.CreateList();
            this._redis.StoreAll(tos);

            var all = this._redis.GetAll();

            Assert.That(all.Count, Is.EqualTo(tos.Count));

            this._redis.DeleteAll();

            all = this._redis.GetAll();

            Assert.That(all.Count, Is.EqualTo(0));
        }

        [Test]
        public void Can_Store_and_GetById_ModelWithIdAndName() {
            const int modelId = 1;
            var to = this._factory.CreateInstance(modelId);
            this._redis.Store(to);

            var from = this._redis.GetById(to.GetId().ToString());

            this._factory.AssertIsEqual(to, from);
        }

        [Test]
        public void Can_StoreAll_and_GetByIds_ModelWithIdAndName() {
            var tos = this._factory.CreateList();
            var ids = tos.ConvertAll(x => x.GetId().ToString());

            this._redis.StoreAll(tos);

            var froms = this._redis.GetByIds(ids);
            var fromIds = froms.Select(x => x.GetId().ToString()).ToList();

            Assert.That(fromIds, Is.EquivalentTo(ids));
        }

    }

}
