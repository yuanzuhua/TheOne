using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Tests.Shared;

namespace TheOne.Redis.Tests.Client {

    [TestFixture]
    internal sealed class PersistenceProvider2Tests : RedisTestBase {

        [Test]
        public void Can_Delete_ModelWithIdAndName() {
            using (var redis = new RedisClient(Config.MasterHost)) {
                var ids = new List<int> { 1, 2, 3, 4, 5 };
                var tos = ids.ConvertAll(ModelWithIdAndName.Create);

                redis.StoreAll(tos);

                var deleteIds = new List<int> { 2, 4 };

                redis.DeleteByIds<ModelWithIdAndName>(deleteIds);

                var froms = redis.GetByIds<ModelWithIdAndName>(ids);
                var fromIds = froms.Select(x => x.Id).ToList();

                var expectedIds = ids.Where(x => !deleteIds.Contains(x)).ToList();

                Assert.That(fromIds, Is.EquivalentTo(expectedIds));
            }
        }

        [Test]
        public void Can_Store_and_GetById_ModelWithIdAndName() {
            using (var redis = new RedisClient(Config.MasterHost)) {
                const int modelId = 1;
                var to = ModelWithIdAndName.Create(modelId);
                redis.Store(to);

                var from = redis.GetById<ModelWithIdAndName>(modelId);

                ModelWithIdAndName.AssertIsEqual(to, from);
            }
        }

        [Test]
        public void Can_StoreAll_and_GetByIds_ModelWithIdAndName() {
            using (var redis = new RedisClient(Config.MasterHost)) {
                var ids = new[] { 1, 2, 3, 4, 5 };
                var tos = ids.Select(ModelWithIdAndName.Create).ToList();

                redis.StoreAll(tos);

                var froms = redis.GetByIds<ModelWithIdAndName>(ids);
                var fromIds = froms.Select(x => x.Id).ToList();

                Assert.That(fromIds, Is.EquivalentTo(ids));
            }
        }

    }

}
