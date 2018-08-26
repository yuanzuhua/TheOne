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
                List<ModelWithIdAndName> tos = ids.ConvertAll(ModelWithIdAndName.Create);

                redis.StoreAll(tos);

                var deleteIds = new List<int> { 2, 4 };

                redis.DeleteByIds<ModelWithIdAndName>(deleteIds);

                IList<ModelWithIdAndName> froms = redis.GetByIds<ModelWithIdAndName>(ids);
                List<int> fromIds = froms.Select(x => x.Id).ToList();

                List<int> expectedIds = ids.Where(x => !deleteIds.Contains(x)).ToList();

                Assert.That(fromIds, Is.EquivalentTo(expectedIds));
            }
        }

        [Test]
        public void Can_Store_and_GetById_ModelWithIdAndName() {
            using (var redis = new RedisClient(Config.MasterHost)) {
                const int modelId = 1;
                ModelWithIdAndName to = ModelWithIdAndName.Create(modelId);
                redis.Store(to);

                var from = redis.GetById<ModelWithIdAndName>(modelId);

                ModelWithIdAndName.AssertIsEqual(to, from);
            }
        }

        [Test]
        public void Can_StoreAll_and_GetByIds_ModelWithIdAndName() {
            using (var redis = new RedisClient(Config.MasterHost)) {
                var ids = new[] { 1, 2, 3, 4, 5 };
                List<ModelWithIdAndName> tos = ids.Select(ModelWithIdAndName.Create).ToList();

                redis.StoreAll(tos);

                IList<ModelWithIdAndName> froms = redis.GetByIds<ModelWithIdAndName>(ids);
                List<int> fromIds = froms.Select(x => x.Id).ToList();

                Assert.That(fromIds, Is.EquivalentTo(ids));
            }
        }

    }

}
