using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;

namespace TheOne.Redis.Tests.Client {

    [TestFixture]
    internal sealed class PersistenceProviderTests : RedisClientTestsBase {

        #region Models

        public class TestModel : IHasRedisId<Guid> {

            public string Name { get; set; }
            public int Age { get; set; }
            public Guid Id { get; set; }

            public bool Equals(TestModel other) {
                if (ReferenceEquals(null, other)) {
                    return false;
                }

                if (ReferenceEquals(this, other)) {
                    return true;
                }

                return other.Id.Equals(this.Id) && Equals(other.Name, this.Name) && other.Age == this.Age;
            }

            public override bool Equals(object obj) {
                if (ReferenceEquals(null, obj)) {
                    return false;
                }

                if (ReferenceEquals(this, obj)) {
                    return true;
                }

                if (obj.GetType() != typeof(TestModel)) {
                    return false;
                }

                return this.Equals((TestModel)obj);
            }

            public override int GetHashCode() {
                unchecked {
                    var result = this.Id.GetHashCode();
                    result = (result * 397) ^ (this.Name != null ? this.Name.GetHashCode() : 0);
                    result = (result * 397) ^ this.Age;
                    return result;
                }
            }

            public static List<TestModel> GetModels() {
                var list = new List<TestModel>();
                for (var i = 0; i < 5; i++) {
                    list.Add(new TestModel { Id = Guid.NewGuid(), Name = "Name" + i, Age = 20 + i });
                }

                return list;
            }

        }

        #endregion

        private static readonly string _testModelIdsSetKey = "ids:" + typeof(TestModel).Name;

        private List<TestModel> _testModels;

        [SetUp]
        public override void SetUp() {
            base.SetUp();
            this.Redis.NamespacePrefix = "RedisBasicPersistenceProviderTests";
            this._testModels = TestModel.GetModels();
        }

        [Test]
        public void Can_Delete() {
            this.Redis.StoreAll(this._testModels);

            var last = this._testModels.Last();
            this.Redis.Delete(last);

            this._testModels.Remove(last);

            var allModels = this.Redis.GetAll<TestModel>().OrderBy(x => x.Age).ToList();

            Assert.That(allModels, Is.EquivalentTo(this._testModels));

            // Test internal TestModelIdsSetKey state
            var idsRemaining = this.Redis.GetAllItemsFromSet(this.PrefixedKey(_testModelIdsSetKey))
                                   .OrderBy(x => x).Select(x => new Guid(x)).ToList();

            var testModelIds = this._testModels.OrderBy(x => x.Id).Select(x => x.Id).ToList();

            Assert.That(idsRemaining, Is.EquivalentTo(testModelIds));
        }

        [Test]
        public void Can_DeleteAll() {
            this.Redis.StoreAll(this._testModels);

            this.Redis.DeleteAll<TestModel>();

            var allModels = this.Redis.GetAll<TestModel>();

            Assert.That(allModels, Is.Empty);

            // Test internal TestModelIdsSetKey state
            var idsRemaining = this.Redis.GetAllItemsFromSet(_testModelIdsSetKey);
            Assert.That(idsRemaining, Is.Empty);
        }

        [Test]
        public void Can_DeleteByIds() {
            this.Redis.StoreAll(this._testModels);

            var evenTestModels = this._testModels.Where(x => x.Age % 2 == 0)
                                     .OrderBy(x => x.Id).ToList();
            var evenTestModelIds = evenTestModels.Select(x => x.Id).ToList();

            this.Redis.DeleteByIds<TestModel>(evenTestModelIds);

            evenTestModels.ForEach(x => this._testModels.Remove(x));

            var allModels = this.Redis.GetAll<TestModel>().OrderBy(x => x.Age).ToList();

            Assert.That(allModels, Is.EqualTo(this._testModels));


            // Test internal TestModelIdsSetKey state
            var idsRemaining = this.Redis.GetAllItemsFromSet(this.PrefixedKey(_testModelIdsSetKey))
                                   .OrderBy(x => x).Select(x => new Guid(x)).ToList();

            var testModelIds = this._testModels.OrderBy(x => x.Id).Select(x => x.Id).ToList();

            Assert.That(idsRemaining, Is.EquivalentTo(testModelIds));
        }

        [Test]
        public void Can_GetById() {
            this.Redis.StoreAll(this._testModels);

            var last = this._testModels.Last();
            var lastById = this.Redis.GetById<TestModel>(last.Id);

            Assert.That(lastById, Is.EqualTo(last));
        }

        [Test]
        public void Can_GetByIds() {
            this.Redis.StoreAll(this._testModels);

            var evenTestModels = this._testModels.Where(x => x.Age % 2 == 0).OrderBy(x => x.Id).ToList();
            var evenTestModelIds = evenTestModels.Select(x => x.Id).ToList();

            var selectedModels = this.Redis.GetByIds<TestModel>(evenTestModelIds).OrderBy(x => x.Id).ToList();
            Assert.That(selectedModels, Is.EqualTo(evenTestModels));
        }

        [Test]
        public void Can_Store() {
            this._testModels.ForEach(x => this.Redis.Store(x));

            var allModels = this.Redis.GetAll<TestModel>().OrderBy(x => x.Age).ToList();

            Assert.That(allModels, Is.EquivalentTo(this._testModels));
        }

        [Test]
        public void Can_StoreAll() {
            this.Redis.StoreAll(this._testModels);

            var allModels = this.Redis.GetAll<TestModel>().OrderBy(x => x.Age).ToList();

            Assert.That(allModels, Is.EquivalentTo(this._testModels));
        }

        [Test]
        public void Can_WriteAll() {
            this.Redis.WriteAll(this._testModels);

            var testModelIds = this._testModels.ConvertAll(x => x.Id);

            var allModels = this.Redis.GetByIds<TestModel>(testModelIds)
                                .OrderBy(x => x.Age).ToList();

            Assert.That(allModels, Is.EquivalentTo(this._testModels));
        }

    }

}
