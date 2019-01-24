using System;
using System.Collections.Generic;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Tests.Shared;

namespace TheOne.Redis.Tests.Client {

    [TestFixture]
    internal sealed class RedisTypedPipelineTests : RedisClientTestsBase {

        private const string _key = "gmultitest";
        private const string _listKey = "gmultitest-list";
        private const string _setKey = "gmultitest-set";
        private const string _sortedSetKey = "gmultitest-sortedset";
        private readonly ShipperFactory _modelFactory = new ShipperFactory();
        private Shipper _model;
        private IRedisTypedClient<Shipper> _typedClient;

        public override void SetUp() {
            base.SetUp();
            this._typedClient = this.Redis.As<Shipper>();
            this._model = this._modelFactory.CreateInstance(1);
        }

        [Test]
        public void Can_call_multi_string_operations_in_pipeline() {
            Shipper item1 = null;
            Shipper item4 = null;

            var results = new List<Shipper>();

            var typedList = this._typedClient.Lists[_listKey];
            Assert.That(typedList.Count, Is.EqualTo(0));

            using (var pipeline = this._typedClient.CreatePipeline()) {
                pipeline.QueueCommand(r => r.AddItemToList(typedList, this._modelFactory.CreateInstance(1)));
                pipeline.QueueCommand(r => r.AddItemToList(typedList, this._modelFactory.CreateInstance(2)));
                pipeline.QueueCommand(r => r.AddItemToList(typedList, this._modelFactory.CreateInstance(3)));
                pipeline.QueueCommand(r => r.GetAllItemsFromList(typedList), x => results = x);
                pipeline.QueueCommand(r => r.GetItemFromList(typedList, 0), x => item1 = x);
                pipeline.QueueCommand(r => r.GetItemFromList(typedList, 4), x => item4 = x);

                pipeline.Flush();
            }

            Assert.That(typedList.Count, Is.EqualTo(3));

            this._modelFactory.AssertListsAreEqual(results,
                new List<Shipper> {
                    this._modelFactory.CreateInstance(1),
                    this._modelFactory.CreateInstance(2),
                    this._modelFactory.CreateInstance(3)
                });

            this._modelFactory.AssertIsEqual(item1, this._modelFactory.CreateInstance(1));
            Assert.That(item4, Is.Null);
        }

        [Test]
        public void Can_call_single_operation_3_Times_in_pipeline() {
            var typedList = this._typedClient.Lists[_listKey];
            Assert.That(typedList.Count, Is.EqualTo(0));

            using (var pipeline = this._typedClient.CreatePipeline()) {
                pipeline.QueueCommand(r => r.AddItemToList(typedList, this._modelFactory.CreateInstance(1)));
                pipeline.QueueCommand(r => r.AddItemToList(typedList, this._modelFactory.CreateInstance(2)));
                pipeline.QueueCommand(r => r.AddItemToList(typedList, this._modelFactory.CreateInstance(3)));

                pipeline.Flush();
            }

            Assert.That(typedList.Count, Is.EqualTo(3));
        }


        [Test]
        public void Can_call_single_operation_in_pipeline() {
            Assert.That(this._typedClient.GetValue(_key), Is.Null);

            using (var pipeline = this._typedClient.CreatePipeline()) {
                pipeline.QueueCommand(r => r.SetValue(_key, this._model));

                pipeline.Flush();
            }

            this._modelFactory.AssertIsEqual(this._typedClient.GetValue(_key), this._model);
        }

        [Test]
        public void Can_call_single_operation_with_callback_3_Times_in_pipeline() {
            var results = new List<int>();

            var typedList = this._typedClient.Lists[_listKey];
            Assert.That(typedList.Count, Is.EqualTo(0));

            using (var pipeline = this._typedClient.CreatePipeline()) {
                pipeline.QueueCommand(r => r.AddItemToList(typedList, this._modelFactory.CreateInstance(1)), () => results.Add(1));
                pipeline.QueueCommand(r => r.AddItemToList(typedList, this._modelFactory.CreateInstance(2)), () => results.Add(2));
                pipeline.QueueCommand(r => r.AddItemToList(typedList, this._modelFactory.CreateInstance(3)), () => results.Add(3));

                pipeline.Flush();
            }

            Assert.That(typedList.Count, Is.EqualTo(3));
            Assert.That(results, Is.EquivalentTo(new List<int> { 1, 2, 3 }));
        }

        [Test]
        public void Exception_in_atomic_pipelines_discards_all_commands() {
            Assert.That(this._typedClient.GetValue(_key), Is.Null);
            try {
                using (var pipeline = this._typedClient.CreatePipeline()) {
                    pipeline.QueueCommand(r => r.SetValue(_key, this._model));
                    throw new NotSupportedException();
                }
            } catch (NotSupportedException) {
                Assert.That(this._typedClient.GetValue(_key), Is.Null);
            }
        }

        [Test]
        public void No_commit_of_atomic_pipelines_discards_all_commands() {
            Assert.That(this._typedClient.GetValue(_key), Is.Null);

            using (var pipeline = this._typedClient.CreatePipeline()) {
                pipeline.QueueCommand(r => r.SetValue(_key, this._model));
            }

            Assert.That(this._typedClient.GetValue(_key), Is.Null);
        }

        [Test]
        public void Pipeline_can_be_replayed() {
            const string keySquared = _key + _key;
            Assert.That(this.Redis.GetValue(_key), Is.Null);
            Assert.That(this.Redis.GetValue(keySquared), Is.Null);
            using (var pipeline = this._typedClient.CreatePipeline()) {
                pipeline.QueueCommand(r => r.IncrementValue(_key));
                pipeline.QueueCommand(r => r.IncrementValue(keySquared));
                pipeline.Flush();

                Assert.That(this.Redis.GetValue(_key), Is.EqualTo("1"));
                Assert.That(this.Redis.GetValue(keySquared), Is.EqualTo("1"));
                this._typedClient.RemoveEntry(_key);
                this._typedClient.RemoveEntry(keySquared);
                Assert.That(this.Redis.GetValue(_key), Is.Null);
                Assert.That(this.Redis.GetValue(keySquared), Is.Null);

                pipeline.Replay();
                pipeline.Dispose();
                Assert.That(this.Redis.GetValue(_key), Is.EqualTo("1"));
                Assert.That(this.Redis.GetValue(keySquared), Is.EqualTo("1"));
            }

        }

        [Test]
        public void Supports_different_operation_types_in_same_pipeline() {
            var incrementResults = new List<long>();
            var collectionCounts = new List<long>();
            var containsItem = false;

            var typedList = this._typedClient.Lists[_listKey];
            var typedSet = this._typedClient.Sets[_setKey];
            var typedSortedSet = this._typedClient.SortedSets[_sortedSetKey];

            Assert.That(this._typedClient.GetValue(_key), Is.Null);
            using (var pipeline = this._typedClient.CreatePipeline()) {
                pipeline.QueueCommand(r => r.IncrementValue(_key), intResult => incrementResults.Add(intResult));
                pipeline.QueueCommand(r => r.AddItemToList(typedList, this._modelFactory.CreateInstance(1)));
                pipeline.QueueCommand(r => r.AddItemToList(typedList, this._modelFactory.CreateInstance(2)));
                pipeline.QueueCommand(r => r.AddItemToSet(typedSet, this._modelFactory.CreateInstance(3)));
                pipeline.QueueCommand(r => r.SetContainsItem(typedSet, this._modelFactory.CreateInstance(3)), b => containsItem = b);
                pipeline.QueueCommand(r => r.AddItemToSortedSet(typedSortedSet, this._modelFactory.CreateInstance(4)));
                pipeline.QueueCommand(r => r.AddItemToSortedSet(typedSortedSet, this._modelFactory.CreateInstance(5)));
                pipeline.QueueCommand(r => r.AddItemToSortedSet(typedSortedSet, this._modelFactory.CreateInstance(6)));
                pipeline.QueueCommand(r => r.GetListCount(typedList), intResult => collectionCounts.Add(intResult));
                pipeline.QueueCommand(r => r.GetSetCount(typedSet), intResult => collectionCounts.Add(intResult));
                pipeline.QueueCommand(r => r.GetSortedSetCount(typedSortedSet), intResult => collectionCounts.Add(intResult));
                pipeline.QueueCommand(r => r.IncrementValue(_key), intResult => incrementResults.Add(intResult));

                pipeline.Flush();
            }

            Assert.That(containsItem, Is.True);
            Assert.That(this.Redis.GetValue(_key), Is.EqualTo("2"));
            Assert.That(incrementResults, Is.EquivalentTo(new List<int> { 1, 2 }));
            Assert.That(collectionCounts, Is.EquivalentTo(new List<int> { 2, 1, 3 }));

            this._modelFactory.AssertListsAreEqual(typedList.GetAll(),
                new List<Shipper> {
                    this._modelFactory.CreateInstance(1),
                    this._modelFactory.CreateInstance(2)
                });

            Assert.That(typedSet.GetAll(),
                Is.EquivalentTo(new List<Shipper> {
                    this._modelFactory.CreateInstance(3)
                }));

            this._modelFactory.AssertListsAreEqual(typedSortedSet.GetAll(),
                new List<Shipper> {
                    this._modelFactory.CreateInstance(4),
                    this._modelFactory.CreateInstance(5),
                    this._modelFactory.CreateInstance(6)
                });
        }

    }

}
