using System;
using System.Collections.Generic;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Pipeline;
using TheOne.Redis.Tests.Shared;

namespace TheOne.Redis.Tests.Client {

    [TestFixture]
    internal sealed class RedisTypedTransactionTests : RedisClientTestsBase {

        private const string _key = "multitest";
        private const string _listKey = "multitest-list";
        private const string _setKey = "multitest-set";
        private const string _sortedSetKey = "multitest-sortedset";

        private readonly ShipperFactory _modelFactory = new ShipperFactory();
        private Shipper _model;
        private IRedisTypedClient<Shipper> _typedClient;

        public override void SetUp() {
            base.SetUp();

            this._typedClient = this.Redis.As<Shipper>();
            this._model = this._modelFactory.CreateInstance(1);
        }

        [Test]
        public void Can_call_multi_string_operations_in_transaction() {
            Shipper item1 = null;
            Shipper item4 = null;

            var results = new List<Shipper>();

            IRedisList<Shipper> typedList = this._typedClient.Lists[_listKey];
            Assert.That(typedList.Count, Is.EqualTo(0));

            using (IRedisTypedTransaction<Shipper> trans = this._typedClient.CreateTransaction()) {
                trans.QueueCommand(r => r.AddItemToList(typedList, this._modelFactory.CreateInstance(1)));
                trans.QueueCommand(r => r.AddItemToList(typedList, this._modelFactory.CreateInstance(2)));
                trans.QueueCommand(r => r.AddItemToList(typedList, this._modelFactory.CreateInstance(3)));
                trans.QueueCommand(r => r.GetAllItemsFromList(typedList), x => results = x);
                trans.QueueCommand(r => r.GetItemFromList(typedList, 0), x => item1 = x);
                trans.QueueCommand(r => r.GetItemFromList(typedList, 4), x => item4 = x);

                trans.Commit();
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
        // Operations that are not supported in older versions will look at server info to determine what to do.
        // If server info is fetched each time, then it will interfere with transaction
        public void Can_call_operation_not_supported_on_older_servers_in_transaction() {
            var temp = new byte[1];
            using (IRedisTransaction trans = this.Redis.CreateTransaction()) {
                trans.QueueCommand(r => ((RedisNativeClient)r).SetEx("key", 5, temp));
                trans.Commit();
            }
        }

        [Test]
        public void Can_call_single_operation_3_Times_in_transaction() {
            IRedisList<Shipper> typedList = this._typedClient.Lists[_listKey];
            Assert.That(typedList.Count, Is.EqualTo(0));

            using (IRedisTypedTransaction<Shipper> trans = this._typedClient.CreateTransaction()) {
                trans.QueueCommand(r => r.AddItemToList(typedList, this._modelFactory.CreateInstance(1)));
                trans.QueueCommand(r => r.AddItemToList(typedList, this._modelFactory.CreateInstance(2)));
                trans.QueueCommand(r => r.AddItemToList(typedList, this._modelFactory.CreateInstance(3)));

                trans.Commit();
            }

            Assert.That(typedList.Count, Is.EqualTo(3));
        }

        [Test]
        public void Can_call_single_operation_in_transaction() {
            Assert.That(this._typedClient.GetValue(_key), Is.Null);

            using (IRedisTypedTransaction<Shipper> trans = this._typedClient.CreateTransaction()) {
                trans.QueueCommand(r => r.SetValue(_key, this._model));

                trans.Commit();
            }

            this._modelFactory.AssertIsEqual(this._typedClient.GetValue(_key), this._model);
        }

        [Test]
        public void Can_call_single_operation_with_callback_3_Times_in_transaction() {
            var results = new List<int>();

            IRedisList<Shipper> typedList = this._typedClient.Lists[_listKey];
            Assert.That(typedList.Count, Is.EqualTo(0));

            using (IRedisTypedTransaction<Shipper> trans = this._typedClient.CreateTransaction()) {
                trans.QueueCommand(r => r.AddItemToList(typedList, this._modelFactory.CreateInstance(1)), () => results.Add(1));
                trans.QueueCommand(r => r.AddItemToList(typedList, this._modelFactory.CreateInstance(2)), () => results.Add(2));
                trans.QueueCommand(r => r.AddItemToList(typedList, this._modelFactory.CreateInstance(3)), () => results.Add(3));

                trans.Commit();
            }

            Assert.That(typedList.Count, Is.EqualTo(3));
            Assert.That(results, Is.EquivalentTo(new List<int> { 1, 2, 3 }));
        }

        [Test]
        public void Exception_in_atomic_transactions_discards_all_commands() {
            Assert.That(this._typedClient.GetValue(_key), Is.Null);
            try {
                using (IRedisTypedTransaction<Shipper> trans = this._typedClient.CreateTransaction()) {
                    trans.QueueCommand(r => r.SetValue(_key, this._model));
                    throw new NotSupportedException();
                }
            } catch (NotSupportedException) {
                Assert.That(this._typedClient.GetValue(_key), Is.Null);
            }
        }

        [Test]
        public void No_commit_of_atomic_transactions_discards_all_commands() {
            Assert.That(this._typedClient.GetValue(_key), Is.Null);

            using (IRedisTypedTransaction<Shipper> trans = this._typedClient.CreateTransaction()) {
                trans.QueueCommand(r => r.SetValue(_key, this._model));
            }

            Assert.That(this._typedClient.GetValue(_key), Is.Null);
        }

        [Test]
        public void Supports_different_operation_types_in_same_transaction() {
            var incrementResults = new List<long>();
            var collectionCounts = new List<long>();
            var containsItem = false;

            IRedisList<Shipper> typedList = this._typedClient.Lists[_listKey];
            IRedisSet<Shipper> typedSet = this._typedClient.Sets[_setKey];
            IRedisSortedSet<Shipper> typedSortedSet = this._typedClient.SortedSets[_sortedSetKey];

            Assert.That(this._typedClient.GetValue(_key), Is.Null);
            using (IRedisTypedTransaction<Shipper> trans = this._typedClient.CreateTransaction()) {
                trans.QueueCommand(r => r.IncrementValue(_key), intResult => incrementResults.Add(intResult));
                trans.QueueCommand(r => r.AddItemToList(typedList, this._modelFactory.CreateInstance(1)));
                trans.QueueCommand(r => r.AddItemToList(typedList, this._modelFactory.CreateInstance(2)));
                trans.QueueCommand(r => r.AddItemToSet(typedSet, this._modelFactory.CreateInstance(3)));
                trans.QueueCommand(r => r.SetContainsItem(typedSet, this._modelFactory.CreateInstance(3)), b => containsItem = b);
                trans.QueueCommand(r => r.AddItemToSortedSet(typedSortedSet, this._modelFactory.CreateInstance(4)));
                trans.QueueCommand(r => r.AddItemToSortedSet(typedSortedSet, this._modelFactory.CreateInstance(5)));
                trans.QueueCommand(r => r.AddItemToSortedSet(typedSortedSet, this._modelFactory.CreateInstance(6)));
                trans.QueueCommand(r => r.GetListCount(typedList), intResult => collectionCounts.Add(intResult));
                trans.QueueCommand(r => r.GetSetCount(typedSet), intResult => collectionCounts.Add(intResult));
                trans.QueueCommand(r => r.GetSortedSetCount(typedSortedSet), intResult => collectionCounts.Add(intResult));
                trans.QueueCommand(r => r.IncrementValue(_key), intResult => incrementResults.Add(intResult));

                trans.Commit();
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


        [Test]
        public void Transaction_can_be_replayed() {
            var keySquared = _key + _key;
            Assert.That(this.Redis.GetValue(_key), Is.Null);
            Assert.That(this.Redis.GetValue(keySquared), Is.Null);
            using (IRedisTransaction trans = this.Redis.CreateTransaction()) {
                trans.QueueCommand(r => r.IncrementValue(_key));
                trans.QueueCommand(r => r.IncrementValue(keySquared));
                trans.Commit();

                Assert.That(this.Redis.GetValue(_key), Is.EqualTo("1"));
                Assert.That(this.Redis.GetValue(keySquared), Is.EqualTo("1"));
                this.Redis.Del(_key);
                this.Redis.Del(keySquared);
                Assert.That(this.Redis.GetValue(_key), Is.Null);
                Assert.That(this.Redis.GetValue(keySquared), Is.Null);

                trans.Replay();
                trans.Dispose();
                Assert.That(this.Redis.GetValue(_key), Is.EqualTo("1"));
                Assert.That(this.Redis.GetValue(keySquared), Is.EqualTo("1"));
            }
        }

    }

}
