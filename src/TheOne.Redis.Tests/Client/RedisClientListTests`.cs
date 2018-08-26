using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Tests.Shared;

namespace TheOne.Redis.Tests.Client {

    [TestFixture(typeof(CustomType), typeof(CustomTypeFactory))]
    [TestFixture(typeof(DateTime), typeof(DateTimeFactory))]
    [TestFixture(typeof(int), typeof(IntFactory))]
    [TestFixture(typeof(ModelWithFieldsOfDifferentTypes), typeof(ModelWithFieldsOfDifferentTypesFactory))]
    [TestFixture(typeof(Shipper), typeof(ShipperFactory))]
    [TestFixture(typeof(string), typeof(BuiltInsFactory))]
    internal sealed class RedisClientListTests<T, TFactory> : RedisClientTestsBase where TFactory : class, IModelFactory<T>, new() {

        private const string _listId = "testlist";
        private const string _listId2 = "testlist2";
        private readonly IModelFactory<T> _factory = new TFactory();
        private IRedisList<T> _list;
        private IRedisList<T> _list2;
        private IRedisTypedClient<T> _redis;

        [SetUp]
        public override void SetUp() {
            base.SetUp();
            this._redis = this.Redis.As<T>();
            this._list = this._redis.Lists[_listId];
            this._list2 = this._redis.Lists[_listId2];
        }

        [Test]
        public void Can_Add_to_IList() {
            List<T> storeMembers = this._factory.CreateList();
            IRedisList<T> list = this._redis.Lists[_listId];
            storeMembers.ForEach(list.Add);

            List<T> members = list.ToList();
            this._factory.AssertListsAreEqual(members, storeMembers);
        }

        [Test]
        public void Can_AddToList_and_GetAllFromList() {
            List<T> storeMembers = this._factory.CreateList();
            storeMembers.ForEach(x => this._redis.AddItemToList(this._list, x));

            List<T> members = this._redis.GetAllItemsFromList(this._list);

            this._factory.AssertListsAreEqual(members, storeMembers);
        }

        [Test]
        public void Can_BlockingDequeueItemFromList() {
            List<T> storeMembers = this._factory.CreateList();
            storeMembers.ForEach(x => this._redis.EnqueueItemOnList(this._list, x));

            T item1 = this._redis.BlockingDequeueItemFromList(this._list, new TimeSpan(0, 0, 1));

            this._factory.AssertIsEqual(item1, storeMembers.First());
        }

        [Test]
        public void Can_BlockingDequeueItemFromList_Timeout() {
            T item1 = this._redis.BlockingDequeueItemFromList(this._list, new TimeSpan(0, 0, 1));
            Assert.AreEqual(item1, default(T));
        }

        [Test]
        public void Can_Clear_IList() {
            List<T> storeMembers = this._factory.CreateList();
            storeMembers.ForEach(this._list.Add);

            Assert.That(this._list.Count, Is.EqualTo(storeMembers.Count));

            this._list.Clear();

            Assert.That(this._list.Count, Is.EqualTo(0));
        }

        [Test]
        public void Can_ClearList() {
            List<T> storeMembers = this._factory.CreateList();
            storeMembers.ForEach(x => this._redis.EnqueueItemOnList(this._list, x));

            var count = this._redis.GetAllItemsFromList(this._list).Count;
            Assert.That(count, Is.EqualTo(storeMembers.Count));

            this._redis.RemoveAllFromList(this._list);
            count = this._redis.GetAllItemsFromList(this._list).Count;
            Assert.That(count, Is.EqualTo(0));

        }

        [Test]
        public void Can_ClearListWithOneItem() {
            List<T> storeMembers = this._factory.CreateList();
            this._redis.EnqueueItemOnList(this._list, storeMembers[0]);

            var count = this._redis.GetAllItemsFromList(this._list).Count;
            Assert.That(count, Is.EqualTo(1));

            this._redis.RemoveAllFromList(this._list);
            count = this._redis.GetAllItemsFromList(this._list).Count;
            Assert.That(count, Is.EqualTo(0));
        }

        [Test]
        public void Can_DequeueFromList() {

            var queue = new Queue<T>();
            List<T> storeMembers = this._factory.CreateList();
            storeMembers.ForEach(x => queue.Enqueue(x));
            storeMembers.ForEach(x => this._redis.EnqueueItemOnList(this._list, x));

            T item1 = this._redis.DequeueItemFromList(this._list);

            this._factory.AssertIsEqual(item1, queue.Dequeue());
        }

        [Test]
        public void Can_enumerate_large_list() {

            const int listSize = 2500;

            for (var j = 0; j < listSize; j++) {
                this._redis.AddItemToList(this._list, this._factory.CreateInstance(j));
            }

            var i = 0;
            foreach (T item in this._list) {
                this._factory.AssertIsEqual(item, this._factory.CreateInstance(i++));
            }
        }


        [Test]
        public void Can_enumerate_small_list() {
            List<T> storeMembers = this._factory.CreateList();
            storeMembers.ForEach(x => this._redis.AddItemToList(this._list, x));

            var readMembers = new List<T>();
            foreach (T item in this._redis.Lists[_listId]) {
                readMembers.Add(item);
            }

            this._factory.AssertListsAreEqual(readMembers, storeMembers);
        }

        [Test]
        public void Can_get_default_index_from_IList() {
            List<T> storeMembers = this._factory.CreateList();
            storeMembers.ForEach(this._list.Add);

            for (var i = 0; i < storeMembers.Count; i++) {
                this._factory.AssertIsEqual(this._list[i], storeMembers[i]);
            }
        }

        [Test]
        public void Can_GetItemFromList() {
            List<T> storeMembers = this._factory.CreateList();
            storeMembers.ForEach(x => this._redis.AddItemToList(this._list, x));

            T storeMember3 = storeMembers[2];
            T item3 = this._redis.GetItemFromList(this._list, 2);

            this._factory.AssertIsEqual(item3, storeMember3);
        }

        [Test]
        public void Can_GetListCount() {
            List<T> storeMembers = this._factory.CreateList();
            storeMembers.ForEach(x => this._redis.AddItemToList(this._list, x));

            var listCount = this._redis.GetListCount(this._list);

            Assert.That(listCount, Is.EqualTo(storeMembers.Count));
        }


        [Test]
        public void Can_GetRangeFromList() {
            List<T> storeMembers = this._factory.CreateList();
            storeMembers.ForEach(x => this._redis.AddItemToList(this._list, x));

            // in SetUp(): List = redis.Lists["testlist"];
            // alias for: redis.GetRangeFromList(redis.Lists["testlist"], 1, 3);
            List<T> range = this._list.GetRange(1, 3);
            List<T> expected = storeMembers.Skip(1).Take(3).ToList();

            this._factory.AssertListsAreEqual(range, expected);
        }

        [Test]
        public void Can_MoveBetweenLists() {
            List<T> list1Members = this._factory.CreateList();
            List<T> list2Members = this._factory.CreateList2();
            T lastItem = list1Members[list1Members.Count - 1];

            list1Members.ForEach(x => this._redis.AddItemToList(this._list, x));
            list2Members.ForEach(x => this._redis.AddItemToList(this._list2, x));

            list1Members.Remove(lastItem);
            list2Members.Insert(0, lastItem);
            this._redis.PopAndPushItemBetweenLists(this._list, this._list2);

            List<T> readList1 = this._redis.GetAllItemsFromList(this._list);
            List<T> readList2 = this._redis.GetAllItemsFromList(this._list2);

            this._factory.AssertListsAreEqual(readList1, list1Members);
            this._factory.AssertListsAreEqual(readList2, list2Members);
        }

        [Test]
        public void Can_PopFromList() {
            List<T> storeMembers = this._factory.CreateList();
            storeMembers.ForEach(x => this._redis.AddItemToList(this._list, x));

            T lastValue = this._redis.PopItemFromList(this._list);

            this._factory.AssertIsEqual(lastValue, storeMembers[storeMembers.Count - 1]);
        }

        [Test]
        public void Can_Remove_value_from_IList() {
            List<T> storeMembers = this._factory.CreateList();
            storeMembers.ForEach(this._list.Add);

            storeMembers.Remove(this._factory.ExistingValue);
            this._list.Remove(this._factory.ExistingValue);

            List<T> members = this._list.ToList();

            this._factory.AssertListsAreEqual(members, storeMembers);
        }

        [Test]
        public void Can_Remove_value_from_IList2() {
            List<T> storeMembers = this._factory.CreateList();
            storeMembers.ForEach(this._list.Add);

            T equalItem = this._factory.ExistingValue;
            storeMembers.Remove(equalItem);
            this._list.Remove(equalItem);

            List<T> members = this._list.ToList();

            this._factory.AssertListsAreEqual(members, storeMembers);
        }

        [Test]
        public void Can_RemoveAt_value_from_IList() {
            List<T> storeMembers = this._factory.CreateList();
            storeMembers.ForEach(this._list.Add);

            storeMembers.RemoveAt(2);
            this._list.RemoveAt(2);

            List<T> members = this._list.ToList();

            this._factory.AssertListsAreEqual(members, storeMembers);
        }

        [Test]
        public void Can_SetItemInList() {
            List<T> storeMembers = this._factory.CreateList();
            storeMembers.ForEach(x => this._redis.AddItemToList(this._list, x));

            storeMembers[2] = this._factory.NonExistingValue;
            this._redis.SetItemInList(this._list, 2, this._factory.NonExistingValue);

            List<T> members = this._redis.GetAllItemsFromList(this._list);

            this._factory.AssertListsAreEqual(members, storeMembers);
        }

        [Test]
        public void Can_Test_Contains_in_IList() {
            List<T> storeMembers = this._factory.CreateList();
            storeMembers.ForEach(this._list.Add);

            Assert.That(this._list.Contains(this._factory.ExistingValue), Is.True);
            Assert.That(this._list.Contains(this._factory.NonExistingValue), Is.False);
        }

        [Test]
        public void Can_test_for_IndexOf_in_IList() {
            List<T> storeMembers = this._factory.CreateList();
            storeMembers.ForEach(this._list.Add);

            foreach (T item in storeMembers) {
                Assert.That(this._list.IndexOf(item), Is.EqualTo(storeMembers.IndexOf(item)));
            }
        }

        [Test]
        public void PopAndPushSameAsDequeue() {
            var queue = new Queue<T>();
            List<T> storeMembers = this._factory.CreateList();
            storeMembers.ForEach(x => queue.Enqueue(x));
            storeMembers.ForEach(x => this._redis.EnqueueItemOnList(this._list, x));

            T item1 = this._redis.PopAndPushItemBetweenLists(this._list, this._list2);
            Assert.That(item1, Is.EqualTo(queue.Dequeue()));
        }

    }

}
