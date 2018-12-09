using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using TheOne.Redis.Client;

namespace TheOne.Redis.Tests.Client {

    [TestFixture]
    internal sealed class RedisClientListTests : RedisClientTestsBase {

        #region Models

        public class Test {

            public string A { get; set; }

        }

        #endregion

        private const string _listId = "rcl_testlist";
        private const string _listId2 = "rcl_testlist2";
        private List<string> _storeMembers;

        private static void AssertAreEqual(List<string> actualList, List<string> expectedList) {
            Assert.That(actualList, Has.Count.EqualTo(expectedList.Count));
            var i = 0;
            actualList.ForEach(x => Assert.That(x, Is.EqualTo(expectedList[i++])));
        }

        public override void SetUp() {
            base.SetUp();
            this._storeMembers = new List<string> { "one", "two", "three", "four" };
        }

        [Test]
        public void BlockingDequeueFromList_Can_Timeout() {
            var item1 = this.Redis.BlockingDequeueItemFromList(_listId, TimeSpan.FromSeconds(1));
            Assert.That(item1, Is.Null);
        }

        [Test]
        public void BlockingPopFromList_Can_Timeout() {
            var item1 = this.Redis.BlockingPopItemFromList(_listId, TimeSpan.FromSeconds(1));
            Assert.That(item1, Is.Null);
        }

        [Test]
        public void Can_Add_to_IList() {
            IRedisList list = this.Redis.Lists[_listId];
            this._storeMembers.ForEach(list.Add);

            List<string> members = list.ToList();
            AssertAreEqual(members, this._storeMembers);
        }

        [Test]
        public void Can_AddRangeToList_and_GetAllFromList() {
            this.Redis.AddRangeToList(_listId, this._storeMembers);

            List<string> members = this.Redis.GetAllItemsFromList(_listId);
            AssertAreEqual(members, this._storeMembers);
        }

        [Test]
        public void Can_AddRangeToList_and_GetSortedItems() {
            this.Redis.PrependRangeToList(_listId, this._storeMembers);

            List<string> members =
                this.Redis.GetSortedItemsFromList(_listId, new SortOptions { SortAlpha = true, SortDesc = true, Skip = 1, Take = 2 });
            AssertAreEqual(members, this._storeMembers.OrderByDescending(s => s).Skip(1).Take(2).ToList());
        }

        [Test]
        public void Can_AddToList_and_GetAllFromList() {
            this._storeMembers.ForEach(x => this.Redis.AddItemToList(_listId, x));

            List<string> members = this.Redis.GetAllItemsFromList(_listId);

            AssertAreEqual(members, this._storeMembers);
        }

        [Test]
        public void Can_BlockingDequeueFromList() {
            var queue = new Queue<string>();
            this._storeMembers.ForEach(queue.Enqueue);
            this._storeMembers.ForEach(x => this.Redis.EnqueueItemOnList(_listId, x));

            var item1 = this.Redis.BlockingDequeueItemFromList(_listId, null);

            Assert.That(item1, Is.EqualTo(queue.Dequeue()));
        }

        [Test]
        public void Can_BlockingPopAndPushItemBetweenLists() {
            this.Redis.AddItemToList(_listId, "A");
            this.Redis.AddItemToList(_listId, "B");
            var r = this.Redis.BlockingPopAndPushItemBetweenLists(_listId, _listId2, new TimeSpan(0, 0, 1));

            Assert.That(r, Is.EqualTo("B"));
        }

        [Test]
        public void Can_BlockingPopFromList() {
            var stack = new Stack<string>();
            this._storeMembers.ForEach(stack.Push);
            this._storeMembers.ForEach(x => this.Redis.PushItemToList(_listId, x));

            var item1 = this.Redis.BlockingPopItemFromList(_listId, null);

            Assert.That(item1, Is.EqualTo(stack.Pop()));
        }

        [Test]
        public void Can_BlockingRemoveStartFromList() {
            this._storeMembers.ForEach(x => this.Redis.AddItemToList(_listId, x));

            var item1 = this.Redis.BlockingRemoveStartFromList(_listId, null);

            Assert.That(item1, Is.EqualTo(this._storeMembers.First()));
        }

        [Test]
        public void Can_Clear_IList() {
            IRedisList list = this.Redis.Lists[_listId];
            this._storeMembers.ForEach(list.Add);

            Assert.That(list.Count, Is.EqualTo(this._storeMembers.Count));

            list.Clear();

            Assert.That(list.Count, Is.EqualTo(0));
        }

        [Test]
        public void Can_DequeueFromList() {
            var queue = new Queue<string>();
            this._storeMembers.ForEach(queue.Enqueue);
            this._storeMembers.ForEach(x => this.Redis.EnqueueItemOnList(_listId, x));

            var item1 = this.Redis.DequeueItemFromList(_listId);

            Assert.That(item1, Is.EqualTo(queue.Dequeue()));
        }

        [Test]
        public void Can_EnqueueOnList() {
            var queue = new Queue<string>();
            this._storeMembers.ForEach(queue.Enqueue);
            this._storeMembers.ForEach(x => this.Redis.EnqueueItemOnList(_listId, x));

            while (queue.Count > 0) {
                var actual = this.Redis.DequeueItemFromList(_listId);
                Assert.That(actual, Is.EqualTo(queue.Dequeue()));
            }
        }

        [Test]
        public void Can_enumerate_large_list() {
            const int listSize = 2500;

            this._storeMembers = new List<string>();
            for (var i = 0; i < listSize; i++) {
                this.Redis.AddItemToList(_listId, i.ToString());
                this._storeMembers.Add(i.ToString());
            }

            var members = new List<string>();
            foreach (var item in this.Redis.Lists[_listId]) {
                members.Add(item);
            }

            members.Sort((x, y) => int.Parse(x).CompareTo(int.Parse(y)));
            Assert.That(members.Count, Is.EqualTo(this._storeMembers.Count));
            Assert.That(members, Is.EquivalentTo(this._storeMembers));
        }


        [Test]
        public void Can_enumerate_small_list() {
            this._storeMembers.ForEach(x => this.Redis.AddItemToList(_listId, x));

            var readMembers = new List<string>();
            foreach (var item in this.Redis.Lists[_listId]) {
                readMembers.Add(item);
            }

            AssertAreEqual(readMembers, this._storeMembers);
        }

        [Test]
        public void Can_get_default_index_from_IList() {
            IRedisList list = this.Redis.Lists[_listId];
            this._storeMembers.ForEach(list.Add);

            for (var i = 0; i < this._storeMembers.Count; i++) {
                Assert.That(list[i], Is.EqualTo(this._storeMembers[i]));
            }
        }

        [Test]
        public void Can_GetItemFromList() {
            this._storeMembers.ForEach(x => this.Redis.AddItemToList(_listId, x));

            var storeMember3 = this._storeMembers[2];
            var item3 = this.Redis.GetItemFromList(_listId, 2);

            Assert.That(item3, Is.EqualTo(storeMember3));
        }

        [Test]
        public void Can_GetListCount() {
            this._storeMembers.ForEach(x => this.Redis.AddItemToList(_listId, x));

            var listCount = this.Redis.GetListCount(_listId);

            Assert.That(listCount, Is.EqualTo(this._storeMembers.Count));
        }

        [Test]
        public void Can_MoveBetweenLists() {
            var list1Members = new List<string> { "one", "two", "three", "four" };
            var list2Members = new List<string> { "five", "six", "seven" };
            const string item4 = "four";

            list1Members.ForEach(x => this.Redis.AddItemToList(_listId, x));
            list2Members.ForEach(x => this.Redis.AddItemToList(_listId2, x));

            list1Members.Remove(item4);
            list2Members.Insert(0, item4);
            this.Redis.PopAndPushItemBetweenLists(_listId, _listId2);

            List<string> readList1 = this.Redis.GetAllItemsFromList(_listId);
            List<string> readList2 = this.Redis.GetAllItemsFromList(_listId2);

            AssertAreEqual(readList1, list1Members);
            AssertAreEqual(readList2, list2Members);
        }

        [Test]
        public void Can_PopAndPushItemBetweenLists() {
            this.Redis.AddItemToList(_listId, "1");
            this.Redis.PopAndPushItemBetweenLists(_listId, _listId2);
        }

        [Test]
        public void Can_PopFromList() {
            this._storeMembers.ForEach(x => this.Redis.AddItemToList(_listId, x));

            var item4 = this.Redis.PopItemFromList(_listId);

            Assert.That(item4, Is.EqualTo("four"));
        }

        [Test]
        public void Can_PrependRangeToList_and_GetAllFromList() {
            this.Redis.PrependRangeToList(_listId, this._storeMembers);

            List<string> members = this.Redis.GetAllItemsFromList(_listId);
            AssertAreEqual(members, this._storeMembers);
        }

        [Test]
        public void Can_PushToList() {
            var stack = new Stack<string>();
            this._storeMembers.ForEach(stack.Push);
            this._storeMembers.ForEach(x => this.Redis.PushItemToList(_listId, x));

            while (stack.Count > 0) {
                var actual = this.Redis.PopItemFromList(_listId);
                Assert.That(actual, Is.EqualTo(stack.Pop()));
            }
        }

        [Test]
        public void Can_Remove_value_from_IList() {
            IRedisList list = this.Redis.Lists[_listId];
            this._storeMembers.ForEach(list.Add);

            this._storeMembers.Remove("two");
            list.Remove("two");

            List<string> members = list.ToList();

            AssertAreEqual(members, this._storeMembers);
        }

        [Test]
        public void Can_RemoveAt_value_from_IList() {
            IRedisList list = this.Redis.Lists[_listId];
            this._storeMembers.ForEach(list.Add);

            this._storeMembers.RemoveAt(2);
            list.RemoveAt(2);

            List<string> members = list.ToList();

            AssertAreEqual(members, this._storeMembers);
        }

        [Test]
        public void Can_RemoveEndFromList() {
            this._storeMembers.ForEach(x => this.Redis.AddItemToList(_listId, x));

            var item1 = this.Redis.RemoveEndFromList(_listId);

            Assert.That(item1, Is.EqualTo(this._storeMembers.Last()));
        }

        [Test]
        public void Can_RemoveStartFromList() {
            this._storeMembers.ForEach(x => this.Redis.AddItemToList(_listId, x));

            var item1 = this.Redis.RemoveStartFromList(_listId);

            Assert.That(item1, Is.EqualTo(this._storeMembers.First()));
        }

        [Test]
        public void Can_SetItemInList() {
            this._storeMembers.ForEach(x => this.Redis.AddItemToList(_listId, x));

            this._storeMembers[2] = "five";
            this.Redis.SetItemInList(_listId, 2, "five");

            List<string> members = this.Redis.GetAllItemsFromList(_listId);
            AssertAreEqual(members, this._storeMembers);
        }

        [Test]
        public void Can_Test_Contains_in_IList() {
            IRedisList list = this.Redis.Lists[_listId];
            this._storeMembers.ForEach(list.Add);

            Assert.That(list.Contains("two"), Is.True);
            Assert.That(list.Contains("five"), Is.False);
        }

        [Test]
        public void Can_test_for_IndexOf_in_IList() {
            IRedisList list = this.Redis.Lists[_listId];
            this._storeMembers.ForEach(list.Add);

            foreach (var item in this._storeMembers) {
                Assert.That(list.IndexOf(item), Is.EqualTo(this._storeMembers.IndexOf(item)));
            }
        }

        [Test]
        public void Can_Timeout_BlockingPopAndPushItemBetweenLists() {
            var r = this.Redis.BlockingPopAndPushItemBetweenLists(_listId, _listId2, new TimeSpan(0, 0, 1));
            Assert.That(r, Is.Null);
        }

        [Test]
        public void PopAndPushSameAsDequeue() {
            var queue = new Queue<string>();
            this._storeMembers.ForEach(queue.Enqueue);
            this._storeMembers.ForEach(x => this.Redis.EnqueueItemOnList(_listId, x));

            var item1 = this.Redis.PopAndPushItemBetweenLists(_listId, _listId2);
            Assert.That(item1, Is.EqualTo(queue.Dequeue()));
        }

        [Test]
        public void RemoveAll_removes_all_items_from_Named_List() {
            IRedisTypedClient<Test> redis = this.Redis.As<Test>();

            IRedisList<Test> clientRepos = redis.Lists["repo:Client:Test"];

            Assert.IsTrue(clientRepos.Count == 0, "Count 1 = " + clientRepos.Count);
            clientRepos.Add(new Test { A = "Test" });
            Assert.IsTrue(clientRepos.Count == 1, "Count 2 = " + clientRepos.Count);
            clientRepos.RemoveAll();
            Assert.IsTrue(clientRepos.Count == 0, "Count 3 = " + clientRepos.Count);
        }

    }

}
