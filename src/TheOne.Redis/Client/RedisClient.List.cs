using System;
using System.Collections.Generic;
using System.Linq;
using TheOne.Redis.Client.Internal;
using TheOne.Redis.Common;
using TheOne.Redis.Pipeline;

namespace TheOne.Redis.Client {

    public partial class RedisClient {

        private const int _firstElement = 0;
        private const int _lastElement = -1;

        public IHasNamed<IRedisList> Lists { get; set; }

        public List<string> GetAllItemsFromList(string listId) {
            byte[][] multiDataList = this.LRange(listId, _firstElement, _lastElement);
            return multiDataList.ToStringList();
        }

        public List<string> GetRangeFromList(string listId, int startingFrom, int endingAt) {
            byte[][] multiDataList = this.LRange(listId, startingFrom, endingAt);
            return multiDataList.ToStringList();
        }

        public List<string> GetRangeFromSortedList(string listId, int startingFrom, int endingAt) {
            var sortOptions = new SortOptions { Skip = startingFrom, Take = endingAt, SortAlpha = true };
            return this.GetSortedItemsFromList(listId, sortOptions);
        }

        public List<string> GetSortedItemsFromList(string listId, SortOptions sortOptions) {
            byte[][] multiDataList = this.Sort(listId, sortOptions);
            return multiDataList.ToStringList();
        }

        public void AddItemToList(string listId, string value) {
            this.RPush(listId, value.ToUtf8Bytes());
        }

        public void AddRangeToList(string listId, List<string> values) {
            byte[] uListId = listId.ToUtf8Bytes();

            RedisPipelineCommand pipeline = this.CreatePipelineCommand();
            foreach (var value in values) {
                pipeline.WriteCommand(Commands.RPush, uListId, value.ToUtf8Bytes());
            }

            pipeline.Flush();

            // the number of items after 
            /* List<long> intResults = */
            pipeline.ReadAllAsInts();
        }

        public void PrependItemToList(string listId, string value) {
            this.LPush(listId, value.ToUtf8Bytes());
        }

        public void PrependRangeToList(string listId, List<string> values) {
            byte[] uListId = listId.ToUtf8Bytes();

            RedisPipelineCommand pipeline = this.CreatePipelineCommand();
            // ensure list[0] == value[0] after batch operation
            for (var i = values.Count - 1; i >= 0; i--) {
                var value = values[i];
                pipeline.WriteCommand(Commands.LPush, uListId, value.ToUtf8Bytes());
            }

            pipeline.Flush();

            // the number of items after 
            /* List<long> intResults = */
            pipeline.ReadAllAsInts();
        }

        public void RemoveAllFromList(string listId) {
            this.LTrim(listId, _lastElement, _firstElement);
        }

        public string RemoveStartFromList(string listId) {
            return this.LPop(listId).FromUtf8Bytes();
        }

        public string BlockingRemoveStartFromList(string listId, TimeSpan? timeout) {
            return this.BLPopValue(listId, (int)timeout.GetValueOrDefault().TotalSeconds).FromUtf8Bytes();

        }

        public ItemRef BlockingRemoveStartFromLists(string[] listIds, TimeSpan? timeout) {
            byte[][] value = this.BLPopValue(listIds, (int)timeout.GetValueOrDefault().TotalSeconds);
            if (value == null) {
                return null;
            }

            return new ItemRef { Id = value[0].FromUtf8Bytes(), Item = value[1].FromUtf8Bytes() };
        }

        public string RemoveEndFromList(string listId) {
            return this.RPop(listId).FromUtf8Bytes();
        }

        public void TrimList(string listId, int keepStartingFrom, int keepEndingAt) {
            this.LTrim(listId, keepStartingFrom, keepEndingAt);
        }

        public long RemoveItemFromList(string listId, string value) {
            return this.LRem(listId, 0, value.ToUtf8Bytes());
        }

        public long RemoveItemFromList(string listId, string value, int noOfMatches) {
            return this.LRem(listId, noOfMatches, value.ToUtf8Bytes());
        }

        public long GetListCount(string listId) {
            return this.LLen(listId);
        }

        public string GetItemFromList(string listId, int listIndex) {
            return this.LIndex(listId, listIndex).FromUtf8Bytes();
        }

        public void SetItemInList(string listId, int listIndex, string value) {
            this.LSet(listId, listIndex, value.ToUtf8Bytes());
        }

        public void EnqueueItemOnList(string listId, string value) {
            this.LPush(listId, value.ToUtf8Bytes());
        }

        public string DequeueItemFromList(string listId) {
            return this.RPop(listId).FromUtf8Bytes();
        }

        public string BlockingDequeueItemFromList(string listId, TimeSpan? timeout) {
            return this.BRPopValue(listId, (int)timeout.GetValueOrDefault().TotalSeconds).FromUtf8Bytes();
        }

        public ItemRef BlockingDequeueItemFromLists(string[] listIds, TimeSpan? timeout) {
            byte[][] value = this.BRPopValue(listIds, (int)timeout.GetValueOrDefault().TotalSeconds);
            if (value == null) {
                return null;
            }

            return new ItemRef { Id = value[0].FromUtf8Bytes(), Item = value[1].FromUtf8Bytes() };
        }

        public void PushItemToList(string listId, string value) {
            this.RPush(listId, value.ToUtf8Bytes());
        }

        public string PopItemFromList(string listId) {
            return this.RPop(listId).FromUtf8Bytes();
        }

        public string BlockingPopItemFromList(string listId, TimeSpan? timeout) {
            return this.BRPopValue(listId, (int)timeout.GetValueOrDefault().TotalSeconds).FromUtf8Bytes();
        }

        public ItemRef BlockingPopItemFromLists(string[] listIds, TimeSpan? timeout) {
            byte[][] value = this.BRPopValue(listIds, (int)timeout.GetValueOrDefault().TotalSeconds);
            if (value == null) {
                return null;
            }

            return new ItemRef { Id = value[0].FromUtf8Bytes(), Item = value[1].FromUtf8Bytes() };
        }

        public string PopAndPushItemBetweenLists(string fromListId, string toListId) {
            return this.RPopLPush(fromListId, toListId).FromUtf8Bytes();
        }

        public string BlockingPopAndPushItemBetweenLists(string fromListId, string toListId, TimeSpan? timeout) {
            return this.BRPopLPush(fromListId, toListId, (int)timeout.GetValueOrDefault().TotalSeconds).FromUtf8Bytes();
        }

        internal class RedisClientLists : IHasNamed<IRedisList> {

            private readonly RedisClient _client;

            public RedisClientLists(RedisClient client) {
                this._client = client;
            }

            public IRedisList this[string listId] {
                get => new RedisClientList(this._client, listId);
                set {
                    IRedisList list = this[listId];
                    list.Clear();
                    list.CopyTo(value.ToArray(), 0);
                }
            }

        }

    }

}
