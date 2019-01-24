using System;
using System.Collections.Generic;
using System.Linq;
using TheOne.Redis.Client.Internal;
using TheOne.Redis.Common;

namespace TheOne.Redis.Client {

    public partial class RedisClient {

        private const int _firstElement = 0;
        private const int _lastElement = -1;

        /// <inheritdoc />
        public IHasNamed<IRedisList> Lists { get; set; }

        /// <inheritdoc />
        public List<string> GetAllItemsFromList(string listId) {
            var multiDataList = this.LRange(listId, _firstElement, _lastElement);
            return multiDataList.ToStringList();
        }

        /// <inheritdoc />
        public List<string> GetRangeFromList(string listId, int startingFrom, int endingAt) {
            var multiDataList = this.LRange(listId, startingFrom, endingAt);
            return multiDataList.ToStringList();
        }

        /// <inheritdoc />
        public List<string> GetRangeFromSortedList(string listId, int startingFrom, int endingAt) {
            var sortOptions = new SortOptions { Skip = startingFrom, Take = endingAt, SortAlpha = true };
            return this.GetSortedItemsFromList(listId, sortOptions);
        }

        /// <inheritdoc />
        public List<string> GetSortedItemsFromList(string listId, SortOptions sortOptions) {
            var multiDataList = this.Sort(listId, sortOptions);
            return multiDataList.ToStringList();
        }

        /// <inheritdoc />
        public void AddItemToList(string listId, string value) {
            this.RPush(listId, value.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public void AddRangeToList(string listId, List<string> values) {
            var uListId = listId.ToUtf8Bytes();

            var pipeline = this.CreatePipelineCommand();
            foreach (var value in values) {
                pipeline.WriteCommand(Commands.RPush, uListId, value.ToUtf8Bytes());
            }

            pipeline.Flush();

            // the number of items after 
            /* List<long> intResults = */
            pipeline.ReadAllAsInts();
        }

        /// <inheritdoc />
        public void PrependItemToList(string listId, string value) {
            this.LPush(listId, value.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public void PrependRangeToList(string listId, List<string> values) {
            var uListId = listId.ToUtf8Bytes();

            var pipeline = this.CreatePipelineCommand();
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

        /// <inheritdoc />
        public void RemoveAllFromList(string listId) {
            this.LTrim(listId, _lastElement, _firstElement);
        }

        /// <inheritdoc />
        public string RemoveStartFromList(string listId) {
            return this.LPop(listId).FromUtf8Bytes();
        }

        /// <inheritdoc />
        public string BlockingRemoveStartFromList(string listId, TimeSpan? timeout) {
            return this.BLPopValue(listId, (int)timeout.GetValueOrDefault().TotalSeconds).FromUtf8Bytes();

        }

        /// <inheritdoc />
        public ItemRef BlockingRemoveStartFromLists(string[] listIds, TimeSpan? timeout) {
            var value = this.BLPopValue(listIds, (int)timeout.GetValueOrDefault().TotalSeconds);
            if (value == null) {
                return null;
            }

            return new ItemRef { Id = value[0].FromUtf8Bytes(), Item = value[1].FromUtf8Bytes() };
        }

        /// <inheritdoc />
        public string RemoveEndFromList(string listId) {
            return this.RPop(listId).FromUtf8Bytes();
        }

        /// <inheritdoc />
        public void TrimList(string listId, int keepStartingFrom, int keepEndingAt) {
            this.LTrim(listId, keepStartingFrom, keepEndingAt);
        }

        /// <inheritdoc />
        public long RemoveItemFromList(string listId, string value) {
            return this.LRem(listId, 0, value.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public long RemoveItemFromList(string listId, string value, int noOfMatches) {
            return this.LRem(listId, noOfMatches, value.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public long GetListCount(string listId) {
            return this.LLen(listId);
        }

        /// <inheritdoc />
        public string GetItemFromList(string listId, int listIndex) {
            return this.LIndex(listId, listIndex).FromUtf8Bytes();
        }

        /// <inheritdoc />
        public void SetItemInList(string listId, int listIndex, string value) {
            this.LSet(listId, listIndex, value.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public void EnqueueItemOnList(string listId, string value) {
            this.LPush(listId, value.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public string DequeueItemFromList(string listId) {
            return this.RPop(listId).FromUtf8Bytes();
        }

        /// <inheritdoc />
        public string BlockingDequeueItemFromList(string listId, TimeSpan? timeout) {
            return this.BRPopValue(listId, (int)timeout.GetValueOrDefault().TotalSeconds).FromUtf8Bytes();
        }

        /// <inheritdoc />
        public ItemRef BlockingDequeueItemFromLists(string[] listIds, TimeSpan? timeout) {
            var value = this.BRPopValue(listIds, (int)timeout.GetValueOrDefault().TotalSeconds);
            if (value == null) {
                return null;
            }

            return new ItemRef { Id = value[0].FromUtf8Bytes(), Item = value[1].FromUtf8Bytes() };
        }

        /// <inheritdoc />
        public void PushItemToList(string listId, string value) {
            this.RPush(listId, value.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public string PopItemFromList(string listId) {
            return this.RPop(listId).FromUtf8Bytes();
        }

        /// <inheritdoc />
        public string BlockingPopItemFromList(string listId, TimeSpan? timeout) {
            return this.BRPopValue(listId, (int)timeout.GetValueOrDefault().TotalSeconds).FromUtf8Bytes();
        }

        /// <inheritdoc />
        public ItemRef BlockingPopItemFromLists(string[] listIds, TimeSpan? timeout) {
            var value = this.BRPopValue(listIds, (int)timeout.GetValueOrDefault().TotalSeconds);
            if (value == null) {
                return null;
            }

            return new ItemRef { Id = value[0].FromUtf8Bytes(), Item = value[1].FromUtf8Bytes() };
        }

        /// <inheritdoc />
        public string PopAndPushItemBetweenLists(string fromListId, string toListId) {
            return this.RPopLPush(fromListId, toListId).FromUtf8Bytes();
        }

        /// <inheritdoc />
        public string BlockingPopAndPushItemBetweenLists(string fromListId, string toListId, TimeSpan? timeout) {
            return this.BRPopLPush(fromListId, toListId, (int)timeout.GetValueOrDefault().TotalSeconds).FromUtf8Bytes();
        }

        /// <inheritdoc />
        internal class RedisClientLists : IHasNamed<IRedisList> {

            private readonly RedisClient _client;

            /// <inheritdoc />
            public RedisClientLists(RedisClient client) {
                this._client = client;
            }

            /// <inheritdoc />
            public IRedisList this[string listId] {
                get => new RedisClientList(this._client, listId);
                set {
                    var list = this[listId];
                    list.Clear();
                    list.CopyTo(value.ToArray(), 0);
                }
            }

        }

    }

}
