using System;
using System.Collections.Generic;
using System.Linq;
using TheOne.Redis.Client.Internal;

namespace TheOne.Redis.Client {

    public partial class RedisTypedClient<T> {

        private const int _firstElement = 0;
        private const int _lastElement = -1;

        /// <inheritdoc />
        public IHasNamed<IRedisList<T>> Lists { get; set; }

        /// <inheritdoc />
        public List<T> GetAllItemsFromList(IRedisList<T> fromList) {
            byte[][] multiDataList = this._client.LRange(fromList.Id, _firstElement, _lastElement);
            return this.CreateList(multiDataList);
        }

        /// <inheritdoc />
        public List<T> GetRangeFromList(IRedisList<T> fromList, int startingFrom, int endingAt) {
            byte[][] multiDataList = this._client.LRange(fromList.Id, startingFrom, endingAt);
            return this.CreateList(multiDataList);
        }

        /// <inheritdoc />
        public List<T> SortList(IRedisList<T> fromList, int startingFrom, int endingAt) {
            var sortOptions = new SortOptions { Skip = startingFrom, Take = endingAt };
            byte[][] multiDataList = this._client.Sort(fromList.Id, sortOptions);
            return this.CreateList(multiDataList);
        }

        /// <inheritdoc />
        public void AddItemToList(IRedisList<T> fromList, T value) {
            this._client.RPush(fromList.Id, this.SerializeValue(value));
        }

        /// <inheritdoc />
        public void PrependItemToList(IRedisList<T> fromList, T value) {
            this._client.LPush(fromList.Id, this.SerializeValue(value));
        }

        /// <inheritdoc />
        public T RemoveStartFromList(IRedisList<T> fromList) {
            return this.DeserializeValue(this._client.LPop(fromList.Id));
        }

        /// <inheritdoc />
        public T BlockingRemoveStartFromList(IRedisList<T> fromList, TimeSpan? timeout) {
            byte[][] unblockingKeyAndValue = this._client.BLPop(fromList.Id, (int)timeout.GetValueOrDefault().TotalSeconds);
            return unblockingKeyAndValue.Length == 0
                ? default
                : this.DeserializeValue(unblockingKeyAndValue[1]);
        }

        /// <inheritdoc />
        public T RemoveEndFromList(IRedisList<T> fromList) {
            return this.DeserializeValue(this._client.RPop(fromList.Id));
        }

        /// <inheritdoc />
        public void RemoveAllFromList(IRedisList<T> fromList) {
            this._client.LTrim(fromList.Id, int.MaxValue, _firstElement);
        }

        /// <inheritdoc />
        public void TrimList(IRedisList<T> fromList, int keepStartingFrom, int keepEndingAt) {
            this._client.LTrim(fromList.Id, keepStartingFrom, keepEndingAt);
        }

        /// <inheritdoc />
        public long RemoveItemFromList(IRedisList<T> fromList, T value) {
            const int removeAll = 0;
            return this._client.LRem(fromList.Id, removeAll, this.SerializeValue(value));
        }

        /// <inheritdoc />
        public long RemoveItemFromList(IRedisList<T> fromList, T value, int noOfMatches) {
            return this._client.LRem(fromList.Id, noOfMatches, this.SerializeValue(value));
        }

        /// <inheritdoc />
        public long GetListCount(IRedisList<T> fromList) {
            return this._client.LLen(fromList.Id);
        }

        /// <inheritdoc />
        public T GetItemFromList(IRedisList<T> fromList, int listIndex) {
            return this.DeserializeValue(this._client.LIndex(fromList.Id, listIndex));
        }

        /// <inheritdoc />
        public void SetItemInList(IRedisList<T> toList, int listIndex, T value) {
            this._client.LSet(toList.Id, listIndex, this.SerializeValue(value));
        }

        /// <inheritdoc />
        public void InsertBeforeItemInList(IRedisList<T> toList, T pivot, T value) {
            this._client.LInsert(toList.Id, true, this.SerializeValue(pivot), this.SerializeValue(value));
        }

        /// <inheritdoc />
        public void InsertAfterItemInList(IRedisList<T> toList, T pivot, T value) {
            this._client.LInsert(toList.Id, false, this.SerializeValue(pivot), this.SerializeValue(value));
        }

        /// <inheritdoc />
        public void EnqueueItemOnList(IRedisList<T> fromList, T item) {
            this._client.LPush(fromList.Id, this.SerializeValue(item));
        }

        /// <inheritdoc />
        public T DequeueItemFromList(IRedisList<T> fromList) {
            return this.DeserializeValue(this._client.RPop(fromList.Id));
        }

        /// <inheritdoc />
        public T BlockingDequeueItemFromList(IRedisList<T> fromList, TimeSpan? timeout) {
            byte[][] unblockingKeyAndValue = this._client.BRPop(fromList.Id, (int)timeout.GetValueOrDefault().TotalSeconds);
            return unblockingKeyAndValue.Length == 0
                ? default
                : this.DeserializeValue(unblockingKeyAndValue[1]);
        }

        /// <inheritdoc />
        public void PushItemToList(IRedisList<T> fromList, T item) {
            this._client.RPush(fromList.Id, this.SerializeValue(item));
        }

        /// <inheritdoc />
        public T PopItemFromList(IRedisList<T> fromList) {
            return this.DeserializeValue(this._client.RPop(fromList.Id));
        }

        /// <inheritdoc />
        public T BlockingPopItemFromList(IRedisList<T> fromList, TimeSpan? timeout) {
            byte[][] unblockingKeyAndValue = this._client.BRPop(fromList.Id, (int)timeout.GetValueOrDefault().TotalSeconds);
            return unblockingKeyAndValue.Length == 0
                ? default
                : this.DeserializeValue(unblockingKeyAndValue[1]);
        }

        /// <inheritdoc />
        public T PopAndPushItemBetweenLists(IRedisList<T> fromList, IRedisList<T> toList) {
            return this.DeserializeValue(this._client.RPopLPush(fromList.Id, toList.Id));
        }

        /// <inheritdoc />
        public T BlockingPopAndPushItemBetweenLists(IRedisList<T> fromList, IRedisList<T> toList, TimeSpan? timeout) {
            return this.DeserializeValue(this._client.BRPopLPush(fromList.Id, toList.Id, (int)timeout.GetValueOrDefault().TotalSeconds));
        }

        private List<T> CreateList(byte[][] multiDataList) {
            if (multiDataList == null) {
                return new List<T>();
            }

            var results = new List<T>();
            foreach (byte[] multiData in multiDataList) {
                results.Add(this.DeserializeValue(multiData));
            }

            return results;
        }

        // TODO replace it with a pipeline implementation ala AddRangeToSet
        public void AddRangeToList(IRedisList<T> fromList, IEnumerable<T> values) {
            foreach (T value in values) {
                this.AddItemToList(fromList, value);
            }
        }

        /// <inheritdoc />
        internal class RedisClientLists : IHasNamed<IRedisList<T>> {

            private readonly RedisTypedClient<T> _client;

            /// <inheritdoc />
            public RedisClientLists(RedisTypedClient<T> client) {
                this._client = client;
            }

            /// <inheritdoc />
            public IRedisList<T> this[string listId] {
                get => new RedisClientList<T>(this._client, listId);
                set {
                    IRedisList<T> list = this[listId];
                    list.Clear();
                    list.CopyTo(value.ToArray(), 0);
                }
            }

        }

    }

}
