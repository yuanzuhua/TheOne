using System;
using System.Collections;
using System.Collections.Generic;
using TheOne.Redis.Common;

namespace TheOne.Redis.Client.Internal {

    internal class RedisClientList<T> : IRedisList<T> {

        public const int PageLimit = 1000;
        private readonly RedisTypedClient<T> _client;

        public RedisClientList(RedisTypedClient<T> client, string listId) {
            this.Id = listId;
            this._client = client;
        }

        /// <inheritdoc />
        public string Id { get; }

        /// <inheritdoc />
        public IEnumerator<T> GetEnumerator() {
            return this.Count <= PageLimit
                ? this._client.GetAllItemsFromList(this).GetEnumerator()
                : this.GetPagingEnumerator();
        }

        /// <inheritdoc />
        IEnumerator IEnumerable.GetEnumerator() {
            return this.GetEnumerator();
        }

        /// <inheritdoc />
        public void Add(T item) {
            this._client.AddItemToList(this, item);
        }

        /// <inheritdoc />
        public void Clear() {
            this._client.RemoveAllFromList(this);
        }

        /// <inheritdoc />
        public bool Contains(T item) {
            // TODO replace with native implementation when exists
            foreach (var existingItem in this) {
                if (Equals(existingItem, item)) {
                    return true;
                }
            }

            return false;
        }

        /// <inheritdoc />
        public void CopyTo(T[] array, int arrayIndex) {
            var allItemsInList = this._client.GetAllItemsFromList(this);
            allItemsInList.CopyTo(array, arrayIndex);
        }

        /// <inheritdoc />
        public bool Remove(T item) {
            var index = this.IndexOf(item);
            if (index != -1) {
                this.RemoveAt(index);
                return true;
            }

            return false;
        }

        /// <inheritdoc />
        public int Count => (int)this._client.GetListCount(this);

        /// <inheritdoc />
        public bool IsReadOnly => false;

        /// <inheritdoc />
        public int IndexOf(T item) {
            // TODO replace with native implementation when exists
            var i = 0;
            foreach (var existingItem in this) {
                if (Equals(existingItem, item)) {
                    return i;
                }

                i++;
            }

            return -1;
        }

        /// <inheritdoc />
        public void Insert(int index, T item) {
            this._client.InsertAfterItemInList(this, this[index], item);
        }

        /// <inheritdoc />
        public void RemoveAt(int index) {
            // TODO replace with native implementation when one exists
            var markForDelete = Guid.NewGuid().ToString();
            this._client.NativeClient.LSet(this.Id, index, markForDelete.ToUtf8Bytes());

            const int removeAll = 0;
            this._client.NativeClient.LRem(this.Id, removeAll, markForDelete.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public T this[int index] {
            get => this._client.GetItemFromList(this, index);
            set => this._client.SetItemInList(this, index, value);
        }

        /// <inheritdoc />
        public List<T> GetAll() {
            return this._client.GetAllItemsFromList(this);
        }

        /// <inheritdoc />
        public List<T> GetRange(int startingFrom, int endingAt) {
            return this._client.GetRangeFromList(this, startingFrom, endingAt);
        }

        /// <inheritdoc />
        public List<T> GetRangeFromSortedList(int startingFrom, int endingAt) {
            return this._client.SortList(this, startingFrom, endingAt);
        }

        /// <inheritdoc />
        public void RemoveAll() {
            this._client.RemoveAllFromList(this);
        }

        /// <inheritdoc />
        public void Trim(int keepStartingFrom, int keepEndingAt) {
            this._client.TrimList(this, keepStartingFrom, keepEndingAt);
        }

        /// <inheritdoc />
        public long RemoveValue(T value) {
            return this._client.RemoveItemFromList(this, value);
        }

        /// <inheritdoc />
        public long RemoveValue(T value, int noOfMatches) {
            return this._client.RemoveItemFromList(this, value, noOfMatches);
        }

        /// <inheritdoc />
        public void AddRange(IEnumerable<T> values) {
            this._client.AddRangeToList(this, values);
        }

        /// <inheritdoc />
        public void Append(T value) {
            this.Add(value);
        }

        /// <inheritdoc />
        public void Prepend(T value) {
            this._client.PrependItemToList(this, value);
        }

        /// <inheritdoc />
        public T RemoveStart() {
            return this._client.RemoveStartFromList(this);
        }

        /// <inheritdoc />
        public T BlockingRemoveStart(TimeSpan? timeout) {
            return this._client.BlockingRemoveStartFromList(this, timeout);
        }

        /// <inheritdoc />
        public T RemoveEnd() {
            return this._client.RemoveEndFromList(this);
        }

        /// <inheritdoc />
        public void Enqueue(T value) {
            this._client.EnqueueItemOnList(this, value);
        }

        /// <inheritdoc />
        public T Dequeue() {
            return this._client.DequeueItemFromList(this);
        }

        /// <inheritdoc />
        public T BlockingDequeue(TimeSpan? timeout) {
            return this._client.BlockingDequeueItemFromList(this, timeout);
        }

        /// <inheritdoc />
        public void Push(T value) {
            this._client.PushItemToList(this, value);
        }

        /// <inheritdoc />
        public T Pop() {
            return this._client.PopItemFromList(this);
        }

        /// <inheritdoc />
        public T BlockingPop(TimeSpan? timeout) {
            return this._client.BlockingPopItemFromList(this, timeout);
        }

        /// <inheritdoc />
        public T PopAndPush(IRedisList<T> toList) {
            return this._client.PopAndPushItemBetweenLists(this, toList);
        }

        public IEnumerator<T> GetPagingEnumerator() {
            var skip = 0;
            List<T> pageResults;
            do {
                pageResults = this._client.GetRangeFromList(this, skip, PageLimit);
                foreach (var result in pageResults) {
                    yield return result;
                }

                skip += PageLimit;
            } while (pageResults.Count == PageLimit);
        }

        public T BlockingPopAndPush(IRedisList<T> toList, TimeSpan? timeout) {
            return this._client.BlockingPopAndPushItemBetweenLists(this, toList, timeout);
        }

    }

}
