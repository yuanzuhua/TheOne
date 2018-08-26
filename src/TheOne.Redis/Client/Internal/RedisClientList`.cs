using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace TheOne.Redis.Client.Internal {

    internal class RedisClientList<T> : IRedisList<T> {

        public const int PageLimit = 1000;
        private readonly RedisTypedClient<T> _client;

        public RedisClientList(RedisTypedClient<T> client, string listId) {
            this.Id = listId;
            this._client = client;
        }

        public string Id { get; }

        public IEnumerator<T> GetEnumerator() {
            return this.Count <= PageLimit
                ? this._client.GetAllItemsFromList(this).GetEnumerator()
                : this.GetPagingEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return this.GetEnumerator();
        }

        public void Add(T item) {
            this._client.AddItemToList(this, item);
        }

        public void Clear() {
            this._client.RemoveAllFromList(this);
        }

        public bool Contains(T item) {
            // TODO replace with native implementation when exists
            foreach (T existingItem in this) {
                if (Equals(existingItem, item)) {
                    return true;
                }
            }

            return false;
        }

        public void CopyTo(T[] array, int arrayIndex) {
            List<T> allItemsInList = this._client.GetAllItemsFromList(this);
            allItemsInList.CopyTo(array, arrayIndex);
        }

        public bool Remove(T item) {
            var index = this.IndexOf(item);
            if (index != -1) {
                this.RemoveAt(index);
                return true;
            }

            return false;
        }

        public int Count => (int)this._client.GetListCount(this);

        public bool IsReadOnly => false;

        public int IndexOf(T item) {
            // TODO replace with native implementation when exists
            var i = 0;
            foreach (T existingItem in this) {
                if (Equals(existingItem, item)) {
                    return i;
                }

                i++;
            }

            return -1;
        }

        public void Insert(int index, T item) {
            this._client.InsertAfterItemInList(this, this[index], item);
        }

        public void RemoveAt(int index) {
            // TODO replace with native implementation when one exists
            var markForDelete = Guid.NewGuid().ToString();
            this._client.NativeClient.LSet(this.Id, index, Encoding.UTF8.GetBytes(markForDelete));

            const int removeAll = 0;
            this._client.NativeClient.LRem(this.Id, removeAll, Encoding.UTF8.GetBytes(markForDelete));
        }

        public T this[int index] {
            get => this._client.GetItemFromList(this, index);
            set => this._client.SetItemInList(this, index, value);
        }

        public List<T> GetAll() {
            return this._client.GetAllItemsFromList(this);
        }

        public List<T> GetRange(int startingFrom, int endingAt) {
            return this._client.GetRangeFromList(this, startingFrom, endingAt);
        }

        public List<T> GetRangeFromSortedList(int startingFrom, int endingAt) {
            return this._client.SortList(this, startingFrom, endingAt);
        }

        public void RemoveAll() {
            this._client.RemoveAllFromList(this);
        }

        public void Trim(int keepStartingFrom, int keepEndingAt) {
            this._client.TrimList(this, keepStartingFrom, keepEndingAt);
        }

        public long RemoveValue(T value) {
            return this._client.RemoveItemFromList(this, value);
        }

        public long RemoveValue(T value, int noOfMatches) {
            return this._client.RemoveItemFromList(this, value, noOfMatches);
        }

        public void AddRange(IEnumerable<T> values) {
            this._client.AddRangeToList(this, values);
        }

        public void Append(T value) {
            this.Add(value);
        }

        public void Prepend(T value) {
            this._client.PrependItemToList(this, value);
        }

        public T RemoveStart() {
            return this._client.RemoveStartFromList(this);
        }

        public T BlockingRemoveStart(TimeSpan? timeout) {
            return this._client.BlockingRemoveStartFromList(this, timeout);
        }

        public T RemoveEnd() {
            return this._client.RemoveEndFromList(this);
        }

        public void Enqueue(T value) {
            this._client.EnqueueItemOnList(this, value);
        }

        public T Dequeue() {
            return this._client.DequeueItemFromList(this);
        }

        public T BlockingDequeue(TimeSpan? timeout) {
            return this._client.BlockingDequeueItemFromList(this, timeout);
        }

        public void Push(T value) {
            this._client.PushItemToList(this, value);
        }

        public T Pop() {
            return this._client.PopItemFromList(this);
        }

        public T BlockingPop(TimeSpan? timeout) {
            return this._client.BlockingPopItemFromList(this, timeout);
        }

        public T PopAndPush(IRedisList<T> toList) {
            return this._client.PopAndPushItemBetweenLists(this, toList);
        }

        public IEnumerator<T> GetPagingEnumerator() {
            var skip = 0;
            List<T> pageResults;
            do {
                pageResults = this._client.GetRangeFromList(this, skip, PageLimit);
                foreach (T result in pageResults) {
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
