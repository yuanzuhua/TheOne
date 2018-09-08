using System;
using System.Collections;
using System.Collections.Generic;

namespace TheOne.Redis.Client.Internal {

    /// <summary>
    ///     Wrap the common redis list operations under a IList[string] interface.
    /// </summary>
    internal class RedisClientList : IRedisList {

        public const int PageLimit = 1000;
        private readonly RedisClient _client;

        public RedisClientList(RedisClient client, string listId) {
            this.Id = listId;
            this._client = client;
        }

        /// <inheritdoc />
        public string Id { get; }

        /// <inheritdoc />
        public IEnumerator<string> GetEnumerator() {
            return this.Count <= PageLimit
                ? this._client.GetAllItemsFromList(this.Id).GetEnumerator()
                : this.GetPagingEnumerator();
        }

        /// <inheritdoc />
        IEnumerator IEnumerable.GetEnumerator() {
            return this.GetEnumerator();
        }

        /// <inheritdoc />
        public void Add(string item) {
            this._client.AddItemToList(this.Id, item);
        }

        /// <inheritdoc />
        public void Clear() {
            this._client.RemoveAllFromList(this.Id);
        }

        /// <inheritdoc />
        public bool Contains(string item) {
            // TODO replace with native implementation when exists
            foreach (var existingItem in this) {
                if (existingItem == item) {
                    return true;
                }
            }

            return false;
        }

        /// <inheritdoc />
        public void CopyTo(string[] array, int arrayIndex) {
            List<string> allItemsInList = this._client.GetAllItemsFromList(this.Id);
            allItemsInList.CopyTo(array, arrayIndex);
        }

        /// <inheritdoc />
        public bool Remove(string item) {
            return this._client.RemoveItemFromList(this.Id, item) > 0;
        }

        /// <inheritdoc />
        public int Count => (int)this._client.GetListCount(this.Id);

        /// <inheritdoc />
        public bool IsReadOnly => false;

        public int IndexOf(string item) {
            // TODO replace with native implementation when exists
            var i = 0;
            foreach (var existingItem in this) {
                if (existingItem == item) {
                    return i;
                }

                i++;
            }

            return -1;
        }

        /// <inheritdoc />
        public void Insert(int index, string item) {
            // TODO replace with implementation involving creating on new temp list then replacing
            // otherwise wait for native implementation
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public void RemoveAt(int index) {
            // TODO replace with native implementation when one exists
            var markForDelete = Guid.NewGuid().ToString();
            this._client.SetItemInList(this.Id, index, markForDelete);
            this._client.RemoveItemFromList(this.Id, markForDelete);
        }

        /// <inheritdoc />
        public string this[int index] {
            get => this._client.GetItemFromList(this.Id, index);
            set => this._client.SetItemInList(this.Id, index, value);
        }

        /// <inheritdoc />
        public List<string> GetAll() {
            return this._client.GetAllItemsFromList(this.Id);
        }

        /// <inheritdoc />
        public List<string> GetRange(int startingFrom, int endingAt) {
            return this._client.GetRangeFromList(this.Id, startingFrom, endingAt);
        }

        /// <inheritdoc />
        public List<string> GetRangeFromSortedList(int startingFrom, int endingAt) {
            return this._client.GetRangeFromSortedList(this.Id, startingFrom, endingAt);
        }

        /// <inheritdoc />
        public void RemoveAll() {
            this._client.RemoveAllFromList(this.Id);
        }

        /// <inheritdoc />
        public void Trim(int keepStartingFrom, int keepEndingAt) {
            this._client.TrimList(this.Id, keepStartingFrom, keepEndingAt);
        }

        /// <inheritdoc />
        public long RemoveValue(string value) {
            return this._client.RemoveItemFromList(this.Id, value);
        }

        /// <inheritdoc />
        public long RemoveValue(string value, int noOfMatches) {
            return this._client.RemoveItemFromList(this.Id, value, noOfMatches);
        }

        /// <inheritdoc />
        public void Append(string value) {
            this.Add(value);
        }

        /// <inheritdoc />
        public string RemoveStart() {
            return this._client.RemoveStartFromList(this.Id);
        }

        /// <inheritdoc />
        public string BlockingRemoveStart(TimeSpan? timeout) {
            return this._client.BlockingRemoveStartFromList(this.Id, timeout);
        }

        /// <inheritdoc />
        public string RemoveEnd() {
            return this._client.RemoveEndFromList(this.Id);
        }

        /// <inheritdoc />
        public void Enqueue(string value) {
            this._client.EnqueueItemOnList(this.Id, value);
        }

        /// <inheritdoc />
        public void Prepend(string value) {
            this._client.PrependItemToList(this.Id, value);
        }

        /// <inheritdoc />
        public void Push(string value) {
            this._client.PushItemToList(this.Id, value);
        }

        /// <inheritdoc />
        public string Pop() {
            return this._client.PopItemFromList(this.Id);
        }

        /// <inheritdoc />
        public string BlockingPop(TimeSpan? timeout) {
            return this._client.BlockingPopItemFromList(this.Id, timeout);
        }

        /// <inheritdoc />
        public string Dequeue() {
            return this._client.DequeueItemFromList(this.Id);
        }

        /// <inheritdoc />
        public string BlockingDequeue(TimeSpan? timeout) {
            return this._client.BlockingDequeueItemFromList(this.Id, timeout);
        }

        /// <inheritdoc />
        public string PopAndPush(IRedisList toList) {
            return this._client.PopAndPushItemBetweenLists(this.Id, toList.Id);
        }

        public IEnumerator<string> GetPagingEnumerator() {
            var skip = 0;
            List<string> pageResults;
            do {
                pageResults = this._client.GetRangeFromList(this.Id, skip, skip + PageLimit - 1);
                foreach (var result in pageResults) {
                    yield return result;
                }

                skip += PageLimit;
            } while (pageResults.Count == PageLimit);
        }

    }

}
