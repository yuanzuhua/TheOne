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

        public string Id { get; }

        public IEnumerator<string> GetEnumerator() {
            return this.Count <= PageLimit
                ? this._client.GetAllItemsFromList(this.Id).GetEnumerator()
                : this.GetPagingEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return this.GetEnumerator();
        }

        public void Add(string item) {
            this._client.AddItemToList(this.Id, item);
        }

        public void Clear() {
            this._client.RemoveAllFromList(this.Id);
        }

        public bool Contains(string item) {
            // TODO replace with native implementation when exists
            foreach (var existingItem in this) {
                if (existingItem == item) {
                    return true;
                }
            }

            return false;
        }

        public void CopyTo(string[] array, int arrayIndex) {
            List<string> allItemsInList = this._client.GetAllItemsFromList(this.Id);
            allItemsInList.CopyTo(array, arrayIndex);
        }

        public bool Remove(string item) {
            return this._client.RemoveItemFromList(this.Id, item) > 0;
        }

        public int Count => (int)this._client.GetListCount(this.Id);

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

        public void Insert(int index, string item) {
            // TODO replace with implementation involving creating on new temp list then replacing
            // otherwise wait for native implementation
            throw new NotImplementedException();
        }

        public void RemoveAt(int index) {
            // TODO replace with native implementation when one exists
            var markForDelete = Guid.NewGuid().ToString();
            this._client.SetItemInList(this.Id, index, markForDelete);
            this._client.RemoveItemFromList(this.Id, markForDelete);
        }

        public string this[int index] {
            get => this._client.GetItemFromList(this.Id, index);
            set => this._client.SetItemInList(this.Id, index, value);
        }

        public List<string> GetAll() {
            return this._client.GetAllItemsFromList(this.Id);
        }

        public List<string> GetRange(int startingFrom, int endingAt) {
            return this._client.GetRangeFromList(this.Id, startingFrom, endingAt);
        }

        public List<string> GetRangeFromSortedList(int startingFrom, int endingAt) {
            return this._client.GetRangeFromSortedList(this.Id, startingFrom, endingAt);
        }

        public void RemoveAll() {
            this._client.RemoveAllFromList(this.Id);
        }

        public void Trim(int keepStartingFrom, int keepEndingAt) {
            this._client.TrimList(this.Id, keepStartingFrom, keepEndingAt);
        }

        public long RemoveValue(string value) {
            return this._client.RemoveItemFromList(this.Id, value);
        }

        public long RemoveValue(string value, int noOfMatches) {
            return this._client.RemoveItemFromList(this.Id, value, noOfMatches);
        }

        public void Append(string value) {
            this.Add(value);
        }

        public string RemoveStart() {
            return this._client.RemoveStartFromList(this.Id);
        }

        public string BlockingRemoveStart(TimeSpan? timeout) {
            return this._client.BlockingRemoveStartFromList(this.Id, timeout);
        }

        public string RemoveEnd() {
            return this._client.RemoveEndFromList(this.Id);
        }

        public void Enqueue(string value) {
            this._client.EnqueueItemOnList(this.Id, value);
        }

        public void Prepend(string value) {
            this._client.PrependItemToList(this.Id, value);
        }

        public void Push(string value) {
            this._client.PushItemToList(this.Id, value);
        }

        public string Pop() {
            return this._client.PopItemFromList(this.Id);
        }

        public string BlockingPop(TimeSpan? timeout) {
            return this._client.BlockingPopItemFromList(this.Id, timeout);
        }

        public string Dequeue() {
            return this._client.DequeueItemFromList(this.Id);
        }

        public string BlockingDequeue(TimeSpan? timeout) {
            return this._client.BlockingDequeueItemFromList(this.Id, timeout);
        }

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
