using System.Collections;
using System.Collections.Generic;

namespace TheOne.Redis.Client.Internal {

    /// <summary>
    ///     Wrap the common redis set operations under a ICollection[string] interface.
    /// </summary>
    internal class RedisClientSet<T> : IRedisSet<T> {

        public const int PageLimit = 1000;
        private readonly RedisTypedClient<T> _client;

        public RedisClientSet(RedisTypedClient<T> client, string setId) {
            this._client = client;
            this.Id = setId;
        }

        public string Id { get; }

        public IEnumerator<T> GetEnumerator() {
            return this.Count <= PageLimit
                ? this._client.GetAllItemsFromSet(this).GetEnumerator()
                : this.GetPagingEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return this.GetEnumerator();
        }

        public void Add(T item) {
            this._client.AddItemToSet(this, item);
        }

        public void Clear() {
            this._client.RemoveEntry(this.Id);
        }

        public bool Contains(T item) {
            return this._client.SetContainsItem(this, item);
        }

        public void CopyTo(T[] array, int arrayIndex) {
            HashSet<T> allItemsInSet = this._client.GetAllItemsFromSet(this);
            allItemsInSet.CopyTo(array, arrayIndex);
        }

        public bool Remove(T item) {
            this._client.RemoveItemFromSet(this, item);
            return true;
        }

        public int Count {
            get {
                var setCount = (int)this._client.GetSetCount(this);
                return setCount;
            }
        }

        public bool IsReadOnly => false;

        public List<T> Sort(int startingFrom, int endingAt) {
            return this._client.GetSortedEntryValues(this, startingFrom, endingAt);
        }

        public HashSet<T> GetAll() {
            return this._client.GetAllItemsFromSet(this);
        }

        public T PopRandomItem() {
            return this._client.PopItemFromSet(this);
        }

        public T GetRandomItem() {
            return this._client.GetRandomItemFromSet(this);
        }

        public void MoveTo(T item, IRedisSet<T> toSet) {
            this._client.MoveBetweenSets(this, toSet, item);
        }

        public void PopulateWithIntersectOf(params IRedisSet<T>[] sets) {
            this._client.StoreIntersectFromSets(this, sets);
        }

        public void PopulateWithUnionOf(params IRedisSet<T>[] sets) {
            this._client.StoreUnionFromSets(this, sets);
        }

        public void GetDifferences(params IRedisSet<T>[] withSets) {
            this._client.StoreUnionFromSets(this, withSets);
        }

        public void PopulateWithDifferencesOf(IRedisSet<T> fromSet, params IRedisSet<T>[] withSets) {
            this._client.StoreDifferencesFromSet(this, fromSet, withSets);
        }

        public IEnumerator<T> GetPagingEnumerator() {
            var skip = 0;
            List<T> pageResults;
            do {
                pageResults = this._client.GetSortedEntryValues(this, skip, skip + PageLimit - 1);
                foreach (T result in pageResults) {
                    yield return result;
                }

                skip += PageLimit;
            } while (pageResults.Count == PageLimit);
        }

    }

}
