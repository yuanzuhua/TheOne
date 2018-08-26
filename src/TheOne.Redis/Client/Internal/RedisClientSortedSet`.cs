using System.Collections;
using System.Collections.Generic;

namespace TheOne.Redis.Client.Internal {

    /// <summary>
    ///     Wrap the common redis set operations under a ICollection[string] interface.
    /// </summary>
    internal class RedisClientSortedSet<T> : IRedisSortedSet<T> {

        public const int PageLimit = 1000;
        private readonly RedisTypedClient<T> _client;

        public RedisClientSortedSet(RedisTypedClient<T> client, string setId) {
            this._client = client;
            this.Id = setId;
        }

        public string Id { get; }

        public IEnumerator<T> GetEnumerator() {
            return this.Count <= PageLimit
                ? this._client.GetAllItemsFromSortedSet(this).GetEnumerator()
                : this.GetPagingEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return this.GetEnumerator();
        }

        public void Add(T item) {
            this._client.AddItemToSortedSet(this, item);
        }

        public void Clear() {
            this._client.RemoveEntry(this.Id);
        }

        public bool Contains(T item) {
            return this._client.SortedSetContainsItem(this, item);
        }

        public void CopyTo(T[] array, int arrayIndex) {
            List<T> allItemsInSet = this._client.GetAllItemsFromSortedSet(this);
            allItemsInSet.CopyTo(array, arrayIndex);
        }

        public bool Remove(T item) {
            this._client.RemoveItemFromSortedSet(this, item);
            return true;
        }

        public int Count {
            get {
                var setCount = (int)this._client.GetSortedSetCount(this);
                return setCount;
            }
        }

        public bool IsReadOnly => false;

        public T PopItemWithHighestScore() {
            return this._client.PopItemWithHighestScoreFromSortedSet(this);
        }

        public T PopItemWithLowestScore() {
            return this._client.PopItemWithLowestScoreFromSortedSet(this);
        }

        public double IncrementItem(T item, double incrementBy) {
            return this._client.IncrementItemInSortedSet(this, item, incrementBy);
        }

        public int IndexOf(T item) {
            return (int)this._client.GetItemIndexInSortedSet(this, item);
        }

        public long IndexOfDescending(T item) {
            return this._client.GetItemIndexInSortedSetDesc(this, item);
        }

        public List<T> GetAll() {
            return this._client.GetAllItemsFromSortedSet(this);
        }

        public List<T> GetAllDescending() {
            return this._client.GetAllItemsFromSortedSetDesc(this);
        }

        public List<T> GetRange(int fromRank, int toRank) {
            return this._client.GetRangeFromSortedSet(this, fromRank, toRank);
        }

        public List<T> GetRangeByLowestScore(double fromScore, double toScore) {
            return this._client.GetRangeFromSortedSetByLowestScore(this, fromScore, toScore);
        }

        public List<T> GetRangeByLowestScore(double fromScore, double toScore, int? skip, int? take) {
            return this._client.GetRangeFromSortedSetByLowestScore(this, fromScore, toScore, skip, take);
        }

        public List<T> GetRangeByHighestScore(double fromScore, double toScore) {
            return this._client.GetRangeFromSortedSetByHighestScore(this, fromScore, toScore);
        }

        public List<T> GetRangeByHighestScore(double fromScore, double toScore, int? skip, int? take) {
            return this._client.GetRangeFromSortedSetByHighestScore(this, fromScore, toScore, skip, take);
        }

        public long RemoveRange(int minRank, int maxRank) {
            return this._client.RemoveRangeFromSortedSet(this, minRank, maxRank);
        }

        public long RemoveRangeByScore(double fromScore, double toScore) {
            return this._client.RemoveRangeFromSortedSetByScore(this, fromScore, toScore);
        }

        public double GetItemScore(T item) {
            return this._client.GetItemScoreInSortedSet(this, item);
        }

        public long PopulateWithIntersectOf(params IRedisSortedSet<T>[] setIds) {
            return this._client.StoreIntersectFromSortedSets(this, setIds);
        }

        public long PopulateWithUnionOf(params IRedisSortedSet<T>[] setIds) {
            return this._client.StoreUnionFromSortedSets(this, setIds);
        }

        public IEnumerator<T> GetPagingEnumerator() {
            var skip = 0;
            List<T> pageResults;
            do {
                pageResults = this._client.GetRangeFromSortedSet(this, skip, skip + PageLimit - 1);
                foreach (T result in pageResults) {
                    yield return result;
                }

                skip += PageLimit;
            } while (pageResults.Count == PageLimit);
        }

        public void Add(T item, double score) {
            this._client.AddItemToSortedSet(this, item, score);
        }

        public long PopulateWithIntersectOf(IRedisSortedSet<T>[] setIds, string[] args) {
            return this._client.StoreIntersectFromSortedSets(this, setIds, args);
        }

        public long PopulateWithUnionOf(IRedisSortedSet<T>[] setIds, string[] args) {
            return this._client.StoreUnionFromSortedSets(this, setIds, args);
        }

    }

}
