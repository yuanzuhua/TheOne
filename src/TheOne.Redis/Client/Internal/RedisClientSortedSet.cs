using System.Collections;
using System.Collections.Generic;

namespace TheOne.Redis.Client.Internal {

    /// <summary>
    ///     Wrap the common redis set operations under a ICollection[string] interface.
    /// </summary>
    internal class RedisClientSortedSet : IRedisSortedSet {

        public const int PageLimit = 1000;
        private readonly RedisClient _client;

        public RedisClientSortedSet(RedisClient client, string setId) {
            this._client = client;
            this.Id = setId;
        }

        public IEnumerator<string> GetEnumerator() {
            return this.Count <= PageLimit
                ? this._client.GetAllItemsFromSortedSet(this.Id).GetEnumerator()
                : this.GetPagingEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return this.GetEnumerator();
        }

        public void Add(string item) {
            this._client.AddItemToSortedSet(this.Id, item);
        }

        public void Clear() {
            this._client.Remove(this.Id);
        }

        public bool Contains(string item) {
            return this._client.SortedSetContainsItem(this.Id, item);
        }

        public void CopyTo(string[] array, int arrayIndex) {
            List<string> allItemsInSet = this._client.GetAllItemsFromSortedSet(this.Id);
            allItemsInSet.CopyTo(array, arrayIndex);
        }

        public bool Remove(string item) {
            this._client.RemoveItemFromSortedSet(this.Id, item);
            return true;
        }

        public int Count => (int)this._client.GetSortedSetCount(this.Id);

        public bool IsReadOnly => false;

        public string Id { get; }

        public List<string> GetAll() {
            return this._client.GetAllItemsFromSortedSet(this.Id);
        }

        public List<string> GetRange(int startingRank, int endingRank) {
            return this._client.GetRangeFromSortedSet(this.Id, startingRank, endingRank);
        }

        public List<string> GetRangeByScore(string fromStringScore, string toStringScore) {
            return this.GetRangeByScore(fromStringScore, toStringScore, null, null);
        }

        public List<string> GetRangeByScore(string fromStringScore, string toStringScore, int? skip, int? take) {
            return this._client.GetRangeFromSortedSetByLowestScore(this.Id, fromStringScore, toStringScore, skip, take);
        }

        public List<string> GetRangeByScore(double fromScore, double toScore) {
            return this.GetRangeByScore(fromScore, toScore, null, null);
        }

        public List<string> GetRangeByScore(double fromScore, double toScore, int? skip, int? take) {
            return this._client.GetRangeFromSortedSetByLowestScore(this.Id, fromScore, toScore, skip, take);
        }

        public void RemoveRange(int startingFrom, int toRank) {
            this._client.RemoveRangeFromSortedSet(this.Id, startingFrom, toRank);
        }

        public void RemoveRangeByScore(double fromScore, double toScore) {
            this._client.RemoveRangeFromSortedSetByScore(this.Id, fromScore, toScore);
        }

        public void StoreFromIntersect(params IRedisSortedSet[] ofSets) {
            this._client.StoreIntersectFromSortedSets(this.Id, ofSets.GetIds());
        }

        public void StoreFromUnion(params IRedisSortedSet[] ofSets) {
            this._client.StoreUnionFromSortedSets(this.Id, ofSets.GetIds());
        }

        public long GetItemIndex(string value) {
            return this._client.GetItemIndexInSortedSet(this.Id, value);
        }

        public double GetItemScore(string value) {
            return this._client.GetItemScoreInSortedSet(this.Id, value);
        }

        public string PopItemWithLowestScore() {
            return this._client.PopItemWithLowestScoreFromSortedSet(this.Id);
        }

        public string PopItemWithHighestScore() {
            return this._client.PopItemWithHighestScoreFromSortedSet(this.Id);
        }

        public void IncrementItemScore(string value, double incrementByScore) {
            this._client.IncrementItemInSortedSet(this.Id, value, incrementByScore);
        }

        public IEnumerator<string> GetPagingEnumerator() {
            var skip = 0;
            List<string> pageResults;
            do {
                pageResults = this._client.GetRangeFromSortedSet(this.Id, skip, skip + PageLimit - 1);
                foreach (var result in pageResults) {
                    yield return result;
                }

                skip += PageLimit;
            } while (pageResults.Count == PageLimit);
        }

    }

}
