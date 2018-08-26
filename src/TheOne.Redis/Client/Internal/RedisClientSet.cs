using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace TheOne.Redis.Client.Internal {

    /// <summary>
    ///     Wrap the common redis set operations under a ICollection[string] interface.
    /// </summary>
    internal class RedisClientSet : IRedisSet {

        public const int PageLimit = 1000;
        private readonly RedisClient _client;

        public RedisClientSet(RedisClient client, string setId) {
            this._client = client;
            this.Id = setId;
        }

        public IEnumerator<string> GetEnumerator() {
            return this.Count <= PageLimit
                ? this._client.GetAllItemsFromSet(this.Id).GetEnumerator()
                : this.GetPagingEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return this.GetEnumerator();
        }

        public void Add(string item) {
            this._client.AddItemToSet(this.Id, item);
        }

        public void Clear() {
            this._client.Remove(this.Id);
        }

        public bool Contains(string item) {
            return this._client.SetContainsItem(this.Id, item);
        }

        public void CopyTo(string[] array, int arrayIndex) {
            HashSet<string> allItemsInSet = this._client.GetAllItemsFromSet(this.Id);
            allItemsInSet.CopyTo(array, arrayIndex);
        }

        public bool Remove(string item) {
            this._client.RemoveItemFromSet(this.Id, item);
            return true;
        }

        public int Count => (int)this._client.GetSetCount(this.Id);

        public bool IsReadOnly => false;

        public string Id { get; }

        public List<string> GetRangeFromSortedSet(int startingFrom, int endingAt) {
            return this._client.GetSortedEntryValues(this.Id, startingFrom, endingAt);
        }

        public HashSet<string> GetAll() {
            return this._client.GetAllItemsFromSet(this.Id);
        }

        public string Pop() {
            return this._client.PopItemFromSet(this.Id);
        }

        public void Move(string value, IRedisSet toSet) {
            this._client.MoveBetweenSets(this.Id, toSet.Id, value);
        }

        public HashSet<string> Intersect(params IRedisSet[] withSets) {
            List<string> allSetIds = this.MergeSetIds(withSets);
            return this._client.GetIntersectFromSets(allSetIds.ToArray());
        }

        public void StoreIntersect(params IRedisSet[] withSets) {
            string[] withSetIds = withSets.ToList().ConvertAll(x => x.Id).ToArray();
            this._client.StoreIntersectFromSets(this.Id, withSetIds);
        }

        public HashSet<string> Union(params IRedisSet[] withSets) {
            List<string> allSetIds = this.MergeSetIds(withSets);
            return this._client.GetUnionFromSets(allSetIds.ToArray());
        }

        public void StoreUnion(params IRedisSet[] withSets) {
            string[] withSetIds = withSets.ToList().ConvertAll(x => x.Id).ToArray();
            this._client.StoreUnionFromSets(this.Id, withSetIds);
        }

        public HashSet<string> Diff(IRedisSet[] withSets) {
            string[] withSetIds = withSets.ToList().ConvertAll(x => x.Id).ToArray();
            return this._client.GetDifferencesFromSet(this.Id, withSetIds);
        }

        public void StoreDiff(IRedisSet fromSet, params IRedisSet[] withSets) {
            string[] withSetIds = withSets.ToList().ConvertAll(x => x.Id).ToArray();
            this._client.StoreDifferencesFromSet(this.Id, fromSet.Id, withSetIds);
        }

        public string GetRandomEntry() {
            return this._client.GetRandomItemFromSet(this.Id);
        }

        public IEnumerator<string> GetPagingEnumerator() {
            var skip = 0;
            List<string> pageResults;
            do {
                pageResults = this._client.GetSortedEntryValues(this.Id, skip, skip + PageLimit - 1);
                foreach (var result in pageResults) {
                    yield return result;
                }

                skip += PageLimit;
            } while (pageResults.Count == PageLimit);
        }

        private List<string> MergeSetIds(IRedisSet[] withSets) {
            var allSetIds = new List<string> { this.Id };
            allSetIds.AddRange(withSets.ToList().ConvertAll(x => x.Id));
            return allSetIds;
        }

    }

}
