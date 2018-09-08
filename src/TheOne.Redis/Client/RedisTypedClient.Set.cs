using System.Collections.Generic;
using System.Linq;
using TheOne.Redis.Client.Internal;

namespace TheOne.Redis.Client {

    public partial class RedisTypedClient<T> {

        /// <inheritdoc />
        public IHasNamed<IRedisSet<T>> Sets { get; set; }

        /// <inheritdoc />
        public long Db {
            get => this._client.Db;
            set => this._client.Db = value;
        }

        /// <inheritdoc />
        public List<T> GetSortedEntryValues(IRedisSet<T> fromSet, int startingFrom, int endingAt) {
            var sortOptions = new SortOptions { Skip = startingFrom, Take = endingAt };
            byte[][] multiDataList = this._client.Sort(fromSet.Id, sortOptions);
            return this.CreateList(multiDataList);
        }

        /// <inheritdoc />
        public HashSet<T> GetAllItemsFromSet(IRedisSet<T> fromSet) {
            byte[][] multiDataList = this._client.SMembers(fromSet.Id);
            return this.CreateHashSet(multiDataList);
        }

        /// <inheritdoc />
        public void AddItemToSet(IRedisSet<T> toSet, T item) {
            this._client.SAdd(toSet.Id, this.SerializeValue(item));
        }

        /// <inheritdoc />
        public void RemoveItemFromSet(IRedisSet<T> fromSet, T item) {
            this._client.SRem(fromSet.Id, this.SerializeValue(item));
        }

        /// <inheritdoc />
        public T PopItemFromSet(IRedisSet<T> fromSet) {
            return this.DeserializeValue(this._client.SPop(fromSet.Id));
        }

        /// <inheritdoc />
        public void MoveBetweenSets(IRedisSet<T> fromSet, IRedisSet<T> toSet, T item) {
            this._client.SMove(fromSet.Id, toSet.Id, this.SerializeValue(item));
        }

        /// <inheritdoc />
        public long GetSetCount(IRedisSet<T> set) {
            return this._client.SCard(set.Id);
        }

        /// <inheritdoc />
        public bool SetContainsItem(IRedisSet<T> set, T item) {
            return this._client.SIsMember(set.Id, this.SerializeValue(item)) == 1;
        }

        /// <inheritdoc />
        public HashSet<T> GetIntersectFromSets(params IRedisSet<T>[] sets) {
            byte[][] multiDataList = this._client.SInter(sets.Select(x => x.Id).ToArray());
            return this.CreateHashSet(multiDataList);
        }

        /// <inheritdoc />
        public void StoreIntersectFromSets(IRedisSet<T> intoSet, params IRedisSet<T>[] sets) {
            this._client.SInterStore(intoSet.Id, sets.Select(x => x.Id).ToArray());
        }

        /// <inheritdoc />
        public HashSet<T> GetUnionFromSets(params IRedisSet<T>[] sets) {
            byte[][] multiDataList = this._client.SUnion(sets.Select(x => x.Id).ToArray());
            return this.CreateHashSet(multiDataList);
        }

        /// <inheritdoc />
        public void StoreUnionFromSets(IRedisSet<T> intoSet, params IRedisSet<T>[] sets) {
            this._client.SUnionStore(intoSet.Id, sets.Select(x => x.Id).ToArray());
        }

        /// <inheritdoc />
        public HashSet<T> GetDifferencesFromSet(IRedisSet<T> fromSet, params IRedisSet<T>[] withSets) {
            byte[][] multiDataList = this._client.SDiff(fromSet.Id, withSets.Select(x => x.Id).ToArray());
            return this.CreateHashSet(multiDataList);
        }

        /// <inheritdoc />
        public void StoreDifferencesFromSet(IRedisSet<T> intoSet, IRedisSet<T> fromSet, params IRedisSet<T>[] withSets) {
            this._client.SDiffStore(intoSet.Id, fromSet.Id, withSets.Select(x => x.Id).ToArray());
        }

        /// <inheritdoc />
        public T GetRandomItemFromSet(IRedisSet<T> fromSet) {
            return this.DeserializeValue(this._client.SRandMember(fromSet.Id));
        }

        private HashSet<T> CreateHashSet(byte[][] multiDataList) {
            var results = new HashSet<T>();
            foreach (byte[] multiData in multiDataList) {
                results.Add(this.DeserializeValue(multiData));
            }

            return results;
        }

        /// <inheritdoc />
        internal class RedisClientSets : IHasNamed<IRedisSet<T>> {

            private readonly RedisTypedClient<T> _client;

            /// <inheritdoc />
            public RedisClientSets(RedisTypedClient<T> client) {
                this._client = client;
            }

            /// <inheritdoc />
            public IRedisSet<T> this[string setId] {
                get => new RedisClientSet<T>(this._client, setId);
                set {
                    IRedisSet<T> col = this[setId];
                    col.Clear();
                    col.CopyTo(value.ToArray(), 0);
                }
            }

        }

    }

}
