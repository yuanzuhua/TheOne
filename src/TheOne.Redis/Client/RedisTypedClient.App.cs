using System;
using System.Collections.Generic;
using System.Linq;
using TheOne.Redis.Common;

namespace TheOne.Redis.Client {

    public partial class RedisTypedClient<T> {

        /// <inheritdoc />
        public void StoreRelatedEntities<TChild>(object parentId, List<TChild> children) {
            var childRefKey = this.GetChildReferenceSetKey<TChild>(parentId);
            var childKeys = children.ConvertAll(x => this._client.UrnKey(x));

            using (var trans = this._client.CreateTransaction()) {
                // Ugly but need access to a generic constraint-free StoreAll method
                trans.QueueCommand(c => ((RedisClient)c)._StoreAll(children));
                trans.QueueCommand(c => c.AddRangeToSet(childRefKey, childKeys));

                trans.Commit();
            }
        }

        /// <inheritdoc />
        public void StoreRelatedEntities<TChild>(object parentId, params TChild[] children) {
            this.StoreRelatedEntities(parentId, new List<TChild>(children));
        }

        /// <inheritdoc />
        public void DeleteRelatedEntity<TChild>(object parentId, object childId) {
            var childRefKey = this.GetChildReferenceSetKey<TChild>(parentId);

            this._client.RemoveItemFromSet(childRefKey, childId.ToJson());
        }

        /// <inheritdoc />
        public void DeleteRelatedEntities<TChild>(object parentId) {
            var childRefKey = this.GetChildReferenceSetKey<TChild>(parentId);
            this._client.Remove(childRefKey);
        }

        /// <inheritdoc />
        public List<TChild> GetRelatedEntities<TChild>(object parentId) {
            var childRefKey = this.GetChildReferenceSetKey<TChild>(parentId);
            var childKeys = this._client.GetAllItemsFromSet(childRefKey).ToList();

            return this._client.As<TChild>().GetValues(childKeys);
        }

        /// <inheritdoc />
        public long GetRelatedEntitiesCount<TChild>(object parentId) {
            var childRefKey = this.GetChildReferenceSetKey<TChild>(parentId);
            return this._client.GetSetCount(childRefKey);
        }

        /// <inheritdoc />
        public void AddToRecentsList(T value) {
            var key = this._client.UrnKey(value);
            var nowScore = DateTime.UtcNow.ToUnixTime();
            this._client.AddItemToSortedSet(this._recentSortedSetKey, key, nowScore);
        }

        /// <inheritdoc />
        public List<T> GetLatestFromRecentsList(int skip, int take) {
            var toRank = take - 1;
            var keys = this._client.GetRangeFromSortedSetDesc(this._recentSortedSetKey, skip, toRank);
            var values = this.GetValues(keys);
            return values;
        }

        /// <inheritdoc />
        public List<T> GetEarliestFromRecentsList(int skip, int take) {
            var toRank = take - 1;
            var keys = this._client.GetRangeFromSortedSet(this._recentSortedSetKey, skip, toRank);
            var values = this.GetValues(keys);
            return values;
        }

        private string GetChildReferenceSetKey<TChild>(object parentId) {
            return string.Concat(this._client.NamespacePrefix, "ref:", typeof(T).Name, "/", typeof(TChild).Name, ":", parentId);
        }

    }

}
