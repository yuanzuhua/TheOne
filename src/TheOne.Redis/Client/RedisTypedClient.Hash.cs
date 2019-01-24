using System.Collections.Generic;
using System.Linq;
using TheOne.Redis.Client.Internal;
using TheOne.Redis.Common;

namespace TheOne.Redis.Client {

    public partial class RedisTypedClient<T> {

        /// <inheritdoc />
        public IRedisHash<TKey, T> GetHash<TKey>(string hashId) {
            return new RedisClientHash<TKey, T>(this, hashId);
        }

        /// <inheritdoc />
        public bool HashContainsEntry<TKey>(IRedisHash<TKey, T> hash, TKey key) {
            return this._client.HashContainsEntry(hash.Id, key.ToJson());
        }

        /// <inheritdoc />
        public bool SetEntryInHash<TKey>(IRedisHash<TKey, T> hash, TKey key, T value) {
            return this._client.SetEntryInHash(hash.Id, key.ToJson(), value.ToJson());
        }

        /// <inheritdoc />
        public bool SetEntryInHashIfNotExists<TKey>(IRedisHash<TKey, T> hash, TKey key, T value) {
            return this._client.SetEntryInHashIfNotExists(hash.Id, key.ToJson(), value.ToJson());
        }

        /// <inheritdoc />
        public void SetRangeInHash<TKey>(IRedisHash<TKey, T> hash, IEnumerable<KeyValuePair<TKey, T>> keyValuePairs) {
            var stringKeyValuePairs = keyValuePairs.ToList().ConvertAll(
                x => new KeyValuePair<string, string>(
                    x.Key.ToJson(),
                    x.Value.ToJson()));

            this._client.SetRangeInHash(hash.Id, stringKeyValuePairs);
        }

        /// <inheritdoc />
        public T GetValueFromHash<TKey>(IRedisHash<TKey, T> hash, TKey key) {
            return DeserializeFromString(this._client.GetValueFromHash(hash.Id, key.ToJson()));
        }

        /// <inheritdoc />
        public bool RemoveEntryFromHash<TKey>(IRedisHash<TKey, T> hash, TKey key) {
            return this._client.RemoveEntryFromHash(hash.Id, key.ToJson());
        }

        /// <inheritdoc />
        public long GetHashCount<TKey>(IRedisHash<TKey, T> hash) {
            return this._client.GetHashCount(hash.Id);
        }

        /// <inheritdoc />
        public List<TKey> GetHashKeys<TKey>(IRedisHash<TKey, T> hash) {
            return ConvertEachTo<TKey>(this._client.GetHashKeys(hash.Id));
        }

        /// <inheritdoc />
        public List<T> GetHashValues<TKey>(IRedisHash<TKey, T> hash) {
            return ConvertEachTo<T>(this._client.GetHashValues(hash.Id));
        }

        /// <inheritdoc />
        public Dictionary<TKey, T> GetAllEntriesFromHash<TKey>(IRedisHash<TKey, T> hash) {
            return ConvertEachTo<TKey, T>(this._client.GetAllEntriesFromHash(hash.Id));
        }

        private static Dictionary<TKey, TValue> ConvertEachTo<TKey, TValue>(IDictionary<string, string> map) {
            var to = new Dictionary<TKey, TValue>();
            foreach (var item in map) {
                to[item.Key.FromJson<TKey>()] = item.Value.FromJson<TValue>();
            }

            return to;
        }

        private static List<T1> ConvertEachTo<T1>(IReadOnlyCollection<string> list) {
            var to = new List<T1>(list.Count);
            foreach (var item in list) {
                to.Add(item.FromJson<T1>());
            }

            return to;
        }

    }

}
