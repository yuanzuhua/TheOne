using System.Collections;
using System.Collections.Generic;

namespace TheOne.Redis.Client.Internal {

    /// <summary>
    ///     Wrap the common redis set operations under a ICollection[string] interface.
    /// </summary>
    internal class RedisClientHash<TKey, T> : IRedisHash<TKey, T> {

        private readonly RedisTypedClient<T> _client;

        public RedisClientHash(RedisTypedClient<T> client, string hashId) {
            this._client = client;
            this.Id = hashId;
        }

        /// <inheritdoc />
        public string Id { get; }

        /// <inheritdoc />
        public IEnumerator<KeyValuePair<TKey, T>> GetEnumerator() {
            return this._client.GetAllEntriesFromHash(this).GetEnumerator();
        }

        /// <inheritdoc />
        IEnumerator IEnumerable.GetEnumerator() {
            return this.GetEnumerator();
        }

        /// <inheritdoc />
        public Dictionary<TKey, T> GetAll() {
            return this._client.GetAllEntriesFromHash(this);
        }

        /// <inheritdoc />
        public void Add(KeyValuePair<TKey, T> item) {
            this._client.SetEntryInHash(this, item.Key, item.Value);
        }

        /// <inheritdoc />
        public void Clear() {
            this._client.RemoveEntry(this);
        }

        /// <inheritdoc />
        public bool Contains(KeyValuePair<TKey, T> item) {
            var value = this._client.GetValueFromHash(this, item.Key);
            return !Equals(value, default(T)) && Equals(value, item.Value);
        }

        /// <inheritdoc />
        public void CopyTo(KeyValuePair<TKey, T>[] array, int arrayIndex) {
            var allItemsInHash = this._client.GetAllEntriesFromHash(this);

            var i = arrayIndex;
            foreach (var entry in allItemsInHash) {
                if (i >= array.Length) {
                    return;
                }

                array[i] = entry;
            }
        }

        /// <inheritdoc />
        public bool Remove(KeyValuePair<TKey, T> item) {
            return this.Contains(item) && this._client.RemoveEntryFromHash(this, item.Key);
        }

        /// <inheritdoc />
        public int Count => (int)this._client.GetHashCount(this);

        /// <inheritdoc />
        public bool IsReadOnly => false;

        /// <inheritdoc />
        public bool ContainsKey(TKey key) {
            return this._client.HashContainsEntry(this, key);
        }

        /// <inheritdoc />
        public void Add(TKey key, T value) {
            this._client.SetEntryInHash(this, key, value);
        }

        /// <inheritdoc />
        public bool Remove(TKey key) {
            return this._client.RemoveEntryFromHash(this, key);
        }

        /// <inheritdoc />
        public bool TryGetValue(TKey key, out T value) {
            if (this.ContainsKey(key)) {
                value = this._client.GetValueFromHash(this, key);
                return true;
            }

            value = default;
            return false;
        }

        /// <inheritdoc />
        public T this[TKey key] {
            get => this._client.GetValueFromHash(this, key);
            set => this._client.SetEntryInHash(this, key, value);
        }

        /// <inheritdoc />
        public ICollection<TKey> Keys => this._client.GetHashKeys(this);

        /// <inheritdoc />
        public ICollection<T> Values => this._client.GetHashValues(this);

        public List<TKey> GetAllKeys() {
            return this._client.GetHashKeys(this);
        }

        public List<T> GetAllValues() {
            return this._client.GetHashValues(this);
        }

    }

}
