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

        public string Id { get; }

        public IEnumerator<KeyValuePair<TKey, T>> GetEnumerator() {
            return this._client.GetAllEntriesFromHash(this).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return this.GetEnumerator();
        }

        public Dictionary<TKey, T> GetAll() {
            return this._client.GetAllEntriesFromHash(this);
        }

        public void Add(KeyValuePair<TKey, T> item) {
            this._client.SetEntryInHash(this, item.Key, item.Value);
        }

        public void Clear() {
            this._client.RemoveEntry(this);
        }

        public bool Contains(KeyValuePair<TKey, T> item) {
            T value = this._client.GetValueFromHash(this, item.Key);
            return !Equals(value, default(T)) && Equals(value, item.Value);
        }

        public void CopyTo(KeyValuePair<TKey, T>[] array, int arrayIndex) {
            Dictionary<TKey, T> allItemsInHash = this._client.GetAllEntriesFromHash(this);

            var i = arrayIndex;
            foreach (KeyValuePair<TKey, T> entry in allItemsInHash) {
                if (i >= array.Length) {
                    return;
                }

                array[i] = entry;
            }
        }

        public bool Remove(KeyValuePair<TKey, T> item) {
            return this.Contains(item) && this._client.RemoveEntryFromHash(this, item.Key);
        }

        public int Count => (int)this._client.GetHashCount(this);

        public bool IsReadOnly => false;

        public bool ContainsKey(TKey key) {
            return this._client.HashContainsEntry(this, key);
        }

        public void Add(TKey key, T value) {
            this._client.SetEntryInHash(this, key, value);
        }

        public bool Remove(TKey key) {
            return this._client.RemoveEntryFromHash(this, key);
        }

        public bool TryGetValue(TKey key, out T value) {
            if (this.ContainsKey(key)) {
                value = this._client.GetValueFromHash(this, key);
                return true;
            }

            value = default;
            return false;
        }

        public T this[TKey key] {
            get => this._client.GetValueFromHash(this, key);
            set => this._client.SetEntryInHash(this, key, value);
        }

        public ICollection<TKey> Keys => this._client.GetHashKeys(this);

        public ICollection<T> Values => this._client.GetHashValues(this);

        public List<TKey> GetAllKeys() {
            return this._client.GetHashKeys(this);
        }

        public List<T> GetAllValues() {
            return this._client.GetHashValues(this);
        }

    }

}
