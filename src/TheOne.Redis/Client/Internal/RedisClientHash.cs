using System.Collections;
using System.Collections.Generic;

namespace TheOne.Redis.Client.Internal {

    /// <summary>
    ///     Wrap the common redis set operations under a ICollection[string] interface.
    /// </summary>
    internal class RedisClientHash : IRedisHash {

        private readonly RedisClient _client;

        public RedisClientHash(RedisClient client, string hashId) {
            this._client = client;
            this.Id = hashId;
        }

        /// <inheritdoc />
        public IEnumerator<KeyValuePair<string, string>> GetEnumerator() {
            return this._client.GetAllEntriesFromHash(this.Id).GetEnumerator();
        }

        /// <inheritdoc />
        IEnumerator IEnumerable.GetEnumerator() {
            return this.GetEnumerator();
        }

        /// <inheritdoc />
        public void Add(KeyValuePair<string, string> item) {
            this._client.SetEntryInHash(this.Id, item.Key, item.Value);
        }

        /// <inheritdoc />
        public bool AddIfNotExists(KeyValuePair<string, string> item) {
            return this._client.SetEntryInHashIfNotExists(this.Id, item.Key, item.Value);
        }

        /// <inheritdoc />
        public void AddRange(IEnumerable<KeyValuePair<string, string>> items) {
            this._client.SetRangeInHash(this.Id, items);
        }

        /// <inheritdoc />
        public long IncrementValue(string key, int incrementBy) {
            return this._client.IncrementValueInHash(this.Id, key, incrementBy);
        }

        /// <inheritdoc />
        public void Clear() {
            this._client.Remove(this.Id);
        }

        /// <inheritdoc />
        public bool Contains(KeyValuePair<string, string> item) {
            var itemValue = this._client.GetValueFromHash(this.Id, item.Key);
            return itemValue == item.Value;
        }

        /// <inheritdoc />
        public void CopyTo(KeyValuePair<string, string>[] array, int arrayIndex) {
            Dictionary<string, string> allItemsInHash = this._client.GetAllEntriesFromHash(this.Id);

            var i = arrayIndex;
            foreach (KeyValuePair<string, string> item in allItemsInHash) {
                if (i >= array.Length) {
                    return;
                }

                array[i++] = item;
            }
        }

        /// <inheritdoc />
        public bool Remove(KeyValuePair<string, string> item) {
            if (this.Contains(item)) {
                this._client.RemoveEntryFromHash(this.Id, item.Key);
                return true;
            }

            return false;
        }

        /// <inheritdoc />
        public int Count => (int)this._client.GetHashCount(this.Id);

        /// <inheritdoc />
        public bool IsReadOnly => false;

        /// <inheritdoc />
        public bool ContainsKey(string key) {
            return this._client.HashContainsEntry(this.Id, key);
        }

        /// <inheritdoc />
        public void Add(string key, string value) {
            this._client.SetEntryInHash(this.Id, key, value);
        }

        /// <inheritdoc />
        public bool Remove(string key) {
            return this._client.RemoveEntryFromHash(this.Id, key);
        }

        /// <inheritdoc />
        public bool TryGetValue(string key, out string value) {
            value = this._client.GetValueFromHash(this.Id, key);
            return value != null;
        }

        /// <inheritdoc />
        public string this[string key] {
            get => this._client.GetValueFromHash(this.Id, key);
            set => this._client.SetEntryInHash(this.Id, key, value);
        }

        /// <inheritdoc />
        public ICollection<string> Keys => this._client.GetHashKeys(this.Id);

        /// <inheritdoc />
        public ICollection<string> Values => this._client.GetHashValues(this.Id);

        /// <inheritdoc />
        public string Id { get; }

    }

}
