using System.Collections.Generic;
using System.Linq;
using TheOne.Redis.Client.Internal;
using TheOne.Redis.Common;

namespace TheOne.Redis.Client {

    public partial class RedisClient {

        public IHasNamed<IRedisHash> Hashes { get; set; }

        public bool SetEntryInHash(string hashId, string key, string value) {
            return this.HSet(hashId, key.ToUtf8Bytes(), value.ToUtf8Bytes()) == Success;
        }

        public bool SetEntryInHashIfNotExists(string hashId, string key, string value) {
            return this.HSetNX(hashId, key.ToUtf8Bytes(), value.ToUtf8Bytes()) == Success;
        }

        public void SetRangeInHash(string hashId, IEnumerable<KeyValuePair<string, string>> keyValuePairs) {
            List<KeyValuePair<string, string>> keyValuePairsList = keyValuePairs.ToList();
            if (keyValuePairsList.Count == 0) {
                return;
            }

            var keys = new byte[keyValuePairsList.Count][];
            var values = new byte[keyValuePairsList.Count][];

            for (var i = 0; i < keyValuePairsList.Count; i++) {
                KeyValuePair<string, string> kvp = keyValuePairsList[i];
                keys[i] = kvp.Key.ToUtf8Bytes();
                values[i] = kvp.Value.ToUtf8Bytes();
            }

            this.HMSet(hashId, keys, values);
        }

        public long IncrementValueInHash(string hashId, string key, int incrementBy) {
            return this.HIncrby(hashId, key.ToUtf8Bytes(), incrementBy);
        }

        public double IncrementValueInHash(string hashId, string key, double incrementBy) {
            return this.HIncrbyFloat(hashId, key.ToUtf8Bytes(), incrementBy);
        }

        public string GetValueFromHash(string hashId, string key) {
            return this.HGet(hashId, key.ToUtf8Bytes()).FromUtf8Bytes();
        }

        public bool HashContainsEntry(string hashId, string key) {
            return this.HExists(hashId, key.ToUtf8Bytes()) == Success;
        }

        public bool RemoveEntryFromHash(string hashId, string key) {
            return this.HDel(hashId, key.ToUtf8Bytes()) == Success;
        }

        public long GetHashCount(string hashId) {
            return this.HLen(hashId);
        }

        public List<string> GetHashKeys(string hashId) {
            byte[][] multiDataList = this.HKeys(hashId);
            return multiDataList.ToStringList();
        }

        public List<string> GetHashValues(string hashId) {
            byte[][] multiDataList = this.HVals(hashId);
            return multiDataList.ToStringList();
        }

        public Dictionary<string, string> GetAllEntriesFromHash(string hashId) {
            byte[][] multiDataList = this.HGetAll(hashId);
            return multiDataList.ToStringDictionary();
        }

        public List<string> GetValuesFromHash(string hashId, params string[] keys) {
            if (keys.Length == 0) {
                return new List<string>();
            }

            byte[][] keyBytes = this.ConvertToBytes(keys);
            byte[][] multiDataList = this.HMGet(hashId, keyBytes);
            return multiDataList.ToStringList();
        }

        public long IncrementValueInHash(string hashId, string key, long incrementBy) {
            return this.HIncrby(hashId, key.ToUtf8Bytes(), incrementBy);
        }

        internal class RedisClientHashes : IHasNamed<IRedisHash> {

            private readonly RedisClient _client;

            public RedisClientHashes(RedisClient client) {
                this._client = client;
            }

            public IRedisHash this[string hashId] {
                get => new RedisClientHash(this._client, hashId);
                set {
                    IRedisHash hash = this[hashId];
                    hash.Clear();
                    hash.CopyTo(value.ToArray(), 0);
                }
            }

        }

    }

}
