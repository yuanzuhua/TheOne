using System;
using System.Collections.Generic;
using System.Linq;
using TheOne.Redis.Client;

namespace TheOne.Redis.ClientManager {

    /// <summary>
    ///     For interoperability GetCacheClient() and GetReadOnlyCacheClient()
    ///     return an ICacheClient wrapper around the redis manager which has the affect of calling
    ///     GetClient() for all write operations and GetReadOnlyClient() for the read ones.
    ///     This works well for master-slave replication scenarios where you have
    ///     1 master that replicates to multiple read slaves.
    /// </summary>
    public class RedisClientManagerCacheClient : IRemoveByPattern, ICacheClientExtended {

        private readonly IRedisClientManager _redisManager;

        /// <inheritdoc />
        public RedisClientManagerCacheClient(IRedisClientManager redisManager) {
            this._redisManager = redisManager;
        }

        public bool ReadOnly { get; set; }

        /// <summary>
        ///     Ignore dispose on RedisClientManager, which should be registered as a singleton
        /// </summary>
        public void Dispose() { }

        /// <inheritdoc />
        public T Get<T>(string key) {
            using (IRedisClient client = this._redisManager.GetReadOnlyClient()) {
                return client.Get<T>(key);
            }
        }

        /// <inheritdoc />
        public IDictionary<string, T> GetAll<T>(IEnumerable<string> keys) {
            using (IRedisClient client = this._redisManager.GetReadOnlyClient()) {
                return client.GetAll<T>(keys);
            }
        }

        /// <inheritdoc />
        public bool Remove(string key) {
            using (ICacheClient client = this.GetClient()) {
                return client.Remove(key);
            }
        }

        /// <inheritdoc />
        public void RemoveAll(IEnumerable<string> keys) {
            using (ICacheClient client = this.GetClient()) {
                client.RemoveAll(keys);
            }
        }

        /// <inheritdoc />
        public long Increment(string key, uint amount) {
            using (ICacheClient client = this.GetClient()) {
                return client.Increment(key, amount);
            }
        }

        /// <inheritdoc />
        public long Decrement(string key, uint amount) {
            using (ICacheClient client = this.GetClient()) {
                return client.Decrement(key, amount);
            }
        }

        /// <inheritdoc />
        public bool Add<T>(string key, T value) {
            using (ICacheClient client = this.GetClient()) {
                return client.Add(key, value);
            }
        }

        /// <inheritdoc />
        public bool Set<T>(string key, T value) {
            using (ICacheClient client = this.GetClient()) {
                return client.Set(key, value);
            }
        }

        /// <inheritdoc />
        public bool Replace<T>(string key, T value) {
            using (ICacheClient client = this.GetClient()) {
                return client.Replace(key, value);
            }
        }

        /// <inheritdoc />
        public bool Add<T>(string key, T value, DateTime expiresAt) {
            using (ICacheClient client = this.GetClient()) {
                return client.Add(key, value, expiresAt);
            }
        }

        /// <inheritdoc />
        public bool Set<T>(string key, T value, DateTime expiresAt) {
            using (ICacheClient client = this.GetClient()) {
                return client.Set(key, value, expiresAt);
            }
        }

        /// <inheritdoc />
        public bool Replace<T>(string key, T value, DateTime expiresAt) {
            using (ICacheClient client = this.GetClient()) {
                return client.Replace(key, value, expiresAt);
            }
        }

        /// <inheritdoc />
        public bool Add<T>(string key, T value, TimeSpan expiresIn) {
            using (ICacheClient client = this.GetClient()) {
                return client.Set(key, value, expiresIn);
            }
        }

        /// <inheritdoc />
        public bool Set<T>(string key, T value, TimeSpan expiresIn) {
            using (ICacheClient client = this.GetClient()) {
                return client.Set(key, value, expiresIn);
            }
        }

        /// <inheritdoc />
        public bool Replace<T>(string key, T value, TimeSpan expiresIn) {
            using (ICacheClient client = this.GetClient()) {
                return client.Replace(key, value, expiresIn);
            }
        }

        /// <inheritdoc />
        public void FlushAll() {
            using (ICacheClient client = this.GetClient()) {
                client.FlushAll();
            }
        }

        /// <inheritdoc />
        public void SetAll<T>(IDictionary<string, T> values) {
            using (ICacheClient client = this.GetClient()) {
                client.SetAll(values);
            }
        }

        /// <inheritdoc />
        public TimeSpan? GetTimeToLive(string key) {
            using (ICacheClient client = this.GetClient()) {
                if (client is ICacheClientExtended redisClient) {
                    return redisClient.GetTimeToLive(key);
                }
            }

            return null;
        }

        /// <inheritdoc />
        public IEnumerable<string> GetKeysByPattern(string pattern) {
            using (var client = (ICacheClientExtended)this.GetClient()) {
                return client.GetKeysByPattern(pattern).ToList();
            }
        }

        /// <inheritdoc />
        public void RemoveByPattern(string pattern) {
            using (ICacheClient client = this.GetClient()) {
                if (client is IRemoveByPattern redisClient) {
                    redisClient.RemoveByPattern(pattern);
                }
            }
        }

        /// <inheritdoc />
        public void RemoveByRegex(string pattern) {
            using (ICacheClient client = this.GetClient()) {
                if (client is IRemoveByPattern redisClient) {
                    redisClient.RemoveByRegex(pattern);
                }
            }
        }

        private void AssertNotReadOnly() {
            if (this.ReadOnly) {
                throw new InvalidOperationException("Cannot perform write operations on a Read-only client");
            }
        }

        public ICacheClient GetClient() {
            this.AssertNotReadOnly();
            return this._redisManager.GetClient();
        }

    }

}
