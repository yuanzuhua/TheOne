using System;
using System.Collections.Generic;
using TheOne.Redis.Client;

namespace TheOne.Redis.ClientManager {

    public partial class BasicRedisClientManager {

        /// <inheritdoc />
        public ICacheClient GetCacheClient() {
            return new RedisClientManagerCacheClient(this);
        }

        /// <inheritdoc />
        public ICacheClient GetReadOnlyCacheClient() {
            return this.ConfigureRedisClient(this.GetReadOnlyClient());
        }

        private ICacheClient ConfigureRedisClient(IRedisClient client) {
            return client;
        }

        #region Implementation of ICacheClient

        /// <inheritdoc />
        public bool Remove(string key) {
            using (var client = this.GetReadOnlyCacheClient()) {
                return client.Remove(key);
            }
        }

        /// <inheritdoc />
        public void RemoveAll(IEnumerable<string> keys) {
            using (var client = this.GetCacheClient()) {
                client.RemoveAll(keys);
            }
        }

        /// <inheritdoc />
        public T Get<T>(string key) {
            using (var client = this.GetReadOnlyCacheClient()) {
                return client.Get<T>(key);
            }
        }

        /// <inheritdoc />
        public long Increment(string key, uint amount) {
            using (var client = this.GetCacheClient()) {
                return client.Increment(key, amount);
            }
        }

        /// <inheritdoc />
        public long Decrement(string key, uint amount) {
            using (var client = this.GetCacheClient()) {
                return client.Decrement(key, amount);
            }
        }

        /// <inheritdoc />
        public bool Add<T>(string key, T value) {
            using (var client = this.GetCacheClient()) {
                return client.Add(key, value);
            }
        }

        /// <inheritdoc />
        public bool Set<T>(string key, T value) {
            using (var client = this.GetCacheClient()) {
                return client.Set(key, value);
            }
        }

        /// <inheritdoc />
        public bool Replace<T>(string key, T value) {
            using (var client = this.GetCacheClient()) {
                return client.Replace(key, value);
            }
        }

        /// <inheritdoc />
        public bool Add<T>(string key, T value, DateTime expiresAt) {
            using (var client = this.GetCacheClient()) {
                return client.Add(key, value, expiresAt);
            }
        }

        /// <inheritdoc />
        public bool Set<T>(string key, T value, DateTime expiresAt) {
            using (var client = this.GetCacheClient()) {
                return client.Set(key, value, expiresAt);
            }
        }

        /// <inheritdoc />
        public bool Replace<T>(string key, T value, DateTime expiresAt) {
            using (var client = this.GetCacheClient()) {
                return client.Replace(key, value, expiresAt);
            }
        }

        /// <inheritdoc />
        public bool Add<T>(string key, T value, TimeSpan expiresIn) {
            using (var client = this.GetCacheClient()) {
                return client.Add(key, value, expiresIn);
            }
        }

        /// <inheritdoc />
        public bool Set<T>(string key, T value, TimeSpan expiresIn) {
            using (var client = this.GetCacheClient()) {
                return client.Set(key, value, expiresIn);
            }
        }

        /// <inheritdoc />
        public bool Replace<T>(string key, T value, TimeSpan expiresIn) {
            using (var client = this.GetCacheClient()) {
                return client.Replace(key, value, expiresIn);
            }
        }

        /// <inheritdoc />
        public void FlushAll() {
            using (var client = this.GetCacheClient()) {
                client.FlushAll();
            }
        }

        /// <inheritdoc />
        public IDictionary<string, T> GetAll<T>(IEnumerable<string> keys) {
            using (var client = this.GetReadOnlyCacheClient()) {
                return client.GetAll<T>(keys);
            }
        }

        #endregion

    }

}
