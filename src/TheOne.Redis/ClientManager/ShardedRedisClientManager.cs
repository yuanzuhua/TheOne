using System;
using System.Collections.Generic;
using TheOne.Redis.ClientManager.Internal;

namespace TheOne.Redis.ClientManager {

    /// <summary>
    ///     Provides sharding of redis client connections.
    ///     uses consistent hashing to distribute keys across connection pools
    /// </summary>
    public class ShardedRedisClientManager {

        private readonly ConsistentHash<ShardedConnectionPool> _consistentHash;

        public ShardedRedisClientManager(params ShardedConnectionPool[] connectionPools) {
            if (connectionPools == null) {
                throw new ArgumentNullException("connection pools can not be null.");
            }

            var pools = new List<KeyValuePair<ShardedConnectionPool, int>>();
            foreach (ShardedConnectionPool connectionPool in connectionPools) {
                pools.Add(new KeyValuePair<ShardedConnectionPool, int>(connectionPool, connectionPool.Weight));
            }

            this._consistentHash = new ConsistentHash<ShardedConnectionPool>(pools);
        }

        /// <summary>
        ///     maps a key to a redis connection pool
        /// </summary>
        /// <param name="key" >key to map</param>
        /// <returns>a redis connection pool</returns>
        public ShardedConnectionPool GetConnectionPool(string key) {
            return this._consistentHash.GetTarget(key);
        }

    }

}
