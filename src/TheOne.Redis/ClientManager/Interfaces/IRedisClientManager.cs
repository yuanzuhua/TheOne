using System;
using TheOne.Redis.Client;

namespace TheOne.Redis.ClientManager {

    public interface IRedisClientManager : IDisposable {

        /// <summary>
        ///     Returns a Read/Write client (The default) using the hosts defined in ReadWriteHosts
        /// </summary>
        IRedisClient GetClient();

        /// <summary>
        ///     Returns a ReadOnly client using the hosts defined in ReadOnlyHosts.
        /// </summary>
        IRedisClient GetReadOnlyClient();

        /// <summary>
        ///     Returns a Read/Write ICacheClient (The default) using the hosts defined in ReadWriteHosts
        /// </summary>
        ICacheClient GetCacheClient();

        /// <summary>
        ///     Returns a ReadOnly ICacheClient using the hosts defined in ReadOnlyHosts.
        /// </summary>
        ICacheClient GetReadOnlyCacheClient();

    }

}
