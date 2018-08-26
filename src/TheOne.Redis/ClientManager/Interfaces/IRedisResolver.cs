using System;
using System.Collections.Generic;
using TheOne.Redis.Client;

namespace TheOne.Redis.ClientManager {

    /// <summary>
    ///     Resolver strategy for resolving hosts and creating clients
    /// </summary>
    public interface IRedisResolver {

        Func<RedisEndpoint, RedisClient> ClientFactory { get; set; }

        int ReadWriteHostsCount { get; }
        int ReadOnlyHostsCount { get; }

        void ResetMasters(IEnumerable<string> hosts);
        void ResetSlaves(IEnumerable<string> hosts);

        RedisClient CreateMasterClient(int desiredIndex);
        RedisClient CreateSlaveClient(int desiredIndex);

    }

    public interface IRedisResolverExtended : IRedisResolver {

        RedisClient CreateRedisClient(RedisEndpoint config, bool master);

        RedisEndpoint GetReadWriteHost(int desiredIndex);
        RedisEndpoint GetReadOnlyHost(int desiredIndex);

    }

    public interface IHasRedisResolver {

        IRedisResolver RedisResolver { get; set; }

    }

}
