using System;
using TheOne.Redis.ClientManager;

namespace TheOne.Redis.Sentinel {

    public interface IRedisSentinel : IDisposable {

        IRedisClientManager Start();

    }

}
