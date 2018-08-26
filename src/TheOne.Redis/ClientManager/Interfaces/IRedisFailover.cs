using System;
using System.Collections.Generic;

namespace TheOne.Redis.ClientManager {

    public interface IRedisFailover {

        List<Action<IRedisClientManager>> OnFailover { get; }

        void FailoverTo(params string[] readWriteHosts);

        void FailoverTo(IEnumerable<string> readWriteHosts, IEnumerable<string> readOnlyHosts);

    }

}
