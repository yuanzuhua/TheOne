using System;
using TheOne.Redis.ClientManager;

namespace TheOne.Redis.Queue {

    /// <summary>
    ///     distributed work item queue
    /// </summary>
    public class RedisWorkQueue<T> {

        protected readonly PooledRedisClientManager ClientManager;
        protected readonly string PendingWorkItemIdQueue;
        protected readonly RedisNamespace QueueNamespace;
        protected readonly string WorkQueue;

        public RedisWorkQueue(int maxReadPoolSize, int maxWritePoolSize, string host, int port, string queueName = null) {
            var qname = queueName ?? typeof(T) + "_Shared_Work_Queue";
            this.QueueNamespace = new RedisNamespace(qname);
            this.PendingWorkItemIdQueue = this.QueueNamespace.GlobalCacheKey("PendingWorkItemIdQueue");
            this.WorkQueue = this.QueueNamespace.GlobalCacheKey("WorkQueue");

            var poolConfig = new RedisClientManagerConfig {
                MaxReadPoolSize = maxReadPoolSize,
                MaxWritePoolSize = maxWritePoolSize
            };

            this.ClientManager = new PooledRedisClientManager(new[] { host + ":" + port },
                Array.Empty<string>(),
                poolConfig) {
                RedisResolver = { ClientFactory = config => new SerializingRedisClient(config) }
            };
        }

        public void Dispose() {
            this.ClientManager.Dispose();
        }

    }

}
