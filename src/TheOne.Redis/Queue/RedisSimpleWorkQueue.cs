using System.Collections.Generic;
using TheOne.Redis.Client;
using TheOne.Redis.ClientManager;
using TheOne.Redis.Pipeline;

namespace TheOne.Redis.Queue {

    /// <summary>
    ///     simple distributed work item queue
    /// </summary>
    public class RedisSimpleWorkQueue<T> : RedisWorkQueue<T>, ISimpleWorkQueue<T> where T : class {

        /// <inheritdoc />
        public RedisSimpleWorkQueue(int maxReadPoolSize, int maxWritePoolSize, string host, int port)
            : base(maxReadPoolSize, maxWritePoolSize, host, port) { }

        /// <inheritdoc />
        public RedisSimpleWorkQueue(int maxReadPoolSize, int maxWritePoolSize, string host, int port, string queueName)
            : base(maxReadPoolSize, maxWritePoolSize, host, port, queueName) { }

        /// <summary>
        ///     Queue incoming messages
        /// </summary>
        public void Enqueue(T msg) {
            var key = this.QueueNamespace.GlobalCacheKey(this.PendingWorkItemIdQueue);
            using (PooledRedisClientManager.DisposablePooledClient<SerializingRedisClient> disposableClient =
                this.ClientManager.GetDisposableClient<SerializingRedisClient>()) {
                SerializingRedisClient client = disposableClient.Client;
                client.RPush(key, client.Serialize(msg));
            }
        }


        /// <summary>
        ///     Dequeue next batch of work items for processing. After this method is called,
        ///     no other work items with same id will be available for
        ///     dequeuing until PostDequeue is called
        /// </summary>
        /// <returns>
        ///     KeyValuePair: key is work item id, and value is list of dequeued items.
        /// </returns>
        public IList<T> Dequeue(int maxBatchSize) {
            using (PooledRedisClientManager.DisposablePooledClient<SerializingRedisClient> disposableClient =
                this.ClientManager.GetDisposableClient<SerializingRedisClient>()) {
                SerializingRedisClient client = disposableClient.Client;
                var dequeueItems = new List<T>();
                using (IRedisPipeline pipe = client.CreatePipeline()) {
                    var key = this.QueueNamespace.GlobalCacheKey(this.PendingWorkItemIdQueue);
                    for (var i = 0; i < maxBatchSize; ++i) {
                        pipe.QueueCommand(
                            r => ((RedisNativeClient)r).LPop(key),
                            x => {
                                if (x != null) {
                                    dequeueItems.Add((T)client.Deserialize(x));
                                }
                            });

                    }

                    pipe.Flush();

                }

                return dequeueItems;
            }
        }

    }

}
