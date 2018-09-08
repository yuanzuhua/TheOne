using System.Collections.Generic;
using TheOne.Redis.Client;
using TheOne.Redis.ClientManager;
using TheOne.Redis.Pipeline;

namespace TheOne.Redis.Queue {

    /// <summary>
    ///     distributed work item queue. Messages are processed in chronological order
    /// </summary>
    public class RedisChronologicalWorkQueue<T> : RedisWorkQueue<T>, IChronologicalWorkQueue<T> where T : class {

        /// <inheritdoc />
        public RedisChronologicalWorkQueue(int maxReadPoolSize, int maxWritePoolSize, string host, int port, string queueName = null)
            : base(maxReadPoolSize, maxWritePoolSize, host, port, queueName) { }

        /// <summary>
        ///     Enqueue incoming messages
        /// </summary>
        public void Enqueue(string workItemId, T workItem, double time) {
            using (PooledRedisClientManager.DisposablePooledClient<SerializingRedisClient> disposableClient =
                this.ClientManager.GetDisposableClient<SerializingRedisClient>()) {
                SerializingRedisClient client = disposableClient.Client;
                byte[] workItemIdRaw = client.Serialize(workItemId);
                using (IRedisPipeline pipe = client.CreatePipeline()) {
                    pipe.QueueCommand(r => ((RedisNativeClient)r).HSet(this.WorkQueue, workItemIdRaw, client.Serialize(workItem)));
                    pipe.QueueCommand(r => ((RedisNativeClient)r).ZAdd(this.PendingWorkItemIdQueue, time, workItemIdRaw));
                    pipe.Flush();
                }
            }
        }


        /// <summary>
        ///     Dequeue next batch of work items
        /// </summary>
        public IList<KeyValuePair<string, T>> Dequeue(double minTime, double maxTime, int maxBatchSize) {
            using (PooledRedisClientManager.DisposablePooledClient<SerializingRedisClient> disposableClient =
                this.ClientManager.GetDisposableClient<SerializingRedisClient>()) {
                SerializingRedisClient client = disposableClient.Client;

                // 1. get next work item batch 
                var dequeueItems = new List<KeyValuePair<string, T>>();
                byte[][] itemIds = client.ZRangeByScore(this.PendingWorkItemIdQueue, minTime, maxTime, null, maxBatchSize);
                if (itemIds != null && itemIds.Length > 0) {

                    byte[][] rawItems = client.HMGet(this.WorkQueue, itemIds);
                    IList<byte[]> toDelete = new List<byte[]>();
                    for (var i = 0; i < itemIds.Length; ++i) {
                        dequeueItems.Add(new KeyValuePair<string, T>(client.Deserialize(itemIds[i]) as string,
                            client.Deserialize(rawItems[i]) as T));
                        toDelete.Add(itemIds[i]);
                    }

                    // delete batch of keys
                    using (IRedisPipeline pipe = client.CreatePipeline()) {
                        foreach (byte[] rawId in toDelete) {
                            byte[] myRawId = rawId;
                            pipe.QueueCommand(r => ((RedisNativeClient)r).HDel(this.WorkQueue, myRawId));
                            pipe.QueueCommand(r => ((RedisNativeClient)r).ZRem(this.PendingWorkItemIdQueue, myRawId));
                        }

                        pipe.Flush();
                    }
                }

                return dequeueItems;
            }
        }

    }

}
