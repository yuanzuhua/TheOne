using System.Collections.Generic;
using TheOne.Redis.Client;

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
            using (var disposableClient =
                this.ClientManager.GetDisposableClient<SerializingRedisClient>()) {
                var client = disposableClient.Client;
                var workItemIdRaw = client.Serialize(workItemId);
                using (var pipe = client.CreatePipeline()) {
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
            using (var disposableClient =
                this.ClientManager.GetDisposableClient<SerializingRedisClient>()) {
                var client = disposableClient.Client;

                // 1. get next work item batch 
                var dequeueItems = new List<KeyValuePair<string, T>>();
                var itemIds = client.ZRangeByScore(this.PendingWorkItemIdQueue, minTime, maxTime, null, maxBatchSize);
                if (itemIds != null && itemIds.Length > 0) {

                    var rawItems = client.HMGet(this.WorkQueue, itemIds);
                    IList<byte[]> toDelete = new List<byte[]>();
                    for (var i = 0; i < itemIds.Length; ++i) {
                        dequeueItems.Add(new KeyValuePair<string, T>(client.Deserialize(itemIds[i]) as string,
                            client.Deserialize(rawItems[i]) as T));
                        toDelete.Add(itemIds[i]);
                    }

                    // delete batch of keys
                    using (var pipe = client.CreatePipeline()) {
                        foreach (var rawId in toDelete) {
                            var myRawId = rawId;
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
