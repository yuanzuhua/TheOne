using System;
using System.Collections.Generic;
using TheOne.Redis.Client;
using TheOne.Redis.ClientManager;
using TheOne.Redis.Pipeline;
using TheOne.Redis.Queue.Locking;

namespace TheOne.Redis.Queue {

    /// <summary>
    ///     distributed work item queue. Each message must have an associated
    ///     work item  id. For a given id, all work items are guaranteed to be processed
    ///     in the order in which they are received.
    /// </summary>
    public partial class RedisSequentialWorkQueue<T> : RedisWorkQueue<T>, ISequentialWorkQueue<T> where T : class {

        protected const double ConvenientlySizedFloat = 18014398509481984.0;
        private const int _numTagsForDequeueLock = RedisNamespace.NumTagsForLockKey + 1;
        private readonly string _dequeueIdSet;
        private readonly int _dequeueLockTimeout;
        private readonly int _lockAcquisitionTimeout = 2;
        private readonly int _lockTimeout = 2;
        private readonly string _workItemIdPriorityQueue;
        private DateTime _harvestTime = DateTime.UtcNow;

        /// <inheritdoc />
        public RedisSequentialWorkQueue(int maxReadPoolSize, int maxWritePoolSize, string host, int port, int dequeueLockTimeout = 300)
            : this(maxReadPoolSize, maxWritePoolSize, host, port, null, dequeueLockTimeout) { }

        /// <inheritdoc />
        public RedisSequentialWorkQueue(int maxReadPoolSize, int maxWritePoolSize, string host, int port, string queueName,
            int dequeueLockTimeout = 300)
            : base(maxReadPoolSize, maxWritePoolSize, host, port, queueName) {
            this._dequeueIdSet = this.QueueNamespace.GlobalCacheKey("DequeueIdSet");
            this._workItemIdPriorityQueue = this.QueueNamespace.GlobalCacheKey("WorkItemIdPriorityQueue");
            this._dequeueLockTimeout = dequeueLockTimeout;
        }

        /// <summary>
        ///     Queue incoming messages
        /// </summary>
        public void Enqueue(string workItemId, T workItem) {
            using (PooledRedisClientManager.DisposablePooledClient<SerializingRedisClient> disposableClient =
                this.ClientManager.GetDisposableClient<SerializingRedisClient>()) {
                SerializingRedisClient client = disposableClient.Client;
                var lockKey = this.QueueNamespace.GlobalLockKey(workItemId);
                using (new DisposableDistributedLock(client, lockKey, this._lockAcquisitionTimeout, this._lockTimeout)) {
                    using (IRedisPipeline pipe = client.CreatePipeline()) {
                        pipe.QueueCommand(r =>
                            ((RedisNativeClient)r).RPush(this.QueueNamespace.GlobalCacheKey(workItemId), client.Serialize(workItem)));
                        pipe.QueueCommand(r =>
                            ((RedisNativeClient)r).ZIncrBy(this._workItemIdPriorityQueue, -1, client.Serialize(workItemId)));
                        pipe.Flush();
                    }
                }
            }
        }

        /// <summary>
        ///     Must call this periodically to move work items from priority queue to pending queue
        /// </summary>
        public bool PrepareNextWorkItem() {
            // harvest zombies every 5 minutes
            DateTime now = DateTime.UtcNow;
            TimeSpan ts = now - this._harvestTime;
            if (ts.TotalMinutes > 5) {
                this.HarvestZombies();
                this._harvestTime = now;
            }

            using (PooledRedisClientManager.DisposablePooledClient<SerializingRedisClient> disposableClient =
                this.ClientManager.GetDisposableClient<SerializingRedisClient>()) {
                SerializingRedisClient client = disposableClient.Client;

                // 1. get next workItemId, or return if there isn't one
                byte[][] smallest = client.ZRangeWithScores(this._workItemIdPriorityQueue, 0, 0);
                if (smallest == null || smallest.Length <= 1 ||
                    RedisNativeClient.ParseDouble(smallest[1]) == ConvenientlySizedFloat) {
                    return false;
                }

                var workItemId = client.Deserialize(smallest[0]) as string;

                // lock work item id
                var lockKey = this.QueueNamespace.GlobalLockKey(workItemId);
                using (new DisposableDistributedLock(client, lockKey, this._lockAcquisitionTimeout, this._lockTimeout)) {
                    // if another client has queued this work item id,
                    // then the work item id score will be set to CONVENIENTLY_SIZED_FLOAT
                    // so we return false in this case
                    var score = client.ZScore(this._workItemIdPriorityQueue, smallest[0]);
                    if (score == ConvenientlySizedFloat) {
                        return false;
                    }

                    using (IRedisPipeline pipe = client.CreatePipeline()) {
                        byte[] rawWorkItemId = client.Serialize(workItemId);

                        // lock work item id in priority queue
                        pipe.QueueCommand(
                            r =>
                                ((RedisNativeClient)r).ZAdd(this._workItemIdPriorityQueue, ConvenientlySizedFloat, smallest[0]));

                        // track dequeue lock id
                        pipe.QueueCommand(r => ((RedisNativeClient)r).SAdd(this._dequeueIdSet, rawWorkItemId));

                        // push into pending set
                        pipe.QueueCommand(r => ((RedisNativeClient)r).LPush(this.PendingWorkItemIdQueue, rawWorkItemId));

                        pipe.Flush();
                    }
                }
            }

            return true;
        }

        /// <inheritdoc />
        public ISequentialData<T> Dequeue(int maxBatchSize) {

            using (PooledRedisClientManager.DisposablePooledClient<SerializingRedisClient> disposableClient =
                this.ClientManager.GetDisposableClient<SerializingRedisClient>()) {
                SerializingRedisClient client = disposableClient.Client;

                // 1. get next workItemId 
                var workItems = new List<T>();
                DequeueManager workItemDequeueManager = null;
                try {
                    byte[] rawWorkItemId = client.RPop(this.PendingWorkItemIdQueue);
                    var workItemId = client.Deserialize(rawWorkItemId) as string;
                    if (rawWorkItemId != null) {
                        using (IRedisPipeline pipe = client.CreatePipeline()) {
                            // dequeue items
                            var key = this.QueueNamespace.GlobalCacheKey(workItemId);

                            void DequeueCallback(byte[] x) {
                                if (x != null) {
                                    workItems.Add((T)client.Deserialize(x));
                                }
                            }

                            for (var i = 0; i < maxBatchSize; ++i) {
                                var index = i;
                                pipe.QueueCommand(r => ((RedisNativeClient)r).LIndex(key, index), DequeueCallback);
                            }

                            pipe.Flush();
                        }

                        workItemDequeueManager = new DequeueManager(this.ClientManager,
                            this,
                            workItemId,
                            this.GlobalDequeueLockKey(workItemId),
                            workItems.Count,
                            this._dequeueLockTimeout);
                        // don't lock if there are no work items to be processed (can't lock on null lock key)
                        if (workItems.Count > 0) {
                            workItemDequeueManager.Lock(this._lockAcquisitionTimeout, client);
                        }

                    }

                    return new SequentialData<T>(workItemId, workItems, workItemDequeueManager);

                } catch {
                    // release resources
                    workItemDequeueManager?.Unlock(client);

                    throw;
                }
            }
        }

        /// <summary>
        ///     Replace existing work item in workItemId queue
        /// </summary>
        public void Update(string workItemId, int index, T newWorkItem) {
            using (PooledRedisClientManager.DisposablePooledClient<SerializingRedisClient> disposableClient =
                this.ClientManager.GetDisposableClient<SerializingRedisClient>()) {
                SerializingRedisClient client = disposableClient.Client;
                var key = this.QueueNamespace.GlobalCacheKey(workItemId);
                client.LSet(key, index, client.Serialize(newWorkItem));
            }
        }

        /// <summary>
        ///     Force release of locks held by crashed servers
        /// </summary>
        public bool HarvestZombies() {
            var rc = false;
            using (PooledRedisClientManager.DisposablePooledClient<SerializingRedisClient> disposableClient =
                this.ClientManager.GetDisposableClient<SerializingRedisClient>()) {
                SerializingRedisClient client = disposableClient.Client;
                byte[][] dequeueWorkItemIds = client.SMembers(this._dequeueIdSet);
                if (dequeueWorkItemIds.Length == 0) {
                    return false;
                }

                var keys = new string[dequeueWorkItemIds.Length];
                for (var i = 0; i < dequeueWorkItemIds.Length; ++i) {
                    keys[i] = this.GlobalDequeueLockKey(client.Deserialize(dequeueWorkItemIds[i]));
                }

                byte[][] dequeueLockVals = client.MGet(keys);

                TimeSpan ts = DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0);
                for (var i = 0; i < dequeueLockVals.Length; ++i) {
                    double lockValue = dequeueLockVals[i] != null ? BitConverter.ToInt64(dequeueLockVals[i], 0) : 0;
                    if (lockValue < ts.TotalSeconds) {
                        rc |= this.TryForceReleaseLock(client, (string)client.Deserialize(dequeueWorkItemIds[i]));
                    }
                }
            }

            return rc;
        }

        /// <summary>
        ///     Pop items from list
        /// </summary>
        private void Pop(string workItemId, int itemCount) {
            if (itemCount <= 0) {
                return;
            }

            using (PooledRedisClientManager.DisposablePooledClient<SerializingRedisClient> disposableClient =
                this.ClientManager.GetDisposableClient<SerializingRedisClient>()) {
                SerializingRedisClient client = disposableClient.Client;
                using (IRedisPipeline pipe = client.CreatePipeline()) {
                    var key = this.QueueNamespace.GlobalCacheKey(workItemId);
                    for (var i = 0; i < itemCount; ++i) {
                        pipe.QueueCommand(r => ((RedisNativeClient)r).LPop(key));
                    }

                    pipe.Flush();
                }
            }
        }

        /// <summary>
        ///     release lock held by crashed server
        /// </summary>
        /// <returns>true if lock is released, either by this method or by another client; false otherwise</returns>
        public bool TryForceReleaseLock(SerializingRedisClient client, string workItemId) {
            if (workItemId == null) {
                return false;
            }

            var rc = false;

            var dequeueLockKey = this.GlobalDequeueLockKey(workItemId);
            // handle possibility of crashed client still holding the lock
            long lockValue = 0;
            using (IRedisPipeline pipe = client.CreatePipeline()) {

                pipe.QueueCommand(r => ((RedisNativeClient)r).Watch(dequeueLockKey));
                pipe.QueueCommand(r => ((RedisNativeClient)r).Get(dequeueLockKey),
                    x => lockValue = x != null ? BitConverter.ToInt64(x, 0) : 0);
                pipe.Flush();
            }

            TimeSpan ts = DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0);
            // no lock to release
            if (lockValue == 0) {
                client.UnWatch();
            }
            // lock still fresh
            else if (lockValue >= ts.TotalSeconds) {
                client.UnWatch();
            } else {
                // lock value is expired; try to release it, and other associated resources
                var len = client.LLen(this.QueueNamespace.GlobalCacheKey(workItemId));
                using (IRedisTransaction trans = client.CreateTransaction()) {
                    // untrack dequeue lock
                    trans.QueueCommand(r => ((RedisNativeClient)r).SRem(this._dequeueIdSet, client.Serialize(workItemId)));

                    // delete dequeue lock
                    trans.QueueCommand(r => ((RedisNativeClient)r).Del(dequeueLockKey));

                    // update priority queue : this will allow other clients to access this workItemId
                    if (len == 0) {
                        trans.QueueCommand(r => ((RedisNativeClient)r).ZRem(this._workItemIdPriorityQueue, client.Serialize(workItemId)));
                    } else {
                        trans.QueueCommand(
                            r => ((RedisNativeClient)r).ZAdd(this._workItemIdPriorityQueue, len, client.Serialize(workItemId)));
                    }

                    rc = trans.Commit();
                }

            }

            return rc;
        }

        /// <summary>
        ///     Unlock work item id, so other servers can process items for this id
        /// </summary>
        private void Unlock(string workItemId) {
            if (workItemId == null) {
                return;
            }

            var key = this.QueueNamespace.GlobalCacheKey(workItemId);
            var lockKey = this.QueueNamespace.GlobalLockKey(workItemId);

            using (PooledRedisClientManager.DisposablePooledClient<SerializingRedisClient> disposableClient =
                this.ClientManager.GetDisposableClient<SerializingRedisClient>()) {
                SerializingRedisClient client = disposableClient.Client;
                using (new DisposableDistributedLock(client, lockKey, this._lockAcquisitionTimeout, this._lockTimeout)) {
                    var len = client.LLen(key);
                    using (IRedisPipeline pipe = client.CreatePipeline()) {
                        // untrack dequeue lock
                        pipe.QueueCommand(r => ((RedisNativeClient)r).SRem(this._dequeueIdSet, client.Serialize(workItemId)));

                        // update priority queue
                        if (len == 0) {
                            pipe.QueueCommand(r =>
                                ((RedisNativeClient)r).ZRem(this._workItemIdPriorityQueue, client.Serialize(workItemId)));
                        } else {
                            pipe.QueueCommand(r =>
                                ((RedisNativeClient)r).ZAdd(this._workItemIdPriorityQueue, len, client.Serialize(workItemId)));
                        }


                        pipe.Flush();
                    }

                }
            }
        }

        private string GlobalDequeueLockKey(object key) {
            return this.QueueNamespace.GlobalKey(key, _numTagsForDequeueLock) + "_DEQUEUE_LOCK";
        }

    }

}
