using TheOne.Redis.Client;
using TheOne.Redis.ClientManager;
using TheOne.Redis.Queue.Locking;

namespace TheOne.Redis.Queue {

    public partial class RedisSequentialWorkQueue<T> {

        public class DequeueManager {

            private readonly string _dequeueLockKey;
            private readonly int _dequeueLockTimeout;
            private readonly DistributedLock _myLock;

            protected readonly PooledRedisClientManager ClientManager;
            protected readonly int NumberOfDequeuedItems;
            protected readonly string WorkItemId;
            protected readonly RedisSequentialWorkQueue<T> WorkQueue;
            private long _lockExpire;
            protected int NumberOfProcessedItems;

            public DequeueManager(PooledRedisClientManager clientManager, RedisSequentialWorkQueue<T> workQueue, string workItemId,
                string dequeueLockKey, int numberOfDequeuedItems, int dequeueLockTimeout = 300) {
                this.WorkQueue = workQueue;
                this.WorkItemId = workItemId;
                this.ClientManager = clientManager;
                this.NumberOfDequeuedItems = numberOfDequeuedItems;
                this._myLock = new DistributedLock();
                this._dequeueLockKey = dequeueLockKey;
                this._dequeueLockTimeout = dequeueLockTimeout;
            }

            public void DoneProcessedWorkItem() {
                this.NumberOfProcessedItems++;
                if (this.NumberOfProcessedItems == this.NumberOfDequeuedItems) {
                    using (var disposable =
                        new PooledRedisClientManager.DisposablePooledClient<SerializingRedisClient>(this.ClientManager)) {
                        this.Unlock(disposable.Client);
                    }
                }
            }

            public void UpdateNextUnprocessed(T newWorkItem) {
                this.WorkQueue.Update(this.WorkItemId, this.NumberOfProcessedItems, newWorkItem);
            }

            public long Lock(int acquisitionTimeout, IRedisClient client) {
                return this._myLock.Lock(this._dequeueLockKey, acquisitionTimeout, this._dequeueLockTimeout, out this._lockExpire, client);
            }

            public bool Unlock(IRedisClient client) {
                return this.PopAndUnlock(this.NumberOfDequeuedItems, client);
            }

            public bool PopAndUnlock(int numProcessed, IRedisClient client) {
                if (numProcessed < 0) {
                    numProcessed = 0;
                }

                if (numProcessed > this.NumberOfDequeuedItems) {
                    numProcessed = this.NumberOfDequeuedItems;
                }

                // remove items from queue
                this.WorkQueue.Pop(this.WorkItemId, numProcessed);

                // unlock work queue id
                this.WorkQueue.Unlock(this.WorkItemId);
                return this._myLock.Unlock(this._dequeueLockKey, this._lockExpire, client);
            }

            public bool PopAndUnlock(int numProcessed) {
                using (var disposable = new PooledRedisClientManager.DisposablePooledClient<SerializingRedisClient>(this.ClientManager)) {
                    return this.PopAndUnlock(numProcessed, disposable.Client);
                }
            }

        }

    }

}
