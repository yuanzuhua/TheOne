using System.Collections.Generic;

namespace TheOne.Redis.Queue {

    public class SequentialData<T> : ISequentialData<T> where T : class {

        private readonly RedisSequentialWorkQueue<T>.DequeueManager _dequeueManager;
        private int _processedCount;

        public SequentialData(string dequeueId, IList<T> dequeueItems, RedisSequentialWorkQueue<T>.DequeueManager dequeueManager) {
            this.DequeueId = dequeueId;
            this.DequeueItems = dequeueItems;
            this._dequeueManager = dequeueManager;
        }

        public IList<T> DequeueItems { get; private set; }

        public string DequeueId { get; }

        /// <summary>
        ///     pop remaining items that were returned by dequeue, and unlock queue
        /// </summary>
        public void PopAndUnlock() {
            if (this.DequeueItems == null || this.DequeueItems.Count <= 0 || this._processedCount >= this.DequeueItems.Count) {
                return;
            }

            this._dequeueManager.PopAndUnlock(this._processedCount);
            this._processedCount = 0;
            this.DequeueItems = null;
        }

        /// <summary>
        ///     indicate that an item has been processed by the caller
        /// </summary>
        public void DoneProcessedWorkItem() {
            if (this._processedCount >= this.DequeueItems.Count) {
                return;
            }

            this._dequeueManager.DoneProcessedWorkItem();
            this._processedCount++;
        }

        /// <summary>
        ///     Update first unprocessed work item
        /// </summary>
        public void UpdateNextUnprocessed(T newWorkItem) {
            this._dequeueManager.UpdateNextUnprocessed(newWorkItem);
        }

    }

}
