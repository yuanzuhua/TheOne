using System;

namespace TheOne.Redis.Queue {

    public interface ISequentialWorkQueue<T> : IDisposable where T : class {

        /// <summary>
        ///     Enqueue item in priority queue corresponding to workItemId identifier
        /// </summary>
        void Enqueue(string workItemId, T workItem);

        /// <summary>
        ///     Preprare next work item id for dequeueing
        /// </summary>
        bool PrepareNextWorkItem();


        /// <summary>
        ///     Dequeue up to maxBatchSize items from queue corresponding to workItemId identifier.
        ///     Once this method is called, <see cref="Dequeue" /> will not
        ///     return any items for workItemId until the dequeue lock returned is unlocked.
        /// </summary>
        /// <param name="maxBatchSize" ></param>
        ISequentialData<T> Dequeue(int maxBatchSize);


        /// <summary>
        ///     Replace existing work item in workItemId queue
        /// </summary>
        void Update(string workItemId, int index, T newWorkItem);

        bool HarvestZombies();

    }

}
