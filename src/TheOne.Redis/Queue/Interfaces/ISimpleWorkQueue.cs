using System;
using System.Collections.Generic;

namespace TheOne.Redis.Queue {

    public interface ISimpleWorkQueue<T> : IDisposable where T : class {

        /// <summary>
        ///     Enqueue item
        /// </summary>
        void Enqueue(T workItem);

        /// <summary>
        ///     Dequeue up to maxBatchSize items from queue
        /// </summary>
        IList<T> Dequeue(int maxBatchSize);

    }

}
