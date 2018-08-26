using System;
using System.Threading;

namespace TheOne.Redis.Queue.Locking {

    /// <summary>
    ///     This class manages a read lock for a local readers/writer lock,
    ///     using the Resource Acquisition Is Initialization pattern
    /// </summary>
    public class ReadLock : IDisposable {

        private readonly ReaderWriterLockSlim _lockObject;

        /// <summary>
        ///     RAII initialization
        /// </summary>
        public ReadLock(ReaderWriterLockSlim lockObject) {
            this._lockObject = lockObject;
            lockObject.EnterReadLock();
        }

        /// <summary>
        ///     RAII disposal
        /// </summary>
        public void Dispose() {
            this._lockObject.ExitReadLock();
        }

    }

}
