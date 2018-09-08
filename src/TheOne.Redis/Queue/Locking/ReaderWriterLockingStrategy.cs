using System;
using System.Threading;

namespace TheOne.Redis.Queue.Locking {

    /// <inheritdoc />
    public class ReaderWriterLockingStrategy : ILockingStrategy {

        private readonly ReaderWriterLockSlim _lockObject = new ReaderWriterLockSlim();

        /// <inheritdoc />
        public IDisposable ReadLock() {
            return new ReadLock(this._lockObject);
        }

        /// <inheritdoc />
        public IDisposable WriteLock() {
            return new WriteLock(this._lockObject);
        }

    }

}
