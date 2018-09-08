using System;

namespace TheOne.Redis.Queue.Locking {

    /// <inheritdoc />
    public class NoLockingStrategy : ILockingStrategy {

        /// <inheritdoc />
        public IDisposable ReadLock() {
            return null;
        }

        /// <inheritdoc />
        public IDisposable WriteLock() {
            return null;
        }

    }

}
