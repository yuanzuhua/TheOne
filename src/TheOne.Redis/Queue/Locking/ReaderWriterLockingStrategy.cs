using System;
using System.Threading;

namespace TheOne.Redis.Queue.Locking {

    public class ReaderWriterLockingStrategy : ILockingStrategy {

        private readonly ReaderWriterLockSlim _lockObject = new ReaderWriterLockSlim();

        public IDisposable ReadLock() {
            return new ReadLock(this._lockObject);
        }

        public IDisposable WriteLock() {
            return new WriteLock(this._lockObject);
        }

    }

}
