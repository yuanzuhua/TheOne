using System;

namespace TheOne.Redis.Queue.Locking {

    public class NoLockingStrategy : ILockingStrategy {

        public IDisposable ReadLock() {
            return null;
        }

        public IDisposable WriteLock() {
            return null;
        }

    }

}
