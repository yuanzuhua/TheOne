using System;

namespace TheOne.Redis.Queue.Locking {

    /// <summary>
    ///     Locking strategy interface
    /// </summary>
    public interface ILockingStrategy {

        IDisposable ReadLock();

        IDisposable WriteLock();

    }

}
