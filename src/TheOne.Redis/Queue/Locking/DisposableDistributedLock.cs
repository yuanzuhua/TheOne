using System;
using TheOne.Redis.Client;

namespace TheOne.Redis.Queue.Locking {

    /// <summary>
    ///     distributed lock class that follows the Resource Allocation Is Initialization pattern
    /// </summary>
    public class DisposableDistributedLock : IDisposable {

        private readonly string _globalLockKey;
        private readonly long _lockExpire;
        private readonly IRedisClient _myClient;
        private readonly IDistributedLock _myLock;

        /// <summary>
        ///     Lock
        /// </summary>
        /// <param name="client" >client</param>
        /// <param name="globalLockKey" >globalLockKey</param>
        /// <param name="acquisitionTimeout" >in seconds</param>
        /// <param name="lockTimeout" >in seconds</param>
        public DisposableDistributedLock(IRedisClient client, string globalLockKey, int acquisitionTimeout, int lockTimeout) {
            this._myLock = new DistributedLock();
            this._myClient = client;
            this._globalLockKey = globalLockKey;
            this.LockState = this._myLock.Lock(globalLockKey, acquisitionTimeout, lockTimeout, out this._lockExpire, this._myClient);
        }

        public long LockState { get; }

        public long LockExpire => this._lockExpire;

        /// <summary>
        ///     unlock
        /// </summary>
        public void Dispose() {
            this._myLock.Unlock(this._globalLockKey, this._lockExpire, this._myClient);
        }

    }

}
