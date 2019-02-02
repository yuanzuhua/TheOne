using System;
using System.Threading;
using TheOne.Logging;
using TheOne.Redis.Client;

namespace TheOne.Redis.Queue.Locking {

    public class DistributedLock : IDistributedLock {

        public const int LockNotAcquired = 0;
        public const int LockAcquired = 1;
        public const int LockRecovered = 2;

        private static readonly ILog _logger = LogProvider.GetCurrentClassLogger();

        /// <summary>
        ///     acquire distributed, non-reentrant lock on key
        /// </summary>
        /// <param name="key" >global key for this lock</param>
        /// <param name="acquisitionTimeout" >timeout for acquiring lock</param>
        /// <param name="lockTimeout" >timeout for lock, in seconds (stored as value against lock key) </param>
        /// <param name="client" >client</param>
        /// <param name="lockExpire" >lockExpire</param>
        public virtual long Lock(string key, int acquisitionTimeout, int lockTimeout, out long lockExpire, IRedisClient client) {
            lockExpire = 0;

            // cannot lock on a null key
            if (key == null) {
                return LockNotAcquired;
            }

            const int sleepIfLockSet = 200;
            acquisitionTimeout *= 1000; // convert to ms
            var tryCount = acquisitionTimeout / sleepIfLockSet + 1;

            var ts = DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0);
            var newLockExpire = CalculateLockExpire(ts, lockTimeout);

            var localClient = (RedisClient)client;
            var wasSet = localClient.SetNX(key, BitConverter.GetBytes(newLockExpire));
            var totalTime = 0;
            while (wasSet == LockNotAcquired && totalTime < acquisitionTimeout) {
                var count = 0;
                while (wasSet == 0 && count < tryCount && totalTime < acquisitionTimeout) {
                    Thread.Sleep(sleepIfLockSet);
                    totalTime += sleepIfLockSet;
                    ts = DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0);
                    newLockExpire = CalculateLockExpire(ts, lockTimeout);
                    wasSet = localClient.SetNX(key, BitConverter.GetBytes(newLockExpire));
                    count++;
                }

                // acquired lock!
                if (wasSet != LockNotAcquired) {
                    break;
                }

                // handle possibility of crashed client still holding the lock
                using (var pipe = localClient.CreatePipeline()) {
                    long lockValue = 0;
                    pipe.QueueCommand(r => ((RedisNativeClient)r).Watch(key));
                    pipe.QueueCommand(r => ((RedisNativeClient)r).Get(key), x => lockValue = x != null ? BitConverter.ToInt64(x, 0) : 0);
                    pipe.Flush();

                    // if lock value is 0 (key is empty), or expired, then we can try to acquire it
                    ts = DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0);
                    if (lockValue < ts.TotalSeconds) {
                        ts = DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0);
                        newLockExpire = CalculateLockExpire(ts, lockTimeout);
                        using (var trans = localClient.CreateTransaction()) {
                            var expire = newLockExpire;
                            trans.QueueCommand(r => ((RedisNativeClient)r).Set(key, BitConverter.GetBytes(expire)));
                            if (trans.Commit()) {
                                wasSet = LockRecovered; // recovered lock!
                            }
                        }
                    } else {
                        localClient.UnWatch();
                    }
                }

                if (wasSet != LockNotAcquired) {
                    break;
                }

                Thread.Sleep(sleepIfLockSet);
                totalTime += sleepIfLockSet;
            }

            if (wasSet != LockNotAcquired) {
                lockExpire = newLockExpire;
            }

            return wasSet;
        }

        /// <summary>
        ///     unlock key
        /// </summary>
        public virtual bool Unlock(string key, long lockExpire, IRedisClient client) {
            if (lockExpire <= 0) {
                return false;
            }

            long lockVal = 0;
            var localClient = (RedisClient)client;
            using (var pipe = localClient.CreatePipeline()) {

                pipe.QueueCommand(r => ((RedisNativeClient)r).Watch(key));
                pipe.QueueCommand(r => ((RedisNativeClient)r).Get(key),
                    x => lockVal = x != null ? BitConverter.ToInt64(x, 0) : 0);
                pipe.Flush();
            }

            if (lockVal != lockExpire) {
                if (lockVal != 0) {
                    _logger.Debug("Unlock(): Failed to unlock key {0}; lock has been acquired by another client ", key);
                } else {
                    _logger.Debug("Unlock(): Failed to unlock key {0}; lock has been identifed as a zombie and harvested ", key);
                }

                localClient.UnWatch();
                return false;
            }

            using (var trans = localClient.CreateTransaction()) {
                trans.QueueCommand(r => ((RedisNativeClient)r).Del(key));
                var rc = trans.Commit();
                if (!rc) {
                    _logger.Debug("Unlock(): Failed to delete key {0}; lock has been acquired by another client ", key);
                }

                return rc;
            }
        }

        private static long CalculateLockExpire(TimeSpan ts, int timeout) {
            return (long)(ts.TotalSeconds + timeout + 1.5);
        }

    }

}
