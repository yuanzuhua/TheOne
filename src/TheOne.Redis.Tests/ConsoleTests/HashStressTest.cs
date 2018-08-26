using System;
using System.Collections.Generic;
using System.Threading;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.ClientManager;
using TheOne.Redis.Common;

namespace TheOne.Redis.Tests.ConsoleTests {

    [TestFixture]
    internal sealed class HashStressTest : RedisTestBase {

        #region Models

        internal class DeviceInfo {

            public Guid PlayerId { get; set; }
            public DateTime? LastErrTime { get; set; }
            public DateTime? LastWarnTime { get; set; }

            protected bool Equals(DeviceInfo other) {
                return this.PlayerId.Equals(other.PlayerId)
                       && this.LastErrTime.Equals(other.LastErrTime)
                       && this.LastWarnTime.Equals(other.LastWarnTime);
            }

            public override bool Equals(object obj) {
                if (ReferenceEquals(null, obj)) {
                    return false;
                }

                if (ReferenceEquals(this, obj)) {
                    return true;
                }

                if (obj.GetType() != this.GetType()) {
                    return false;
                }

                return this.Equals((DeviceInfo)obj);
            }

            public override int GetHashCode() {
                return base.GetHashCode();
            }

        }

        #endregion

        private readonly string _collectionKey = typeof(HashStressTest).Name;

        private readonly DeviceInfo _data = new DeviceInfo {
            PlayerId = new Guid("560531b06bc945b688f3a6a8ade65354"),
            LastErrTime = new DateTime(2000, 1, 1),
            LastWarnTime = new DateTime(2001, 1, 1)
        };

        private readonly TimeSpan? _waitBeforeRetry = TimeSpan.FromSeconds(1);
        private long _readCount;
        private int _running;
        private long _writeCount;
        public RedisManagerPool RedisManager;

        private void WorkerLoop() {
            while (Interlocked.CompareExchange(ref this._running, 0, 0) > 0) {
                using (IRedisClient client = this.RedisManager.GetClient()) {
                    try {
                        this.GetCollection<Guid, DeviceInfo>(client)[this._data.PlayerId] = this._data;
                        Interlocked.Increment(ref this._writeCount);
                    } catch (Exception ex) {
                        Console.WriteLine("WRITE ERROR: " + ex.Message);
                    }

                    try {
                        DeviceInfo readData = this.GetCollection<Guid, DeviceInfo>(client)[this._data.PlayerId];
                        Interlocked.Increment(ref this._readCount);

                        if (!readData.Equals(this._data)) {
                            Console.WriteLine("Data Error: " + readData.ToJson());
                        }
                    } catch (Exception ex) {
                        Console.WriteLine("READ ERROR: " + ex.Message);
                    }
                }

                if (this._waitBeforeRetry != null) {
                    Thread.Sleep(this._waitBeforeRetry.Value);
                }
            }
        }

        private IRedisHash<TKey, TValue> GetCollection<TKey, TValue>(IRedisClient redis) {
            IRedisTypedClient<TValue> redisTypedClient = redis.As<TValue>();
            return redisTypedClient.GetHash<TKey>(this._collectionKey);
        }

        [Test]
        public void Execute() {

            const int noOfThreads = 64;

            this.RedisManager = new RedisManagerPool(Config.MasterHost, new RedisPoolConfig { MaxPoolSize = noOfThreads });

            DateTime startedAt = DateTime.UtcNow;
            Interlocked.Increment(ref this._running);

            Console.WriteLine("Starting HashStressTest with {0} threads", noOfThreads);

            var threads = new List<Thread>();

            for (var i = 0; i < noOfThreads; i++) {
                threads.Add(new Thread(this.WorkerLoop));
            }

            threads.ForEach(t => t.Start());

            Interlocked.Decrement(ref this._running);

            Console.WriteLine("Writes: {0}, Reads: {1}", this._writeCount, this._readCount);
            Console.WriteLine("{0} EndedAt: {1}", this.GetType().Name, DateTime.UtcNow.ToLongTimeString());
            Console.WriteLine("{0} TimeTaken: {1}s", this.GetType().Name, (DateTime.UtcNow - startedAt).TotalSeconds);

            threads.ForEach(t => t.Join());
        }

    }

}
