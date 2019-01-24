using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.ClientManager;

namespace TheOne.Redis.Tests.ConsoleTests {

    [TestFixture]
    internal sealed class HashCollectionStressTests : RedisTestBase {

        #region Models

        public class RedisCachedCollection<TKey, TValue> : IEnumerable<TValue> {

            private readonly IRedisClientManager _clientsManager;
            private readonly string _collectionKey;

            public RedisCachedCollection(IRedisClientManager clientsManager, string collectionKey) {
                this._clientsManager = clientsManager;
                this._collectionKey = string.Format("urn:{0}:{1}", "XXXXX", collectionKey);
            }

            public IRedisClient RedisConnection => this._clientsManager.GetClient();

            public TValue this[TKey id] {
                get {
                    return this.RetryAction(redis => {
                        if (this.GetCollection(redis).ContainsKey(id)) {
                            return this.GetCollection(redis)[id];
                        }

                        return default;
                    });
                }
                set {
                    this.RetryAction(redis => {
                        this.GetCollection(redis)[id] = value;
                    });
                }
            }

            public int Count {
                get { return this.RetryAction(redis => this.GetCollection(redis).Count); }
            }

            public bool IsReadOnly {
                get { return this.RetryAction(redis => this.GetCollection(redis).IsReadOnly); }
            }

            public Func<TValue, TKey> GetUniqueIdAction { get; set; }


            public IEnumerator<TValue> GetEnumerator() {
                return this.RetryAction(redis => this.GetCollection(redis).Values.GetEnumerator());
            }

            IEnumerator IEnumerable.GetEnumerator() {
                return this.RetryAction(redis => ((IEnumerable)this.GetCollection(redis).Values).GetEnumerator());

            }

            private IRedisHash<TKey, TValue> GetCollection(IRedisClient redis) {
                var redisTypedClient = redis.As<TValue>();
                return redisTypedClient.GetHash<TKey>(this._collectionKey);
            }

            public void Add(TValue obj) {
                var id = this.GetUniqueIdAction(obj);

                this.RetryAction(redis => {
                    this.GetCollection(redis).Add(id, obj);
                });
            }

            public bool Remove(TValue obj) {
                var id = this.GetUniqueIdAction(obj);
                return this.RetryAction(redis => {
                    if (!id.Equals(default)) {
                        {
                            return this.GetCollection(redis).Remove(id);
                        }
                    }

                    return false;
                });

            }

            public IEnumerable<TValue> Where(Func<TValue, bool> predicate) {
                return this.RetryAction(redis => {
                    return this.GetCollection(redis).Values.Where(predicate);
                });
            }

            public bool Any(Func<TValue, bool> predicate) {
                return this.RetryAction(redis => {
                    return this.GetCollection(redis).Values.Any(predicate);
                });
            }

            public void Clear() {
                this.RetryAction(redis => {
                    this.GetCollection(redis).Clear();
                });
            }

            public bool Contains(TValue obj) {
                var id = this.GetUniqueIdAction(obj);
                return this.RetryAction(redis => this.GetCollection(redis).ContainsKey(id));
            }

            public bool ContainsKey(TKey obj) {
                return this.RetryAction(redis => this.GetCollection(redis).ContainsKey(obj));
            }

            public void CopyTo(TValue[] array, int arrayIndex) {
                this.RetryAction(redis => {
                    this.GetCollection(redis).Values.CopyTo(array, arrayIndex);
                });
            }

            private void RetryAction(Action<IRedisClient> action) {
                try {
                    using (var redis = this.RedisConnection) {
                        action(redis);
                    }
                } catch (Exception ex) {
                    Console.WriteLine(ex);
                    throw;
                }
            }

            private TOut RetryAction<TOut>(Func<IRedisClient, TOut> action) {
                var i = 0;

                while (true) {
                    try {
                        using (var redis = this.RedisConnection) {
                            var result = action(redis);
                            return result;
                        }
                    } catch {

                        if (i++ < 3) {

                            continue;
                        }

                        throw;
                    }
                }
            }

        }

        #endregion

        private IRedisClientManager _clientsManager;
        private long _readCount;
        private RedisCachedCollection<string, string> _redisCollection;
        private int _running;
        private long _writeCount;

        private void WorkerLoop() {
            while (Interlocked.CompareExchange(ref this._running, 0, 0) > 0) {
                this._redisCollection.ContainsKey("key");
                Interlocked.Increment(ref this._readCount);

                this._redisCollection["key"] = "value " + this._readCount;
                Interlocked.Increment(ref this._writeCount);

                var value = this._redisCollection["key"];
                Interlocked.Increment(ref this._readCount);

                if (value == null) {
                    Console.WriteLine("value == null");
                }
            }
        }

        [Test]
        public void Execute() {
            const int noOfThreads = 64;
            this._clientsManager = new PooledRedisClientManager(Config.MasterHost);

            this._redisCollection = new RedisCachedCollection<string, string>(this._clientsManager, "Threads: " + 64);

            var startedAt = DateTime.Now;
            Interlocked.Increment(ref this._running);

            Console.WriteLine("Starting HashCollectionStressTests with {0} threads", noOfThreads);
            var threads = new List<Thread>();
            for (var i = 0; i < noOfThreads; i++) {
                threads.Add(new Thread(this.WorkerLoop));
            }

            threads.ForEach(t => t.Start());

            Interlocked.Decrement(ref this._running);

            Console.WriteLine("Writes: {0}, Reads: {1}", this._writeCount, this._readCount);
            Console.WriteLine("{0} EndedAt: {1}", this.GetType().Name, DateTime.Now.ToLongTimeString());
            Console.WriteLine("{0} TimeTaken: {1}s", this.GetType().Name, (DateTime.Now - startedAt).TotalSeconds);
        }

    }

}
