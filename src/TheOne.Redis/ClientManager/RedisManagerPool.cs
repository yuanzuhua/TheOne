using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using TheOne.Logging;
using TheOne.Redis.Client;
using TheOne.Redis.Client.Internal;

namespace TheOne.Redis.ClientManager {

    /// <summary>
    ///     Provides thread-safe pooling of redis client connections. All connections are treated as read and write hosts.
    /// </summary>
    public class RedisManagerPool
        : IRedisClientManager, IRedisFailover, IHandleClientDispose, IHasRedisResolver, IRedisClientCacheManager {

        private static readonly ILog _logger = LogProvider.GetLogger(typeof(RedisManagerPool));
        private static readonly ReservedClient _reservedSlot = new ReservedClient();
        private readonly RedisClient[] _clients;
        private int _disposeAttempts;
        protected int PoolIndex;
        protected int RedisClientCounter;

        /// <inheritdoc />
        public RedisManagerPool() : this(RedisConfig.DefaultHost) { }
        /// <inheritdoc />
        public RedisManagerPool(string host) : this(new[] { host }) { }
        /// <inheritdoc />
        public RedisManagerPool(string host, RedisPoolConfig config) : this(new[] { host }, config) { }

        /// <inheritdoc />
        public RedisManagerPool(IEnumerable<string> hosts, RedisPoolConfig config = null) {
            if (hosts == null) {
                throw new ArgumentNullException(nameof(hosts));
            }

            this.RedisResolver = new RedisResolver(hosts, null);

            if (config == null) {
                config = new RedisPoolConfig();
            }

            this.OnFailover = new List<Action<IRedisClientManager>>();

            this.MaxPoolSize = config.MaxPoolSize;

            this._clients = new RedisClient[this.MaxPoolSize];
            this.PoolIndex = 0;

            this.AssertAccessOnlyOnSameThread = RedisConfig.AssertAccessOnlyOnSameThread;
        }

        // public Func<RedisEndpoint, RedisClient> ClientFactory { get; set; }
        public Action<IRedisNativeClient> ConnectionFilter { get; set; }

        public int MaxPoolSize { get; }

        public bool AssertAccessOnlyOnSameThread { get; set; }

        /// <inheritdoc />
        public void DisposeClient(RedisNativeClient client) {
            lock (this._clients) {
                for (var i = 0; i < this._clients.Length; i++) {
                    RedisClient writeClient = this._clients[i];
                    if (client != writeClient) {
                        continue;
                    }

                    if (client.IsDisposed) {
                        this._clients[i] = null;
                    } else {
                        client.TrackThread = null;
                        client.Active = false;
                    }

                    Monitor.PulseAll(this._clients);
                    return;
                }
            }
        }

        public IRedisResolver RedisResolver { get; set; }

        /// <summary>
        ///     Returns a Read/Write client (The default) using the hosts defined in ReadWriteHosts
        /// </summary>
        public IRedisClient GetClient() {
            try {
                int inactivePoolIndex;
                lock (this._clients) {
                    this.AssertValidPool();

                    // -1 when no available clients otherwise index of reservedSlot or existing Client
                    inactivePoolIndex = this.GetInActiveClient(out RedisClient inActiveClient);

                    // inActiveClient != null only for Valid InActive Clients
                    if (inActiveClient != null) {
                        this.PoolIndex++;
                        inActiveClient.Active = true;

                        return !this.AssertAccessOnlyOnSameThread
                            ? inActiveClient
                            : inActiveClient.LimitAccessToThread(Thread.CurrentThread.ManagedThreadId, Environment.StackTrace);
                    }
                }

                // Reaches here when there's no Valid InActive Clients
                try {
                    // inactivePoolIndex == -1 || index of reservedSlot || index of invalid client
                    RedisClient existingClient = inactivePoolIndex >= 0 && inactivePoolIndex < this._clients.Length
                        ? this._clients[inactivePoolIndex]
                        : null;

                    if (existingClient != null && existingClient != _reservedSlot && existingClient.HadExceptions) {
                        RedisState.DeactivateClient(existingClient);
                    }

                    RedisClient newClient = this.InitNewClient(this.RedisResolver.CreateMasterClient(Math.Max(inactivePoolIndex, 0)));

                    // Put all blocking I/O or potential Exceptions before lock
                    lock (this._clients) {
                        // Create new client outside of pool when max pool size exceeded
                        // Reverting free-slot not needed when -1 since slot wasn't reserved or 
                        // when existingClient changed (failover) since no longer reserved
                        var stillReserved = inactivePoolIndex >= 0 && inactivePoolIndex < this._clients.Length &&
                                            this._clients[inactivePoolIndex] == existingClient;
                        if (inactivePoolIndex == -1 || !stillReserved) {
                            if (_logger.IsDebugEnabled()) {
                                _logger.Debug(string.Format("clients[inactivePoolIndex] != existingClient: {0}",
                                    !stillReserved ? "!stillReserved" : "-1"));
                            }

                            Interlocked.Increment(ref RedisState.TotalClientsCreatedOutsidePool);

                            // Don't handle callbacks for new client outside pool
                            newClient.ClientManager = null;
                            return newClient;
                        }

                        this.PoolIndex++;
                        this._clients[inactivePoolIndex] = newClient;

                        return !this.AssertAccessOnlyOnSameThread
                            ? newClient
                            : newClient.LimitAccessToThread(Thread.CurrentThread.ManagedThreadId, Environment.StackTrace);
                    }
                } catch {
                    // Revert free-slot for any I/O exceptions that can throw (before lock)
                    lock (this._clients) {
                        if (inactivePoolIndex >= 0 && inactivePoolIndex < this._clients.Length) {
                            this._clients[inactivePoolIndex] = null;
                        }
                    }

                    throw;
                }
            } finally {
                RedisState.DisposeExpiredClients();
            }
        }

        /// <inheritdoc cref="IRedisClientManager.GetReadOnlyClient" />
        public IRedisClient GetReadOnlyClient() {
            return this.GetClient();
        }

        /// <inheritdoc />
        public void Dispose() {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <inheritdoc cref="IRedisClientManager.GetCacheClient" />
        public ICacheClient GetCacheClient() {
            return new RedisClientManagerCacheClient(this);
        }

        /// <inheritdoc cref="IRedisClientManager.GetReadOnlyCacheClient" />
        public ICacheClient GetReadOnlyCacheClient() {
            return new RedisClientManagerCacheClient(this) { ReadOnly = true };
        }

        /// <inheritdoc />
        public List<Action<IRedisClientManager>> OnFailover { get; }

        /// <inheritdoc />
        public void FailoverTo(params string[] readWriteHosts) {
            Interlocked.Increment(ref RedisState.TotalFailovers);

            lock (this._clients) {
                for (var i = 0; i < this._clients.Length; i++) {
                    RedisClient redis = this._clients[i];
                    if (redis != null) {
                        RedisState.DeactivateClient(redis);
                    }

                    this._clients[i] = null;
                }

                this.RedisResolver.ResetMasters(readWriteHosts);
            }

            if (this.OnFailover != null) {
                foreach (Action<IRedisClientManager> callback in this.OnFailover) {
                    try {
                        callback(this);
                    } catch (Exception ex) {
                        _logger.Error("Error firing OnFailover callback(): ", ex);
                    }
                }
            }
        }

        /// <inheritdoc />
        public void FailoverTo(IEnumerable<string> readWriteHosts, IEnumerable<string> readOnlyHosts) {
            this.FailoverTo(readWriteHosts.ToArray()); // only use readWriteHosts
        }


        /// <summary>
        ///     Called within a lock
        /// </summary>
        private int GetInActiveClient(out RedisClient inactiveClient) {
            // this will loop through all hosts in readClients once even though there are 2 for loops
            // both loops are used to try to get the preferred host according to the round robin algorithm
            var readWriteTotal = this.RedisResolver.ReadWriteHostsCount;
            var desiredIndex = this.PoolIndex % this._clients.Length;
            for (var x = 0; x < readWriteTotal; x++) {
                var nextHostIndex = (desiredIndex + x) % readWriteTotal;
                for (var i = nextHostIndex; i < this._clients.Length; i += readWriteTotal) {
                    if (this._clients[i] != null && !this._clients[i].Active && !this._clients[i].HadExceptions) {
                        inactiveClient = this._clients[i];
                        return i;
                    }

                    if (this._clients[i] == null) {
                        this._clients[i] = _reservedSlot;
                        inactiveClient = null;
                        return i;
                    }

                    if (this._clients[i] != _reservedSlot && this._clients[i].HadExceptions) {
                        inactiveClient = null;
                        return i;
                    }
                }
            }

            inactiveClient = null;
            return -1;
        }

        private RedisClient InitNewClient(RedisClient client) {
            client.Id = Interlocked.Increment(ref this.RedisClientCounter);
            client.Active = true;
            client.ClientManager = this;
            client.ConnectionFilter = this.ConnectionFilter;

            return client;
        }

        /// <summary>
        ///     Disposes the write client.
        /// </summary>
        /// <param name="client" >The client.</param>
        public void DisposeWriteClient(RedisNativeClient client) {
            lock (this._clients) {
                client.Active = false;
            }
        }

        public Dictionary<string, string> GetStats() {
            var clientsPoolSize = this._clients.Length;
            var clientsCreated = 0;
            var clientsWithExceptions = 0;
            var clientsInUse = 0;
            var clientsConnected = 0;

            foreach (RedisClient client in this._clients) {
                if (client == null) {
                    clientsCreated++;
                    continue;
                }

                if (client.HadExceptions) {
                    clientsWithExceptions++;
                }

                if (client.Active) {
                    clientsInUse++;
                }

                if (client.IsSocketConnected()) {
                    clientsConnected++;
                }
            }

            var ret = new Dictionary<string, string> {
                //{"VersionString", "" + Text.Env.VersionString},

                { "clientsPoolSize", "" + clientsPoolSize },
                { "clientsCreated", "" + clientsCreated },
                { "clientsWithExceptions", "" + clientsWithExceptions },
                { "clientsInUse", "" + clientsInUse },
                { "clientsConnected", "" + clientsConnected },

                { "RedisResolver.ReadOnlyHostsCount", "" + this.RedisResolver.ReadOnlyHostsCount },
                { "RedisResolver.ReadWriteHostsCount", "" + this.RedisResolver.ReadWriteHostsCount }
            };

            return ret;
        }

        private void AssertValidPool() {
            if (this._clients.Length < 1) {
                throw new InvalidOperationException("Need a minimum pool size of 1");
            }
        }

        public int[] GetClientPoolActiveStates() {
            lock (this._clients) {
                var activeStates = new int[this._clients.Length];
                for (var i = 0; i < this._clients.Length; i++) {
                    RedisClient client = this._clients[i];
                    activeStates[i] = client == null
                        ? -1
                        : client.Active
                            ? 1
                            : 0;
                }

                return activeStates;
            }
        }

        ~RedisManagerPool() {
            this.Dispose(false);
        }

        protected virtual void Dispose(bool disposing) {
            if (Interlocked.Increment(ref this._disposeAttempts) > 1) {
                return;
            }

            if (disposing) {
                // get rid of managed resources
            }

            try {
                // get rid of unmanaged resources
                foreach (RedisClient t in this._clients) {
                    this.Dispose(t);
                }
            } catch (Exception ex) {
                _logger.Error("Error when trying to dispose of PooledRedisClientManager", ex);
            }

            RedisState.DisposeAllDeactivatedClients();
        }

        protected void Dispose(RedisClient redisClient) {
            if (redisClient == null) {
                return;
            }

            try {
                redisClient.DisposeConnection();
            } catch (Exception ex) {
                _logger.Error(string.Format(
                        "Error when trying to dispose of RedisClient to host {0}:{1}",
                        redisClient.Host,
                        redisClient.Port),
                    ex);
            }
        }

        private class ReservedClient : RedisClient {

            public ReservedClient() {
                this.DeactivatedAt = DateTime.UtcNow;
            }

            public override void Dispose() { }

        }

    }

}
