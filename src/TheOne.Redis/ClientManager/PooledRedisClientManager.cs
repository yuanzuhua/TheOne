using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using TheOne.Logging;
using TheOne.Redis.Client;
using TheOne.Redis.Client.Internal;

namespace TheOne.Redis.ClientManager {

    /// <summary>
    ///     Provides thread-safe pooling of redis client connections.
    ///     Allows load-balancing of master-write and read-slave hosts, ideal for
    ///     1 master and multiple replicated read slaves.
    /// </summary>
    public partial class PooledRedisClientManager : IRedisClientCacheManager, IRedisClientManager, IRedisFailover, IHandleClientDispose,
        IHasRedisResolver {

        private const string _poolTimeoutError =
            "Redis Timeout expired. The timeout period elapsed prior to obtaining a connection from the pool. This may have occurred because all pooled connections were in use.";

        private static readonly ILog _logger = LogProvider.GetLogger(typeof(PooledRedisClientManager));
        private static readonly ReservedClient _reservedSlot = new ReservedClient();
        protected readonly int PoolSizeMultiplier;
        private int _disposeAttempts;
        private RedisClient[] _readClients = Array.Empty<RedisClient>();
        private RedisClient[] _writeClients = Array.Empty<RedisClient>();
        protected int ReadPoolIndex;
        public int RecheckPoolAfterMs = 100;
        protected int RedisClientCounter;
        protected int WritePoolIndex;

        /// <inheritdoc />
        public PooledRedisClientManager() : this(RedisConfig.DefaultHost) { }

        /// <inheritdoc />
        public PooledRedisClientManager(int poolSize, int poolTimeoutSeconds, params string[] readWriteHosts)
            : this(readWriteHosts, readWriteHosts, null, null, poolSize, poolTimeoutSeconds) { }

        /// <inheritdoc />
        public PooledRedisClientManager(long initialDb, params string[] readWriteHosts)
            : this(readWriteHosts, readWriteHosts, initialDb) { }

        /// <inheritdoc />
        public PooledRedisClientManager(params string[] readWriteHosts)
            : this(readWriteHosts, readWriteHosts) { }

        /// <summary>
        ///     Hosts can be an IP Address or Hostname in the format: host[:port]
        ///     e.g. 127.0.0.1:6379
        ///     default is: localhost:6379
        /// </summary>
        /// <param name="readWriteHosts" >The write hosts.</param>
        /// <param name="readOnlyHosts" >The read hosts.</param>
        /// <param name="config" >The config.</param>
        public PooledRedisClientManager(
            IEnumerable<string> readWriteHosts,
            IEnumerable<string> readOnlyHosts,
            RedisClientManagerConfig config = null)
            : this(readWriteHosts, readOnlyHosts, config, null, null, null) { }

        /// <inheritdoc />
        public PooledRedisClientManager(
            IEnumerable<string> readWriteHosts,
            IEnumerable<string> readOnlyHosts,
            long initialDb)
            : this(readWriteHosts, readOnlyHosts, null, initialDb, null, null) { }

        /// <inheritdoc />
        public PooledRedisClientManager(
            IEnumerable<string> readWriteHosts,
            IEnumerable<string> readOnlyHosts,
            RedisClientManagerConfig config,
            long? initialDb,
            int? poolSizeMultiplier,
            int? poolTimeoutSeconds) {
            this.Db = config != null
                ? config.DefaultDb ?? initialDb
                : initialDb;

            var masters = readWriteHosts?.ToArray() ?? Array.Empty<string>();
            var slaves = readOnlyHosts?.ToArray() ?? Array.Empty<string>();

            this.RedisResolver = new RedisResolver(masters, slaves);

            this.PoolSizeMultiplier = poolSizeMultiplier ?? 20;

            this.Config = config ?? new RedisClientManagerConfig {
                MaxWritePoolSize = RedisConfig.DefaultMaxPoolSize ?? masters.Length * this.PoolSizeMultiplier,
                MaxReadPoolSize = RedisConfig.DefaultMaxPoolSize ?? slaves.Length * this.PoolSizeMultiplier
            };

            this.OnFailover = new List<Action<IRedisClientManager>>();

            // if timeout provided, convert into milliseconds
            this.PoolTimeout = poolTimeoutSeconds != null
                ? poolTimeoutSeconds * 1000
                : 2000; // Default Timeout

            this.AssertAccessOnlyOnSameThread = RedisConfig.AssertAccessOnlyOnSameThread;

            if (this.Config.AutoStart) {
                this.OnStart();
            }
        }

        public int? PoolTimeout { get; set; }
        public int? ConnectTimeout { get; set; }
        public int? SocketSendTimeout { get; set; }
        public int? SocketReceiveTimeout { get; set; }
        public int? IdleTimeoutSecs { get; set; }
        public bool AssertAccessOnlyOnSameThread { get; set; }

        /// <summary>
        ///     Gets or sets object key prefix.
        /// </summary>
        public string NamespacePrefix { get; set; }

        protected RedisClientManagerConfig Config { get; set; }

        public long? Db { get; }

        public Action<IRedisNativeClient> ConnectionFilter { get; set; }

        /// <inheritdoc />
        public void DisposeClient(RedisNativeClient client) {
            lock (this._readClients) {
                for (var i = 0; i < this._readClients.Length; i++) {
                    var readClient = this._readClients[i];
                    if (client != readClient) {
                        continue;
                    }

                    if (client.IsDisposed) {
                        this._readClients[i] = null;
                    } else {
                        client.TrackThread = null;
                        client.Active = false;
                    }

                    Monitor.PulseAll(this._readClients);
                    return;
                }
            }

            lock (this._writeClients) {
                for (var i = 0; i < this._writeClients.Length; i++) {
                    var writeClient = this._writeClients[i];
                    if (client != writeClient) {
                        continue;
                    }

                    if (client.IsDisposed) {
                        this._writeClients[i] = null;
                    } else {
                        client.TrackThread = null;
                        client.Active = false;
                    }

                    Monitor.PulseAll(this._writeClients);
                    return;
                }
            }

            // Client not found in any pool, pulse both pools.
            lock (this._readClients) {
                Monitor.PulseAll(this._readClients);
            }

            lock (this._writeClients) {
                Monitor.PulseAll(this._writeClients);
            }
        }

        /// <inheritdoc />
        public IRedisResolver RedisResolver { get; set; }

        /// <summary>
        ///     Returns a Read/Write client (The default) using the hosts defined in ReadWriteHosts
        /// </summary>
        /// <inheritdoc />
        public IRedisClient GetClient() {
            try {
                var poolTimedOut = false;
                int inactivePoolIndex;
                lock (this._writeClients) {
                    this.AssertValidReadWritePool();

                    RedisClient inActiveClient;
                    while ((inactivePoolIndex = this.GetInActiveWriteClient(out inActiveClient)) == -1) {
                        if (this.PoolTimeout.HasValue) {
                            // wait for a connection, cry out if made to wait too long
                            if (!Monitor.Wait(this._writeClients, this.PoolTimeout.Value)) {
                                poolTimedOut = true;
                                break;
                            }
                        } else {
                            Monitor.Wait(this._writeClients, this.RecheckPoolAfterMs);
                        }
                    }

                    // inActiveClient != null only for Valid InActive Clients
                    if (inActiveClient != null) {
                        this.WritePoolIndex++;
                        inActiveClient.Active = true;

                        this.InitClient(inActiveClient);

                        return !this.AssertAccessOnlyOnSameThread
                            ? inActiveClient
                            : inActiveClient.LimitAccessToThread(Thread.CurrentThread.ManagedThreadId, Environment.StackTrace);
                    }
                }

                if (poolTimedOut) {
                    throw new TimeoutException(_poolTimeoutError);
                }

                // Reaches here when there's no Valid InActive Clients
                try {
                    // inactivePoolIndex = index of reservedSlot || index of invalid client
                    var existingClient = this._writeClients[inactivePoolIndex];
                    if (existingClient != null && existingClient != _reservedSlot && existingClient.HadExceptions) {
                        RedisState.DeactivateClient(existingClient);
                    }

                    var newClient = this.InitNewClient(this.RedisResolver.CreateMasterClient(inactivePoolIndex));

                    // Put all blocking I/O or potential Exceptions before lock
                    lock (this._writeClients) {
                        // If existingClient at inactivePoolIndex changed (failover) return new client outside of pool
                        if (this._writeClients[inactivePoolIndex] != existingClient) {
                            _logger.Debug("writeClients[inactivePoolIndex] != existingClient: {0}", this._writeClients[inactivePoolIndex]);

                            return newClient; // return client outside of pool
                        }

                        this.WritePoolIndex++;
                        this._writeClients[inactivePoolIndex] = newClient;

                        return !this.AssertAccessOnlyOnSameThread
                            ? newClient
                            : newClient.LimitAccessToThread(Thread.CurrentThread.ManagedThreadId, Environment.StackTrace);
                    }
                } catch {
                    // Revert free-slot for any I/O exceptions that can throw (before lock)
                    lock (this._writeClients) {
                        this._writeClients[inactivePoolIndex] = null; // free slot
                    }

                    throw;
                }
            } finally {
                RedisState.DisposeExpiredClients();
            }
        }

        /// <summary>
        ///     Returns a ReadOnly client using the hosts defined in ReadOnlyHosts.
        /// </summary>
        public virtual IRedisClient GetReadOnlyClient() {
            try {
                var poolTimedOut = false;
                int inactivePoolIndex;
                lock (this._readClients) {
                    this.AssertValidReadOnlyPool();

                    RedisClient inActiveClient;
                    while ((inactivePoolIndex = this.GetInActiveReadClient(out inActiveClient)) == -1) {
                        if (this.PoolTimeout.HasValue) {
                            // wait for a connection, break out if made to wait too long
                            if (!Monitor.Wait(this._readClients, this.PoolTimeout.Value)) {
                                poolTimedOut = true;
                                break;
                            }
                        } else {
                            Monitor.Wait(this._readClients, this.RecheckPoolAfterMs);
                        }
                    }

                    // inActiveClient != null only for Valid InActive Clients
                    if (inActiveClient != null) {
                        this.ReadPoolIndex++;
                        inActiveClient.Active = true;

                        this.InitClient(inActiveClient);

                        return inActiveClient;
                    }
                }

                if (poolTimedOut) {
                    throw new TimeoutException(_poolTimeoutError);
                }

                // Reaches here when there's no Valid InActive Clients
                try {
                    // inactivePoolIndex = index of reservedSlot || index of invalid client
                    var existingClient = this._readClients[inactivePoolIndex];
                    if (existingClient != null && existingClient != _reservedSlot && existingClient.HadExceptions) {
                        RedisState.DeactivateClient(existingClient);
                    }

                    var newClient = this.InitNewClient(this.RedisResolver.CreateSlaveClient(inactivePoolIndex));

                    // Put all blocking I/O or potential Exceptions before lock
                    lock (this._readClients) {
                        // If existingClient at inactivePoolIndex changed (failover) return new client outside of pool
                        if (this._readClients[inactivePoolIndex] != existingClient) {
                            _logger.Debug("readClients[inactivePoolIndex] != existingClient: {0}", this._readClients[inactivePoolIndex]);

                            Interlocked.Increment(ref RedisState.TotalClientsCreatedOutsidePool);

                            // Don't handle callbacks for new client outside pool
                            newClient.ClientManager = null;
                            return newClient; // return client outside of pool
                        }

                        this.ReadPoolIndex++;
                        this._readClients[inactivePoolIndex] = newClient;
                        return newClient;
                    }
                } catch {
                    // Revert free-slot for any I/O exceptions that can throw
                    lock (this._readClients) {
                        this._readClients[inactivePoolIndex] = null; // free slot
                    }

                    throw;
                }
            } finally {
                RedisState.DisposeExpiredClients();
            }
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
            this.FailoverTo(readWriteHosts, readWriteHosts);
        }

        /// <inheritdoc />
        public void FailoverTo(IEnumerable<string> readWriteHosts, IEnumerable<string> readOnlyHosts) {
            Interlocked.Increment(ref RedisState.TotalFailovers);

            lock (this._readClients) {
                for (var i = 0; i < this._readClients.Length; i++) {
                    var redis = this._readClients[i];
                    if (redis != null) {
                        RedisState.DeactivateClient(redis);
                    }

                    this._readClients[i] = null;
                }

                this.RedisResolver.ResetSlaves(readOnlyHosts);
            }

            lock (this._writeClients) {
                for (var i = 0; i < this._writeClients.Length; i++) {
                    var redis = this._writeClients[i];
                    if (redis != null) {
                        RedisState.DeactivateClient(redis);
                    }

                    this._writeClients[i] = null;
                }

                this.RedisResolver.ResetMasters(readWriteHosts);
            }

            if (this.OnFailover != null) {
                foreach (var callback in this.OnFailover) {
                    try {
                        callback(this);
                    } catch (Exception ex) {
                        _logger.Error(ex, "Error firing OnFailover callback(): ");
                    }
                }
            }
        }

        protected virtual void OnStart() {
            this.Start();
        }

        /// <summary>
        ///     Called within a lock
        /// </summary>
        private int GetInActiveWriteClient(out RedisClient inactiveClient) {
            // this will loop through all hosts in readClients once even though there are 2 for loops
            // both loops are used to try to get the preferred host according to the round robin algorithm
            var readWriteTotal = this.RedisResolver.ReadWriteHostsCount;
            var desiredIndex = this.WritePoolIndex % this._writeClients.Length;
            for (var x = 0; x < readWriteTotal; x++) {
                var nextHostIndex = (desiredIndex + x) % readWriteTotal;
                for (var i = nextHostIndex; i < this._writeClients.Length; i += readWriteTotal) {
                    if (this._writeClients[i] != null && !this._writeClients[i].Active && !this._writeClients[i].HadExceptions) {
                        inactiveClient = this._writeClients[i];
                        return i;
                    }

                    if (this._writeClients[i] == null) {
                        this._writeClients[i] = _reservedSlot;
                        inactiveClient = null;
                        return i;
                    }

                    if (this._writeClients[i] != _reservedSlot && this._writeClients[i].HadExceptions) {
                        inactiveClient = null;
                        return i;
                    }
                }
            }

            inactiveClient = null;
            return -1;
        }

        /// <summary>
        ///     Called within a lock
        /// </summary>
        private int GetInActiveReadClient(out RedisClient inactiveClient) {
            var desiredIndex = this.ReadPoolIndex % this._readClients.Length;
            // this will loop through all hosts in readClients once even though there are 2 for loops
            // both loops are used to try to get the preferred host according to the round robin algorithm
            var readOnlyTotal = this.RedisResolver.ReadOnlyHostsCount;
            for (var x = 0; x < readOnlyTotal; x++) {
                var nextHostIndex = (desiredIndex + x) % readOnlyTotal;
                for (var i = nextHostIndex; i < this._readClients.Length; i += readOnlyTotal) {
                    if (this._readClients[i] != null && !this._readClients[i].Active && !this._readClients[i].HadExceptions) {
                        inactiveClient = this._readClients[i];
                        return i;
                    }

                    if (this._readClients[i] == null) {
                        this._readClients[i] = _reservedSlot;
                        inactiveClient = null;
                        return i;
                    }

                    if (this._readClients[i] != _reservedSlot && this._readClients[i].HadExceptions) {
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
            if (this.NamespacePrefix != null) {
                client.NamespacePrefix = this.NamespacePrefix;
            }

            return this.InitClient(client);
        }

        private RedisClient InitClient(RedisClient client) {
            if (this.ConnectTimeout != null) {
                client.ConnectTimeout = this.ConnectTimeout.Value;
            }

            if (this.SocketSendTimeout.HasValue) {
                client.SendTimeout = this.SocketSendTimeout.Value;
            }

            if (this.SocketReceiveTimeout.HasValue) {
                client.ReceiveTimeout = this.SocketReceiveTimeout.Value;
            }

            if (this.IdleTimeoutSecs.HasValue) {
                client.IdleTimeoutSecs = this.IdleTimeoutSecs.Value;
            }

            if (this.NamespacePrefix != null) {
                client.NamespacePrefix = this.NamespacePrefix;
            }

            if (this.Db != null && client.Db != this.Db) // Reset database to default if changed
            {
                client.ChangeDb(this.Db.Value);
            }

            return client;
        }

        /// <summary>
        ///     Disposes the read only client.
        /// </summary>
        /// <param name="client" >The client.</param>
        public void DisposeReadOnlyClient(RedisNativeClient client) {
            lock (this._readClients) {
                client.Active = false;
                Monitor.PulseAll(this._readClients);
            }
        }

        /// <summary>
        ///     Disposes the write client.
        /// </summary>
        /// <param name="client" >The client.</param>
        public void DisposeWriteClient(RedisNativeClient client) {
            lock (this._writeClients) {
                client.Active = false;
                Monitor.PulseAll(this._writeClients);
            }
        }

        public void Start() {
            if (this._writeClients.Length > 0 || this._readClients.Length > 0) {
                throw new InvalidOperationException("Pool has already been started");
            }

            this._writeClients = new RedisClient[this.Config.MaxWritePoolSize];
            this.WritePoolIndex = 0;

            this._readClients = new RedisClient[this.Config.MaxReadPoolSize];
            this.ReadPoolIndex = 0;
        }

        public Dictionary<string, string> GetStats() {
            var writeClientsPoolSize = this._writeClients.Length;
            var writeClientsCreated = 0;
            var writeClientsWithExceptions = 0;
            var writeClientsInUse = 0;
            var writeClientsConnected = 0;

            foreach (var client in this._writeClients) {
                if (client == null) {
                    writeClientsCreated++;
                    continue;
                }

                if (client.HadExceptions) {
                    writeClientsWithExceptions++;
                }

                if (client.Active) {
                    writeClientsInUse++;
                }

                if (client.IsSocketConnected()) {
                    writeClientsConnected++;
                }
            }

            var readClientsPoolSize = this._readClients.Length;
            var readClientsCreated = 0;
            var readClientsWithExceptions = 0;
            var readClientsInUse = 0;
            var readClientsConnected = 0;

            foreach (var client in this._readClients) {
                if (client == null) {
                    readClientsCreated++;
                    continue;
                }

                if (client.HadExceptions) {
                    readClientsWithExceptions++;
                }

                if (client.Active) {
                    readClientsInUse++;
                }

                if (client.IsSocketConnected()) {
                    readClientsConnected++;
                }
            }

            var ret = new Dictionary<string, string> {
                // {"VersionString", "" + Env.VersionString},

                { "writeClientsPoolSize", "" + writeClientsPoolSize },
                { "writeClientsCreated", "" + writeClientsCreated },
                { "writeClientsWithExceptions", "" + writeClientsWithExceptions },
                { "writeClientsInUse", "" + writeClientsInUse },
                { "writeClientsConnected", "" + writeClientsConnected },

                { "readClientsPoolSize", "" + readClientsPoolSize },
                { "readClientsCreated", "" + readClientsCreated },
                { "readClientsWithExceptions", "" + readClientsWithExceptions },
                { "readClientsInUse", "" + readClientsInUse },
                { "readClientsConnected", "" + readClientsConnected },

                { "RedisResolver.ReadOnlyHostsCount", "" + this.RedisResolver.ReadOnlyHostsCount },
                { "RedisResolver.ReadWriteHostsCount", "" + this.RedisResolver.ReadWriteHostsCount }
            };

            return ret;
        }

        private void AssertValidReadWritePool() {
            if (this._writeClients.Length < 1) {
                throw new InvalidOperationException("Need a minimum read-write pool size of 1, then call Start()");
            }
        }

        private void AssertValidReadOnlyPool() {
            if (this._readClients.Length < 1) {
                throw new InvalidOperationException("Need a minimum read pool size of 1, then call Start()");
            }
        }

        public int[] GetClientPoolActiveStates() {
            var activeStates = new int[this._writeClients.Length];
            lock (this._writeClients) {
                for (var i = 0; i < this._writeClients.Length; i++) {
                    activeStates[i] = this._writeClients[i] == null
                        ? -1
                        : this._writeClients[i].Active
                            ? 1
                            : 0;
                }
            }

            return activeStates;
        }

        public int[] GetReadOnlyClientPoolActiveStates() {
            var activeStates = new int[this._readClients.Length];
            lock (this._readClients) {
                for (var i = 0; i < this._readClients.Length; i++) {
                    activeStates[i] = this._readClients[i] == null
                        ? -1
                        : this._readClients[i].Active
                            ? 1
                            : 0;
                }
            }

            return activeStates;
        }

        ~PooledRedisClientManager() {
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
                foreach (var t in this._writeClients) {
                    this.Dispose(t);
                }

                foreach (var t in this._readClients) {
                    this.Dispose(t);
                }
            } catch (Exception ex) {
                _logger.Error(ex, "Error when trying to dispose of PooledRedisClientManager");
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
                _logger.Error(ex, "Error when trying to dispose of RedisClient to host {0}:{1}", redisClient.Host, redisClient.Port);
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
