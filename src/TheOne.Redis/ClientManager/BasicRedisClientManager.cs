using System;
using System.Collections.Generic;
using System.Threading;
using TheOne.Redis.Client;
using TheOne.Redis.Client.Internal;

namespace TheOne.Redis.ClientManager {

    /// <summary>
    ///     Provides thread-safe retrieve of redis clients since each client is a new one.
    ///     <para>
    ///         Allows the configuration of different ReadWrite and ReadOnly hosts
    ///     </para>
    /// </summary>
    /// <remarks>
    ///     this also implementing the <see cref="ICacheClient" /> which has the affect of calling
    ///     <see cref="GetCacheClient" /> for all write operations
    ///     and <see cref="GetReadOnlyCacheClient" />  for the read ones.
    ///     <para>
    ///         This works well for master-slave replication scenarios where you have
    ///         1 master that replicates to multiple read slaves.
    ///     </para>
    /// </remarks>
    public partial class BasicRedisClientManager : ICacheClient, IRedisClientManager, IRedisFailover, IHasRedisResolver {

        private int _readOnlyHostsIndex;
        private int _readWriteHostsIndex;
        protected int RedisClientCounter;

        /// <inheritdoc />
        public BasicRedisClientManager() : this(RedisConfig.DefaultHost) { }

        /// <inheritdoc />
        public BasicRedisClientManager(params string[] readWriteHosts) : this(readWriteHosts, readWriteHosts) { }

        /// <inheritdoc />
        public BasicRedisClientManager(int initialDb, params string[] readWriteHosts) : this(readWriteHosts, readWriteHosts, initialDb) { }

        /// <summary>
        ///     Hosts can be an IP Address or Hostname in the format: host[:port]
        ///     e.g. 127.0.0.1:6379
        ///     default is: localhost:6379
        /// </summary>
        /// <param name="readWriteHosts" >The write hosts.</param>
        /// <param name="readOnlyHosts" >The read hosts.</param>
        /// <param name="initialDb" >initialDb</param>
        public BasicRedisClientManager(
            IEnumerable<string> readWriteHosts,
            IEnumerable<string> readOnlyHosts,
            long? initialDb = null)
            : this(RedisEndpoint.Create(readWriteHosts), RedisEndpoint.Create(readOnlyHosts), initialDb) { }

        /// <inheritdoc />
        public BasicRedisClientManager(
            IEnumerable<RedisEndpoint> readWriteHosts,
            IEnumerable<RedisEndpoint> readOnlyHosts,
            long? initialDb = null) {
            this.Db = initialDb;

            this.RedisResolver = new RedisResolver(readWriteHosts, readOnlyHosts);

            this.OnFailover = new List<Action<IRedisClientManager>>();

            // ReSharper disable once VirtualMemberCallInConstructor
            this.OnStart();
        }

        public int? ConnectTimeout { get; set; }
        public int? SocketSendTimeout { get; set; }
        public int? SocketReceiveTimeout { get; set; }
        public int? IdleTimeoutSecs { get; set; }

        /// <summary>
        ///     Gets or sets object key prefix.
        /// </summary>
        public string NamespacePrefix { get; set; }

        public Func<RedisEndpoint, RedisClient> ClientFactory { get; set; }

        public long? Db { get; }

        public Action<IRedisNativeClient> ConnectionFilter { get; set; }

        /// <inheritdoc />
        public void SetAll<T>(IDictionary<string, T> values) {
            foreach (KeyValuePair<string, T> entry in values) {
                this.Set(entry.Key, entry.Value);
            }
        }

        /// <inheritdoc />
        public void Dispose() { }

        /// <inheritdoc />
        public IRedisResolver RedisResolver { get; set; }

        /// <summary>
        ///     Returns a Read/Write client (The default) using the hosts defined in ReadWriteHosts
        /// </summary>
        public IRedisClient GetClient() {
            RedisClient client = this.InitNewClient(this.RedisResolver.CreateMasterClient(this._readWriteHostsIndex++));
            return client;
        }

        /// <summary>
        ///     Returns a ReadOnly client using the hosts defined in ReadOnlyHosts.
        /// </summary>
        public virtual IRedisClient GetReadOnlyClient() {
            RedisClient client = this.InitNewClient(this.RedisResolver.CreateSlaveClient(this._readOnlyHostsIndex++));
            return client;
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

            lock (this) {
                this.RedisResolver.ResetMasters(readWriteHosts);
                this.RedisResolver.ResetSlaves(readOnlyHosts);
            }

            this.Start();
        }

        protected virtual void OnStart() {
            this.Start();
        }

        private RedisClient InitNewClient(RedisClient client) {
            client.Id = Interlocked.Increment(ref this.RedisClientCounter);
            client.ConnectionFilter = this.ConnectionFilter;
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

            if (this.Db != null && client.Db != this.Db) {
                // Reset database to default if changed
                client.ChangeDb(this.Db.Value);
            }

            return client;
        }

        public void Start() {
            this._readWriteHostsIndex = 0;
            this._readOnlyHostsIndex = 0;
        }

    }

}
