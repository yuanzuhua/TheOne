using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using TheOne.Logging;
using TheOne.Redis.Client;
using TheOne.Redis.Client.Internal;
using TheOne.Redis.ClientManager;
using TheOne.Redis.Common;

namespace TheOne.Redis.Sentinel {

    /// <summary>
    ///     <para>
    ///         Redis Sentinel will connect to a Redis Sentinel Instance and create an IRedisClientManager based off of the first sentinel that
    ///         returns data
    ///     </para>
    ///     <para> Upon failure of a sentinel, other sentinels will be attempted to be connected to</para>
    ///     <para> Upon a s_down event, the RedisClientManager will be failed over to the new set of slaves/masters </para>
    /// </summary>
    public class RedisSentinel : IRedisSentinel {

        private static readonly ILog _logger = LogProvider.GetLogger(typeof(RedisSentinel));
        public static string DefaultMasterName = "mymaster";
        public static string DefaultAddress = "127.0.0.1:26379";
        private static readonly int _maxFailures = 5;
        private readonly object _oLock = new object();
        private int _failures;
        private bool _isDisposed;
        private DateTime _lastSentinelsRefresh;
        private int _sentinelIndex = -1;
        private RedisSentinelWorker _worker;

        /// <inheritdoc />
        public RedisSentinel(string sentinelHost = null, string masterName = null)
            : this(new[] { sentinelHost ?? DefaultAddress }, masterName ?? DefaultMasterName) { }

        /// <inheritdoc />
        public RedisSentinel(IEnumerable<string> sentinelHosts, string masterName = null) {
            this.SentinelHosts = sentinelHosts?.ToList();

            if (this.SentinelHosts == null || this.SentinelHosts.Count == 0) {
                throw new ArgumentException("sentinels must have at least one entry");
            }

            this.MasterName = masterName ?? DefaultMasterName;
            this.IpAddressMap = new Dictionary<string, string>();
            this.RedisManagerFactory = (masters, slaves) => new PooledRedisClientManager(masters, slaves);
            this.ScanForOtherSentinels = true;
            this.RefreshSentinelHostsAfter = TimeSpan.FromMinutes(10);
            this.ResetWhenObjectivelyDown = true;
            this.ResetWhenSubjectivelyDown = true;
            this.SentinelWorkerConnectTimeoutMs = 100;
            this.SentinelWorkerReceiveTimeoutMs = 100;
            this.SentinelWorkerSendTimeoutMs = 100;
            this.WaitBetweenFailedHosts = TimeSpan.FromMilliseconds(250);
            this.MaxWaitBetweenFailedHosts = TimeSpan.FromSeconds(60);
            this.WaitBeforeForcingMasterFailover = TimeSpan.FromSeconds(60);
        }

        public string MasterName { get; }

        public List<string> SentinelHosts { get; }
        internal RedisEndpoint[] SentinelEndpoints { get; private set; }

        /// <summary>
        ///     Change to use a different IRedisClientManager
        /// </summary>
        public Func<string[], string[], IRedisClientManager> RedisManagerFactory { get; set; }

        /// <summary>
        ///     Configure the Redis Connection String to use for a Redis Client Host
        /// </summary>
        public Func<string, string> HostFilter { get; set; }

        /// <summary>
        ///     The configured Redis Client Manager this Sentinel managers
        /// </summary>
        public IRedisClientManager RedisManager { get; set; }

        /// <summary>
        ///     Fired when Sentinel fails over the Redis Client Manager to a new master
        /// </summary>
        public Action<IRedisClientManager> OnFailover { get; set; }

        /// <summary>
        ///     Fired when the Redis Sentinel Worker connection fails
        /// </summary>
        public Action<Exception> OnWorkerError { get; set; }

        /// <summary>
        ///     Fired when the Sentinel worker receives a message from the Sentinel Subscription
        /// </summary>
        public Action<string, string> OnSentinelMessageReceived { get; set; }

        /// <summary>
        ///     Map the internal IP's returned by Sentinels to its external IP
        /// </summary>
        public Dictionary<string, string> IpAddressMap { get; set; }

        /// <summary>
        ///     Whether to routinely scan for other sentinel hosts (default true)
        /// </summary>
        public bool ScanForOtherSentinels { get; set; }

        /// <summary>
        ///     What interval to scan for other sentinel hosts (default 10 mins)
        /// </summary>
        public TimeSpan RefreshSentinelHostsAfter { get; set; }

        /// <summary>
        ///     How long to wait after failing before connecting to next redis instance (default 250ms)
        /// </summary>
        public TimeSpan WaitBetweenFailedHosts { get; set; }

        /// <summary>
        ///     How long to retry connecting to hosts before throwing (default 60 secs)
        /// </summary>
        public TimeSpan MaxWaitBetweenFailedHosts { get; set; }

        /// <summary>
        ///     How long to wait after consecutive failed connection attempts to master before forcing
        ///     a Sentinel to failover the current master (default 60 secs)
        /// </summary>
        public TimeSpan WaitBeforeForcingMasterFailover { get; set; }

        /// <summary>
        ///     The Max Connection time for Sentinel Worker (default 100ms)
        /// </summary>
        public int SentinelWorkerConnectTimeoutMs { get; set; }

        /// <summary>
        ///     The Max TCP Socket Receive time for Sentinel Worker (default 100ms)
        /// </summary>
        public int SentinelWorkerReceiveTimeoutMs { get; set; }

        /// <summary>
        ///     The Max TCP Socket Send time for Sentinel Worker (default 100ms)
        /// </summary>
        public int SentinelWorkerSendTimeoutMs { get; set; }

        /// <summary>
        ///     Reset client connections when Sentinel reports redis instance is subjectively down (default true)
        /// </summary>
        public bool ResetWhenSubjectivelyDown { get; set; }

        /// <summary>
        ///     Reset client connections when Sentinel reports redis instance is objectively down (default true)
        /// </summary>
        public bool ResetWhenObjectivelyDown { get; set; }

        /// <summary>
        ///     Initialize Sentinel Subscription and Configure Redis ClientsManager
        /// </summary>
        public IRedisClientManager Start() {
            lock (this._oLock) {
                for (var i = 0; i < this.SentinelHosts.Count; i++) {
                    var parts = this.SentinelHosts[i].SplitOnLast(':');
                    if (parts.Length == 1) {
                        this.SentinelHosts[i] = parts[0] + ":" + RedisConfig.DefaultPortSentinel;
                    }
                }

                if (this.ScanForOtherSentinels) {
                    this.RefreshActiveSentinels();
                }

                this.SentinelEndpoints = this.SentinelHosts
                                             .Select(x => RedisEndpoint.Create(x, RedisConfig.DefaultPortSentinel))
                                             .ToArray();

                var sentinelWorker = this.GetValidSentinelWorker();

                if (this.RedisManager == null || sentinelWorker == null) {
                    throw new Exception("Unable to resolve sentinels!");
                }

                return this.RedisManager;
            }
        }

        public void Dispose() {

            void LogAndDispose(IDisposable disposable) {
                try {
                    disposable?.Dispose();
                } catch (Exception ex) {
                    _logger.Error(ex, "Error disposing of '{0}'", disposable?.GetType().FullName ?? "");
                }
            }

            this._isDisposed = true;

            LogAndDispose(this.RedisManager);
            LogAndDispose(this._worker);
        }

        public List<string> GetActiveSentinelHosts(IEnumerable<string> sentinelHosts) {
            var activeSentinelHosts = new List<string>();
            foreach (var sentinelHost in sentinelHosts.ToArray()) {
                try {
                    _logger.Debug("Connecting to all available Sentinels to discover Active Sentinel Hosts...");

                    var endpoint = RedisEndpoint.Create(sentinelHost, RedisConfig.DefaultPortSentinel);
                    using (var sentinelWorker = new RedisSentinelWorker(this, endpoint)) {
                        var activeHosts = sentinelWorker.GetSentinelHosts(this.MasterName);

                        if (!activeSentinelHosts.Contains(sentinelHost)) {
                            activeSentinelHosts.Add(sentinelHost);
                        }

                        foreach (var activeHost in activeHosts) {
                            if (!activeSentinelHosts.Contains(activeHost)) {
                                activeSentinelHosts.Add(activeHost);
                            }
                        }
                    }

                    _logger.Debug("All active Sentinels Found: " + string.Join(", ", activeSentinelHosts));
                } catch (Exception ex) {
                    _logger.Error(ex, "Could not get active Sentinels from: {0}", sentinelHost);
                }
            }

            return activeSentinelHosts;
        }

        public void RefreshActiveSentinels() {
            var activeHosts = this.GetActiveSentinelHosts(this.SentinelHosts);
            if (activeHosts.Count == 0) {
                return;
            }

            lock (this.SentinelHosts) {
                this._lastSentinelsRefresh = DateTime.UtcNow;

                foreach (var value in activeHosts) {
                    if (!this.SentinelHosts.Contains(value)) {
                        this.SentinelHosts.Add(value);
                    }
                }

                this.SentinelEndpoints = this.SentinelHosts
                                             .Select(x => RedisEndpoint.Create(x, RedisConfig.DefaultPortSentinel))
                                             .ToArray();
            }
        }

        internal string[] ConfigureHosts(IEnumerable<string> hosts) {
            if (hosts == null) {
                return Array.Empty<string>();
            }

            return this.HostFilter == null
                ? hosts.ToArray()
                : hosts.Select(this.HostFilter).ToArray();
        }

        public SentinelInfo ResetClients() {
            var sentinelInfo = this.GetSentinelInfo();

            if (this.RedisManager == null) {
                _logger.Debug("Configuring initial Redis Clients: {0}", sentinelInfo);

                this.RedisManager = this.CreateRedisManager(sentinelInfo);
            } else {
                _logger.Debug("Failing over to Redis Clients: {0}", sentinelInfo);

                ((IRedisFailover)this.RedisManager).FailoverTo(this.ConfigureHosts(sentinelInfo.RedisMasters),
                    this.ConfigureHosts(sentinelInfo.RedisSlaves));
            }

            return sentinelInfo;
        }

        private IRedisClientManager CreateRedisManager(SentinelInfo sentinelInfo) {
            var masters = this.ConfigureHosts(sentinelInfo.RedisMasters);
            var slaves = this.ConfigureHosts(sentinelInfo.RedisSlaves);
            var redisManager = this.RedisManagerFactory(masters, slaves);

            var hasRedisResolver = (IHasRedisResolver)redisManager;
            hasRedisResolver.RedisResolver = new RedisSentinelResolver(this, masters, slaves);

            if (redisManager is IRedisFailover canFailover && this.OnFailover != null) {
                canFailover.OnFailover.Add(this.OnFailover);
            }

            return redisManager;
        }

        public IRedisClientManager GetRedisManager() {
            return this.RedisManager ?? (this.RedisManager = this.CreateRedisManager(this.GetSentinelInfo()));
        }

        private RedisSentinelWorker GetValidSentinelWorker() {
            if (this._isDisposed) {
                throw new ObjectDisposedException(this.GetType().Name);
            }

            if (this._worker != null) {
                lock (this._oLock) {
                    return this._worker;
                }
            }

            RedisException lastEx = null;

            while (this._worker == null && this.ShouldRetry()) {
                try {
                    this._worker = this.GetNextSentinel();
                    this.GetRedisManager();

                    this._worker.BeginListeningForConfigurationChanges();
                    this._failures = 0; // reset
                    return this._worker;
                } catch (RedisException ex) {
                    this.OnWorkerError?.Invoke(ex);

                    lastEx = ex;
                    this._worker = null;
                    this._failures++;
                    Interlocked.Increment(ref RedisState.TotalFailedSentinelWorkers);
                }
            }

            this._failures = 0; // reset
            Thread.Sleep(this.WaitBetweenFailedHosts);
            throw new RedisException("No Redis Sentinels were available", lastEx);
        }

        public RedisEndpoint GetMaster() {
            var sentinelWorker = this.GetValidSentinelWorker();
            var host = sentinelWorker.GetMasterHost(this.MasterName);

            if (this.ScanForOtherSentinels && DateTime.UtcNow - this._lastSentinelsRefresh > this.RefreshSentinelHostsAfter) {
                this.RefreshActiveSentinels();
            }

            return host != null
                ? RedisEndpoint.Create(this.HostFilter != null ? this.HostFilter(host) : host)
                : null;
        }

        public List<RedisEndpoint> GetSlaves() {
            var sentinelWorker = this.GetValidSentinelWorker();
            var hosts = sentinelWorker.GetSlaveHosts(this.MasterName);
            return this.ConfigureHosts(hosts).Select(x => RedisEndpoint.Create(x)).ToList();
        }

        /// <summary>
        ///     Check if GetValidSentinel should try the next sentinel server
        /// </summary>
        /// <remarks>This will be true if the failures is less than either RedisSentinel.MaxFailures or the # of sentinels, whatever is greater</remarks>
        private bool ShouldRetry() {
            return this._failures < Math.Max(_maxFailures, this.SentinelEndpoints.Length);
        }

        private RedisSentinelWorker GetNextSentinel() {
            RedisSentinelWorker disposeWorker = null;

            try {
                lock (this._oLock) {
                    if (this._worker != null) {
                        disposeWorker = this._worker;
                        this._worker = null;
                    }

                    if (++this._sentinelIndex >= this.SentinelEndpoints.Length) {
                        this._sentinelIndex = 0;
                    }

                    var sentinelWorker = new RedisSentinelWorker(this, this.SentinelEndpoints[this._sentinelIndex]) {
                        OnSentinelError = this.OnSentinelError
                    };

                    return sentinelWorker;
                }
            } finally {
                disposeWorker?.Dispose();
            }
        }

        private void OnSentinelError(Exception ex) {
            if (this._worker != null) {
                _logger.Error("Error on existing SentinelWorker, reconnecting...");

                this.OnWorkerError?.Invoke(ex);

                this._worker = this.GetNextSentinel();
                this._worker.BeginListeningForConfigurationChanges();
            }
        }

        public void ForceMasterFailover() {
            var sentinelWorker = this.GetValidSentinelWorker();
            sentinelWorker.ForceMasterFailover(this.MasterName);
        }

        public SentinelInfo GetSentinelInfo() {
            var sentinelWorker = this.GetValidSentinelWorker();
            return sentinelWorker.GetSentinelInfo();
        }

    }

}
