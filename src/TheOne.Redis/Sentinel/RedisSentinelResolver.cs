using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using TheOne.Logging;
using TheOne.Redis.Client;
using TheOne.Redis.Client.Internal;
using TheOne.Redis.ClientManager;

namespace TheOne.Redis.Sentinel {

        /// <inheritdoc />
    public class RedisSentinelResolver : IRedisResolverExtended {

        private static readonly ILog _logger = LogProvider.GetLogger(typeof(RedisSentinelResolver));
        private readonly HashSet<RedisEndpoint> _allHosts = new HashSet<RedisEndpoint>();
        private readonly object _oLock = new object();
        private readonly RedisSentinel _sentinel;
        private string _lastInvalidMasterHost;
        private long _lastValidMasterTicks = DateTime.UtcNow.Ticks;

        /// <inheritdoc />
        public RedisSentinelResolver(RedisSentinel sentinel)
            : this(sentinel, Array.Empty<RedisEndpoint>(), Array.Empty<RedisEndpoint>()) { }

        /// <inheritdoc />
        public RedisSentinelResolver(RedisSentinel sentinel, IEnumerable<string> masters, IEnumerable<string> slaves)
            : this(sentinel, RedisEndpoint.Create(masters), RedisEndpoint.Create(slaves)) { }

        /// <inheritdoc />
        public RedisSentinelResolver(RedisSentinel sentinel, IEnumerable<RedisEndpoint> masters, IEnumerable<RedisEndpoint> slaves) {
            this._sentinel = sentinel;
            this.ResetMasters(masters.ToList());
            this.ResetSlaves(slaves.ToList());
            this.ClientFactory = RedisConfig.ClientFactory;
        }

        public RedisEndpoint[] Masters { get; private set; }

        public RedisEndpoint[] Slaves { get; private set; }

        private DateTime LastValidMasterFromSentinelAt {
            get => new DateTime(Interlocked.Read(ref this._lastValidMasterTicks), DateTimeKind.Utc);
            set => Interlocked.Exchange(ref this._lastValidMasterTicks, value.Ticks);
        }

        /// <inheritdoc />
        public Func<RedisEndpoint, RedisClient> ClientFactory { get; set; }

        /// <inheritdoc />
        public int ReadWriteHostsCount { get; private set; }
        /// <inheritdoc />
        public int ReadOnlyHostsCount { get; private set; }

        /// <inheritdoc />
        public virtual void ResetMasters(IEnumerable<string> hosts) {
            this.ResetMasters(RedisEndpoint.Create(hosts));
        }

        /// <inheritdoc />
        public virtual void ResetSlaves(IEnumerable<string> hosts) {
            this.ResetSlaves(RedisEndpoint.Create(hosts));
        }

        /// <inheritdoc />
        public RedisClient CreateMasterClient(int desiredIndex) {
            return this.CreateRedisClient(this.GetReadWriteHost(desiredIndex), true);
        }

        /// <inheritdoc />
        public RedisClient CreateSlaveClient(int desiredIndex) {
            return this.CreateRedisClient(this.GetReadOnlyHost(desiredIndex), false);
        }

        /// <inheritdoc />
        public RedisEndpoint GetReadWriteHost(int desiredIndex) {
            return this._sentinel.GetMaster() ?? this.Masters[desiredIndex % this.Masters.Length];
        }

        /// <inheritdoc />
        public RedisEndpoint GetReadOnlyHost(int desiredIndex) {
            List<RedisEndpoint> slavesEndpoints = this._sentinel.GetSlaves();
            if (slavesEndpoints.Count > 0) {
                return slavesEndpoints[desiredIndex % slavesEndpoints.Count];
            }

            return this.ReadOnlyHostsCount > 0
                ? this.Slaves[desiredIndex % this.Slaves.Length]
                : this.GetReadWriteHost(desiredIndex);
        }

        /// <inheritdoc />
        public virtual RedisClient CreateRedisClient(RedisEndpoint config, bool master) {
            RedisClient client = this.ClientFactory(config);
            if (master) {
                var role = RedisServerRole.Unknown;
                try {
                    role = client.GetServerRole();
                    if (role == RedisServerRole.Master) {
                        this.LastValidMasterFromSentinelAt = DateTime.UtcNow;
                        return client;
                    }
                } catch (Exception ex) {
                    Interlocked.Increment(ref RedisState.TotalInvalidMasters);

                    if (client.GetHostString() == this._lastInvalidMasterHost) {
                        lock (this._oLock) {
                            if (DateTime.UtcNow - this.LastValidMasterFromSentinelAt > this._sentinel.WaitBeforeForcingMasterFailover) {
                                this._lastInvalidMasterHost = null;
                                this.LastValidMasterFromSentinelAt = DateTime.UtcNow;

                                _logger.Error(string.Format(
                                        "Valid master was not found at '{0}' within '{1}'. Sending SENTINEL failover...",
                                        client.GetHostString(),
                                        this._sentinel.WaitBeforeForcingMasterFailover),
                                    ex);

                                Interlocked.Increment(ref RedisState.TotalForcedMasterFailovers);

                                this._sentinel.ForceMasterFailover();
                                Thread.Sleep(this._sentinel.WaitBetweenFailedHosts);
                                role = client.GetServerRole();
                            }
                        }
                    } else {
                        this._lastInvalidMasterHost = client.GetHostString();
                    }
                }

                if (role != RedisServerRole.Master && RedisConfig.VerifyMasterConnections) {
                    try {
                        Stopwatch stopwatch = Stopwatch.StartNew();
                        while (true) {
                            try {
                                RedisEndpoint masterConfig = this._sentinel.GetMaster();
                                RedisClient masterClient = this.ClientFactory(masterConfig);
                                masterClient.ConnectTimeout = this._sentinel.SentinelWorkerConnectTimeoutMs;

                                RedisServerRole masterRole = masterClient.GetServerRole();
                                if (masterRole == RedisServerRole.Master) {
                                    this.LastValidMasterFromSentinelAt = DateTime.UtcNow;
                                    return masterClient;
                                }

                                Interlocked.Increment(ref RedisState.TotalInvalidMasters);
                            } catch {
                                /* Ignore errors until MaxWait */
                            }

                            if (stopwatch.Elapsed > this._sentinel.MaxWaitBetweenFailedHosts) {
                                throw new TimeoutException(string.Format("Max Wait Between Sentinel Lookups Elapsed: {0}",
                                    this._sentinel.MaxWaitBetweenFailedHosts.ToString()));
                            }

                            Thread.Sleep(this._sentinel.WaitBetweenFailedHosts);
                        }
                    } catch (Exception ex) {
                        _logger.Error(string.Format("Redis Master Host '{0}' is {1}. Resetting allHosts...", config.GetHostString(), role),
                            ex);

                        var newMasters = new List<RedisEndpoint>();
                        var newSlaves = new List<RedisEndpoint>();
                        RedisClient masterClient = null;
                        foreach (RedisEndpoint hostConfig in this._allHosts) {
                            try {
                                RedisClient testClient = this.ClientFactory(hostConfig);
                                testClient.ConnectTimeout = RedisConfig.HostLookupTimeoutMs;
                                RedisServerRole testRole = testClient.GetServerRole();
                                switch (testRole) {
                                    case RedisServerRole.Master:
                                        newMasters.Add(hostConfig);
                                        if (masterClient == null) {
                                            masterClient = testClient;
                                        }

                                        break;
                                    case RedisServerRole.Slave:
                                        newSlaves.Add(hostConfig);
                                        break;
                                }

                            } catch {
                                /* skip past invalid master connections */
                            }
                        }

                        if (masterClient == null) {
                            Interlocked.Increment(ref RedisState.TotalNoMastersFound);
                            var errorMsg = "No master found in: " + string.Join(", ", this._allHosts.Select(x => x.GetHostString()));
                            _logger.Error(errorMsg);
                            throw new Exception(errorMsg);
                        }

                        this.ResetMasters(newMasters);
                        this.ResetSlaves(newSlaves);
                        return masterClient;
                    }
                }
            }

            return client;
        }

        public virtual void ResetMasters(List<RedisEndpoint> newMasters) {
            if (newMasters == null || newMasters.Count == 0) {
                throw new ArgumentException("Must provide at least 1 master");
            }

            this.Masters = newMasters.ToArray();
            this.ReadWriteHostsCount = this.Masters.Length;
            foreach (RedisEndpoint value in newMasters) {
                this._allHosts.Add(value);
            }

            if (_logger.IsDebugEnabled()) {
                _logger.Debug("New Redis Masters: " + string.Join(", ", this.Masters.Select(x => x.GetHostString())));
            }
        }

        public virtual void ResetSlaves(List<RedisEndpoint> newSlaves) {
            this.Slaves = newSlaves?.ToArray() ?? Array.Empty<RedisEndpoint>();
            this.ReadOnlyHostsCount = this.Slaves.Length;
            foreach (RedisEndpoint value in this.Slaves) {
                this._allHosts.Add(value);
            }

            if (_logger.IsDebugEnabled()) {
                _logger.Debug("New Redis Slaves: " + string.Join(", ", this.Slaves.Select(x => x.GetHostString())));
            }
        }

    }

}
