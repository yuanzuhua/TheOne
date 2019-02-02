using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using TheOne.Logging;
using TheOne.Redis.Client;
using TheOne.Redis.Client.Internal;

namespace TheOne.Redis.ClientManager {

    /// <inheritdoc />
    public class RedisResolver : IRedisResolverExtended {

        private static readonly ILog _logger = LogProvider.GetLogger(typeof(RedisResolver));
        private readonly HashSet<RedisEndpoint> _allHosts = new HashSet<RedisEndpoint>();

        /// <inheritdoc />
        public RedisResolver()
            : this(Array.Empty<RedisEndpoint>(), Array.Empty<RedisEndpoint>()) { }

        /// <inheritdoc />
        public RedisResolver(IEnumerable<string> masters, IEnumerable<string> slaves)
            : this(RedisEndpoint.Create(masters), RedisEndpoint.Create(slaves)) { }

        /// <inheritdoc />
        public RedisResolver(IEnumerable<RedisEndpoint> masters, IEnumerable<RedisEndpoint> slaves) {
            this.ResetMasters(masters.ToList());
            this.ResetSlaves(slaves.ToList());
            this.ClientFactory = RedisConfig.ClientFactory;
        }

        public RedisEndpoint[] Masters { get; private set; }

        public RedisEndpoint[] Slaves { get; private set; }

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
        public virtual RedisClient CreateRedisClient(RedisEndpoint config, bool master) {
            var client = this.ClientFactory(config);

            if (!master || !RedisConfig.VerifyMasterConnections) {
                return client;
            }

            var role = client.GetServerRole();
            if (role == RedisServerRole.Master) {
                return client;
            }

            Interlocked.Increment(ref RedisState.TotalInvalidMasters);

            _logger.Warn("Redis Master Host '{0}' is {1}. Resetting allHosts...", config.GetHostString(), role);

            var newMasters = new List<RedisEndpoint>();
            var newSlaves = new List<RedisEndpoint>();
            RedisClient masterClient = null;
            foreach (var hostConfig in this._allHosts) {
                try {
                    var testClient = this.ClientFactory(hostConfig);
                    testClient.ConnectTimeout = RedisConfig.HostLookupTimeoutMs;
                    var testRole = testClient.GetServerRole();
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
                    /* skip */
                }
            }

            if (masterClient == null) {
                Interlocked.Increment(ref RedisState.TotalNoMastersFound);

                var errorMsg = "No master found in: " + string.Join(", ", this._allHosts.Select(x => x.GetHostString()));

                _logger.Error(errorMsg);

                throw new RedisException(errorMsg);
            }

            this.ResetMasters(newMasters);
            this.ResetSlaves(newSlaves);
            return masterClient;
        }

        /// <inheritdoc />
        public RedisEndpoint GetReadWriteHost(int desiredIndex) {
            return this.Masters[desiredIndex % this.Masters.Length];
        }

        /// <inheritdoc />
        public RedisEndpoint GetReadOnlyHost(int desiredIndex) {
            return this.ReadOnlyHostsCount > 0
                ? this.Slaves[desiredIndex % this.Slaves.Length]
                : this.GetReadWriteHost(desiredIndex);
        }

        public virtual void ResetMasters(List<RedisEndpoint> newMasters) {
            if (newMasters == null || newMasters.Count == 0) {
                throw new InvalidOperationException("Must provide at least 1 master");
            }

            this.Masters = newMasters.ToArray();
            this.ReadWriteHostsCount = this.Masters.Length;
            foreach (var value in newMasters) {
                this._allHosts.Add(value);
            }

            _logger.Debug("New Redis Masters: " + string.Join(", ", this.Masters.Select(x => x.GetHostString())));
        }

        public virtual void ResetSlaves(List<RedisEndpoint> newSlaves) {
            this.Slaves = newSlaves?.ToArray() ?? Array.Empty<RedisEndpoint>();
            this.ReadOnlyHostsCount = this.Slaves.Length;
            foreach (var value in this.Slaves) {
                this._allHosts.Add(value);
            }

            _logger.Debug("New Redis Slaves: " + string.Join(", ", this.Slaves.Select(x => x.GetHostString())));
        }

    }

}
