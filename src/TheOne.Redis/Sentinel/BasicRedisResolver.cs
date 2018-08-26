using System;
using System.Collections.Generic;
using System.Linq;
using TheOne.Logging;
using TheOne.Redis.Client;
using TheOne.Redis.ClientManager;

namespace TheOne.Redis.Sentinel {

    public class BasicRedisResolver : IRedisResolverExtended {

        private static readonly ILog _logger = LogProvider.GetLogger(typeof(BasicRedisResolver));

        public BasicRedisResolver(IEnumerable<RedisEndpoint> masters, IEnumerable<RedisEndpoint> slaves) {
            this.ResetMasters(masters.ToList());
            this.ResetSlaves(slaves.ToList());
            this.ClientFactory = RedisConfig.ClientFactory;
        }

        public RedisEndpoint[] Masters { get; private set; }

        public RedisEndpoint[] Slaves { get; private set; }

        public Func<RedisEndpoint, RedisClient> ClientFactory { get; set; }

        public int ReadWriteHostsCount { get; private set; }
        public int ReadOnlyHostsCount { get; private set; }

        public virtual void ResetMasters(IEnumerable<string> hosts) {
            this.ResetMasters(RedisEndpoint.Create(hosts));
        }

        public virtual void ResetSlaves(IEnumerable<string> hosts) {
            this.ResetSlaves(RedisEndpoint.Create(hosts));
        }

        public RedisClient CreateMasterClient(int desiredIndex) {
            return this.CreateRedisClient(this.GetReadWriteHost(desiredIndex), true);
        }

        public RedisClient CreateSlaveClient(int desiredIndex) {
            return this.CreateRedisClient(this.GetReadOnlyHost(desiredIndex), false);
        }

        public RedisClient CreateRedisClient(RedisEndpoint config, bool master) {
            return this.ClientFactory(config);
        }

        public RedisEndpoint GetReadWriteHost(int desiredIndex) {
            return this.Masters[desiredIndex % this.Masters.Length];
        }

        public RedisEndpoint GetReadOnlyHost(int desiredIndex) {
            return this.ReadOnlyHostsCount > 0
                ? this.Slaves[desiredIndex % this.Slaves.Length]
                : this.GetReadWriteHost(desiredIndex);
        }

        public virtual void ResetMasters(List<RedisEndpoint> newMasters) {
            if (newMasters == null || newMasters.Count == 0) {
                throw new Exception("Must provide at least 1 master");
            }

            this.Masters = newMasters.ToArray();
            this.ReadWriteHostsCount = this.Masters.Length;

            if (_logger.IsDebugEnabled()) {
                _logger.Debug("New Redis Masters: " + string.Join(", ", this.Masters.Select(x => x.GetHostString())));
            }
        }

        public virtual void ResetSlaves(List<RedisEndpoint> newSlaves) {
            this.Slaves = newSlaves?.ToArray() ?? Array.Empty<RedisEndpoint>();
            this.ReadOnlyHostsCount = this.Slaves.Length;

            if (_logger.IsDebugEnabled()) {
                _logger.Debug("New Redis Slaves: " + string.Join(", ", this.Slaves.Select(x => x.GetHostString())));
            }
        }

    }

}
