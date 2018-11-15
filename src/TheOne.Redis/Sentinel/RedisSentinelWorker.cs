using System;
using System.Collections.Generic;
using System.Threading;
using TheOne.Logging;
using TheOne.Redis.Client;
using TheOne.Redis.Client.Internal;
using TheOne.Redis.ClientManager;
using TheOne.Redis.PubSub;

namespace TheOne.Redis.Sentinel {

    internal class RedisSentinelWorker : IDisposable {

        private static readonly ILog _logger = LogProvider.GetLogger(typeof(RedisSentinelWorker));
        private static int _idCounter;
        private readonly object _oLock = new object();
        private readonly RedisSentinel _sentinel;
        private readonly RedisClient _sentinelClient;
        private RedisPubSubServer _sentinelPubSub;
        public Action<Exception> OnSentinelError;

        public RedisSentinelWorker(RedisSentinel sentinel, RedisEndpoint sentinelEndpoint) {
            this.Id = Interlocked.Increment(ref _idCounter);
            this._sentinel = sentinel;
            this._sentinelClient = new RedisClient(sentinelEndpoint) {
                Db = 0, // Sentinel Servers doesn't support DB, reset to 0
                ConnectTimeout = sentinel.SentinelWorkerConnectTimeoutMs,
                ReceiveTimeout = sentinel.SentinelWorkerReceiveTimeoutMs,
                SendTimeout = sentinel.SentinelWorkerSendTimeoutMs
            };

            if (_logger.IsDebugEnabled()) {
                _logger.Debug(string.Format("Set up Redis Sentinel on {0}", sentinelEndpoint));
            }
        }

        public int Id { get; }

        public void Dispose() {
            void LogAndDispose(IDisposable disposable) {
                try {
                    disposable?.Dispose();
                } catch (Exception e) {
                    _logger.Error($"Error disposing of '{disposable?.GetType().FullName ?? ""}'", e);
                }
            }

            LogAndDispose(this._sentinelClient);
            LogAndDispose(this._sentinelPubSub);
        }

        /// <summary>
        ///     Event that is fired when the sentinel subscription raises an event
        /// </summary>
        private void SentinelMessageReceived(string channel, string message) {
            if (_logger.IsDebugEnabled()) {
                _logger.Debug(string.Format("Received '{0}' on channel '{1}' from Sentinel", channel, message));
            }

            // {+|-}sdown is the event for server coming up or down
            var c = channel.ToLower();
            var isSubjectivelyDown = c.Contains("sdown");
            if (isSubjectivelyDown) {
                Interlocked.Increment(ref RedisState.TotalSubjectiveServersDown);
            }

            var isObjectivelyDown = c.Contains("odown");
            if (isObjectivelyDown) {
                Interlocked.Increment(ref RedisState.TotalObjectiveServersDown);
            }

            if (c == "+failover-end"
                || c == "+switch-master"
                || this._sentinel.ResetWhenSubjectivelyDown && isSubjectivelyDown
                || this._sentinel.ResetWhenObjectivelyDown && isObjectivelyDown) {
                if (_logger.IsDebugEnabled()) {
                    _logger.Debug($"Sentinel detected server down/up '{channel}' with message: {message}");
                }

                this._sentinel.ResetClients();
            }

            this._sentinel.OnSentinelMessageReceived?.Invoke(channel, message);
        }

        internal SentinelInfo GetSentinelInfo() {
            var masterHost = this.GetMasterHostInternal(this._sentinel.MasterName);
            if (masterHost == null) {
                throw new RedisException("Redis Sentinel is reporting no master is available");
            }

            var sentinelInfo = new SentinelInfo(this._sentinel.MasterName,
                new[] { masterHost },
                this.GetSlaveHosts(this._sentinel.MasterName));

            return sentinelInfo;
        }

        internal string GetMasterHost(string masterName) {
            try {
                return this.GetMasterHostInternal(masterName);
            } catch (Exception ex) {
                this.OnSentinelError?.Invoke(ex);

                return null;
            }
        }

        private string GetMasterHostInternal(string masterName) {
            List<string> masterInfo;
            lock (this._oLock) {
                masterInfo = this._sentinelClient.SentinelGetMasterAddrByName(masterName);
            }

            return masterInfo.Count > 0
                ? this.SanitizeMasterConfig(masterInfo)
                : null;
        }

        private string SanitizeMasterConfig(List<string> masterInfo) {
            var ip = masterInfo[0];
            var port = masterInfo[1];

            if (this._sentinel.IpAddressMap.TryGetValue(ip, out var aliasIp)) {
                ip = aliasIp;
            }

            return $"{ip}:{port}";
        }

        internal List<string> GetSentinelHosts(string masterName) {
            List<Dictionary<string, string>> sentinelSentinels;
            lock (this._oLock) {
                sentinelSentinels = this._sentinelClient.SentinelSentinels(this._sentinel.MasterName);
            }

            return this.SanitizeHostsConfig(sentinelSentinels);
        }

        internal List<string> GetSlaveHosts(string masterName) {
            List<Dictionary<string, string>> sentinelSlaves;

            lock (this._oLock) {
                sentinelSlaves = this._sentinelClient.SentinelSlaves(this._sentinel.MasterName);
            }

            return this.SanitizeHostsConfig(sentinelSlaves);
        }

        private List<string> SanitizeHostsConfig(IEnumerable<Dictionary<string, string>> slaves) {
            var servers = new List<string>();
            foreach (Dictionary<string, string> slave in slaves) {
                slave.TryGetValue("flags", out var flags);
                slave.TryGetValue("ip", out var ip);
                slave.TryGetValue("port", out var port);

                if (this._sentinel.IpAddressMap.TryGetValue(ip ?? throw new ArgumentException("ip is null"), out var aliasIp)) {
                    ip = aliasIp;
                } else if (ip == "127.0.0.1") {
                    ip = this._sentinelClient.Host;
                }

                if (ip != null && port != null && !flags.Contains("s_down") && !flags.Contains("o_down")) {
                    servers.Add($"{ip}:{port}");
                }
            }

            return servers;
        }

        public void BeginListeningForConfigurationChanges() {
            try {
                lock (this._oLock) {
                    if (this._sentinelPubSub == null) {
                        var sentinelManager = new BasicRedisClientManager(this._sentinel.SentinelHosts, this._sentinel.SentinelHosts) {
                            // Use BasicRedisResolver which doesn't validate non-Master Sentinel instances
                            RedisResolver = new BasicRedisResolver(this._sentinel.SentinelEndpoints, this._sentinel.SentinelEndpoints)
                        };
                        this._sentinelPubSub = new RedisPubSubServer(sentinelManager) {
                            HeartbeatInterval = null,
                            IsSentinelSubscription = true,
                            ChannelsMatching = new[] { RedisPubSubServer.AllChannelsWildCard },
                            OnMessage = this.SentinelMessageReceived
                        };
                    }
                }

                this._sentinelPubSub.Start();
            } catch (Exception ex) {
                _logger.Error($"Error Subscribing to Redis Channel on {this._sentinelClient.Host}:{this._sentinelClient.Port}", ex);

                this.OnSentinelError?.Invoke(ex);
            }
        }

        public void ForceMasterFailover(string masterName) {
            lock (this._oLock) {
                this._sentinelClient.SentinelFailover(masterName);
            }
        }

    }

}
