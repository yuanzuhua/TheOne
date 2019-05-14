using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Security.Authentication;
using TheOne.Redis.Common;
using TheOne.Redis.External;

namespace TheOne.Redis {

    public class RedisEndpoint {

        public RedisEndpoint() {
            this.Host = RedisConfig.DefaultHost;
            this.Port = RedisConfig.DefaultPort;
            this.Db = RedisConfig.DefaultDb;

            this.ConnectTimeout = RedisConfig.DefaultConnectTimeout;
            this.SendTimeout = RedisConfig.DefaultSendTimeout;
            this.ReceiveTimeout = RedisConfig.DefaultReceiveTimeout;
            this.RetryTimeout = RedisConfig.DefaultRetryTimeout;
            this.IdleTimeoutSecs = RedisConfig.DefaultIdleTimeoutSecs;
        }

        public RedisEndpoint(string host, int port, string password = null, long db = RedisConfig.DefaultDb)
            : this() {
            this.Host = host;
            this.Port = port;
            this.Password = password;
            this.Db = db;
        }

        /// <summary>
        ///     If this is an SSL connection
        /// </summary>
        public bool Ssl { get; set; }

        public SslProtocols? SslProtocols { get; set; }

        /// <summary>
        ///     Timeout in ms for making a TCP Socket connection
        /// </summary>
        public int ConnectTimeout { get; set; }

        /// <summary>
        ///     Timeout in ms for making a synchronous TCP Socket Send
        /// </summary>
        public int SendTimeout { get; set; }

        /// <summary>
        ///     Timeout in ms for waiting for a synchronous TCP Socket Receive
        /// </summary>
        public int ReceiveTimeout { get; set; }

        /// <summary>
        ///     RedisClient will transparently retry failed Redis operations due to Socket and I/O Exceptions
        ///     in an exponential back-off starting from 10ms up until the RetryTimeout of 10000ms
        /// </summary>
        public int RetryTimeout { get; set; }

        /// <summary>
        ///     Timeout in Seconds for an Idle connection to be considered active
        /// </summary>
        public int IdleTimeoutSecs { get; set; }

        /// <summary>
        ///     The Redis DB this connection should be set to
        /// </summary>
        public long Db { get; set; }

        /// <summary>
        ///     A text alias to specify for this connection for analytic purposes
        /// </summary>
        public string Client { get; set; }

        /// <summary>
        ///     UrlEncoded version of the Password for this connection
        /// </summary>
        public string Password { get; set; }

        public bool RequiresAuth => !string.IsNullOrEmpty(this.Password);

        /// <summary>
        ///     Use a custom prefix for Redis internal index collections
        /// </summary>
        public string NamespacePrefix { get; set; }

        public string Host { get; set; }
        public int Port { get; set; }

        public string GetHostString() {
            return string.Format("{0}:{1}", this.Host, this.Port);
        }

        public static List<RedisEndpoint> Create(IEnumerable<string> hosts) {
            return hosts == null
                ? new List<RedisEndpoint>()
                : hosts.Select(x => Create(x)).ToList();
        }

        // ReSharper disable once CyclomaticComplexity
        // ReSharper disable once MethodTooLong
        public static RedisEndpoint Create(string connectionString, int? defaultPort = null) {
            if (connectionString == null) {
                throw new ArgumentNullException(nameof(connectionString));
            }

            if (connectionString.StartsWith("redis://")) {
                connectionString = connectionString.Substring("redis://".Length);
            }

            var domainParts = connectionString.SplitOnLast('@');
            var qsParts = domainParts.Last().SplitOnFirst('?');
            var hostParts = qsParts[0].SplitOnLast(':');
            var useDefaultPort = true;
            var port = defaultPort.GetValueOrDefault(RedisConfig.DefaultPort);
            if (hostParts.Length > 1) {
                port = int.Parse(hostParts[1]);
                useDefaultPort = false;
            }

            var endpoint = new RedisEndpoint(hostParts[0], port);
            if (domainParts.Length > 1) {
                var authParts = domainParts[0].SplitOnFirst(':');
                if (authParts.Length > 1) {
                    endpoint.Client = authParts[0];
                }

                endpoint.Password = authParts.Last();
            }

            if (qsParts.Length > 1) {
                var qsParams = qsParts[1].Split('&');
                foreach (var param in qsParams) {
                    var entry = param.Split('=');
                    var value = entry.Length > 1 ? WebUtility.UrlDecode(entry[1]) : null;
                    if (value == null) {
                        continue;
                    }

                    var name = entry[0].ToLower();
                    switch (name) {
                        case "db":
                            endpoint.Db = int.Parse(value);
                            break;
                        case "ssl":
                            endpoint.Ssl = bool.Parse(value);
                            if (useDefaultPort) {
                                endpoint.Port = RedisConfig.DefaultPortSsl;
                            }

                            break;
                        case "sslprotocols":
                            value = value?.Replace("|", ",");
                            if (!Enum.TryParse(value, true, out SslProtocols protocols)) {
                                var message = "Keyword '" + name + "' requires an SslProtocol value (multiple values separated by '|').";
                                throw new ArgumentOutOfRangeException(message);
                            }

                            endpoint.SslProtocols = protocols;
                            break;
                        case "client":
                            endpoint.Client = value;
                            break;
                        case "password":
                            endpoint.Password = value;
                            break;
                        case "namespaceprefix":
                            endpoint.NamespacePrefix = value;
                            break;
                        case "connecttimeout":
                            endpoint.ConnectTimeout = int.Parse(value);
                            break;
                        case "sendtimeout":
                            endpoint.SendTimeout = int.Parse(value);
                            break;
                        case "receivetimeout":
                            endpoint.ReceiveTimeout = int.Parse(value);
                            break;
                        case "retrytimeout":
                            endpoint.RetryTimeout = int.Parse(value);
                            break;
                        case "idletimeout":
                        case "idletimeoutsecs":
                            endpoint.IdleTimeoutSecs = int.Parse(value);
                            break;
                    }
                }
            }

            return endpoint;
        }

        #region override

        public override string ToString() {
            var sb = StringBuilderCache.Acquire();
            sb.AppendFormat("{0}:{1}", this.Host, this.Port);

            var args = new List<string>();
            if (this.Client != null) {
                args.Add("Client=" + this.Client);
            }

            if (this.Password != null) {
                args.Add("Password=" + WebUtility.UrlEncode(this.Password));
            }

            if (this.Db != RedisConfig.DefaultDb) {
                args.Add("Db=" + this.Db);
            }

            if (this.Ssl) {
                args.Add("Ssl=true");
            }

            if (this.SslProtocols != null) {
                args.Add("SslProtocols=" + this.SslProtocols);
            }

            if (this.ConnectTimeout != RedisConfig.DefaultConnectTimeout) {
                args.Add("ConnectTimeout=" + this.ConnectTimeout);
            }

            if (this.SendTimeout != RedisConfig.DefaultSendTimeout) {
                args.Add("SendTimeout=" + this.SendTimeout);
            }

            if (this.ReceiveTimeout != RedisConfig.DefaultReceiveTimeout) {
                args.Add("ReceiveTimeout=" + this.ReceiveTimeout);
            }

            if (this.RetryTimeout != RedisConfig.DefaultRetryTimeout) {
                args.Add("RetryTimeout=" + this.RetryTimeout);
            }

            if (this.IdleTimeoutSecs != RedisConfig.DefaultIdleTimeoutSecs) {
                args.Add("IdleTimeoutSecs=" + this.IdleTimeoutSecs);
            }

            if (this.NamespacePrefix != null) {
                args.Add("NamespacePrefix=" + WebUtility.UrlEncode(this.NamespacePrefix));
            }

            if (args.Count > 0) {
                sb.Append("?").Append(string.Join("&", args));
            }

            return StringBuilderCache.GetStringAndRelease(sb);
        }

        protected bool Equals(RedisEndpoint other) {
            return string.Equals(this.Host, other.Host)
                   && this.Port == other.Port
                   && this.Ssl.Equals(other.Ssl)
                   && this.SslProtocols.Equals(other.SslProtocols)
                   && this.ConnectTimeout == other.ConnectTimeout
                   && this.SendTimeout == other.SendTimeout
                   && this.ReceiveTimeout == other.ReceiveTimeout
                   && this.RetryTimeout == other.RetryTimeout
                   && this.IdleTimeoutSecs == other.IdleTimeoutSecs
                   && this.Db == other.Db
                   && string.Equals(this.Client, other.Client)
                   && string.Equals(this.Password, other.Password)
                   && string.Equals(this.NamespacePrefix, other.NamespacePrefix);
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

            return this.Equals((RedisEndpoint)obj);
        }

        public override int GetHashCode() {
            // ReSharper disable NonReadonlyMemberInGetHashCode
            unchecked {
                var hashCode = this.Host != null ? this.Host.GetHashCode() : 0;
                hashCode = (hashCode * 397) ^ this.Port;
                hashCode = (hashCode * 397) ^ this.Ssl.GetHashCode();
                hashCode = (hashCode * 397) ^ this.SslProtocols.GetHashCode();
                hashCode = (hashCode * 397) ^ this.ConnectTimeout;
                hashCode = (hashCode * 397) ^ this.SendTimeout;
                hashCode = (hashCode * 397) ^ this.ReceiveTimeout;
                hashCode = (hashCode * 397) ^ this.RetryTimeout;
                hashCode = (hashCode * 397) ^ this.IdleTimeoutSecs;
                hashCode = (hashCode * 397) ^ this.Db.GetHashCode();
                hashCode = (hashCode * 397) ^ (this.Client != null ? this.Client.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (this.Password != null ? this.Password.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (this.NamespacePrefix != null ? this.NamespacePrefix.GetHashCode() : 0);
                return hashCode;
            }
            // ReSharper restore NonReadonlyMemberInGetHashCode
        }

        #endregion

    }

}
