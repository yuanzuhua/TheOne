using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Threading;
using TheOne.Logging;
using TheOne.Redis.Client.Internal;
using TheOne.Redis.ClientManager;
using TheOne.Redis.Common;
using TheOne.Redis.Pipeline;
using TheOne.Redis.PubSub;

namespace TheOne.Redis.Client {

    /// <summary>
    ///     This class contains all the common operations for the RedisClient.
    ///     <para>
    ///         The client contains a 1:1 mapping of c# methods to redis operations of the same name.
    ///     </para>
    ///     <para>
    ///         All redis calls on a single instances write to the same Socket.
    ///     </para>
    ///     <para>
    ///         If used in multiple threads (or async Tasks) at the same time
    ///         you will find that commands are not executed properly by redis
    ///         and wont be able to (json) serialize the data that comes back.
    ///     </para>
    /// </summary>
    public partial class RedisNativeClient : IRedisNativeClient {

        internal const int Success = 1;
        internal const int OneGb = 1073741824;

        private const int _yes = 1;
        private const int _no = 0;
        private static readonly ILog _logger = LogProvider.GetLogger(typeof(RedisNativeClient));
        private readonly byte[] _endData = { (byte)'\r', (byte)'\n' };

        /// <summary>
        ///     Used to manage connection pooling
        /// </summary>
        private int _active;

        private int _clientPort;
        private string _lastCommand;
        private SocketException _lastSocketException;
        private IRedisPipelineShared _pipeline;
        private TimeSpan _retryTimeout;
        private IRedisTransactionBase _transaction;
        protected BufferedStream BStream;

        internal long DeactivatedAtTicks;
        internal long LastConnectedAtTimestamp;
        protected Socket Socket;
        protected SslStream SslStream;

        /// <inheritdoc />
        public RedisNativeClient(string connectionString) : this(RedisEndpoint.Create(connectionString)) { }

        /// <inheritdoc />
        public RedisNativeClient(RedisEndpoint config) {
            this.Init(config);
        }

        /// <inheritdoc />
        public RedisNativeClient(string host, int port, string password = null, long db = RedisConfig.DefaultDb) {
            if (host == null) {
                throw new ArgumentNullException(nameof(host));
            }

            this.Init(new RedisEndpoint(host, port, password, db));
        }

        /// <inheritdoc />
        public RedisNativeClient() : this(RedisConfig.DefaultHost, RedisConfig.DefaultPort) { }

        public DateTime? DeactivatedAt {
            get => this.DeactivatedAtTicks != 0
                ? new DateTime(Interlocked.Read(ref this.DeactivatedAtTicks), DateTimeKind.Utc)
                : (DateTime?)null;
            set {
                var ticksValue = value?.Ticks ?? 0;
                Interlocked.Exchange(ref this.DeactivatedAtTicks, ticksValue);
            }
        }

        public bool HadExceptions => this.DeactivatedAtTicks > 0;

        internal bool Active {
            get => Interlocked.CompareExchange(ref this._active, 0, 0) == _yes;
            set => Interlocked.Exchange(ref this._active, value ? _yes : _no);
        }

        internal IHandleClientDispose ClientManager { get; set; }

        public long Id { get; set; }

        public string Host { get; private set; }
        public int Port { get; private set; }
        public bool Ssl { get; private set; }
        public SslProtocols? SslProtocols { get; private set; }

        /// <summary>
        ///     Gets or sets object key prefix.
        /// </summary>
        public string NamespacePrefix { get; set; }

        public int ConnectTimeout { get; set; }

        public int RetryTimeout {
            get => (int)this._retryTimeout.TotalMilliseconds;
            set => this._retryTimeout = TimeSpan.FromMilliseconds(value);
        }

        public int RetryCount { get; set; }
        public int SendTimeout { get; set; }
        public int ReceiveTimeout { get; set; }
        public string Password { get; set; }
        public string Client { get; set; }
        public int IdleTimeoutSecs { get; set; }

        public Action<IRedisNativeClient> ConnectionFilter { get; set; }

        internal IRedisTransactionBase Transaction {
            get => this._transaction;
            set {
                if (value != null) {
                    this.AssertConnectedSocket();
                }

                this._transaction = value;
            }
        }

        internal IRedisPipelineShared Pipeline {
            get => this._pipeline;
            set {
                if (value != null) {
                    this.AssertConnectedSocket();
                }

                this._pipeline = value;
            }
        }

        internal bool IsDisposed { get; set; }

        public bool IsManagedClient => this.ClientManager != null;

        public virtual void Dispose() {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        internal void EndPipeline() {
            this.ResetSendBuffer();

            if (this.Pipeline != null) {
                this.Pipeline = null;
                // Interlocked.Increment(ref _requestsPerHour);
            }
        }

        private void Init(RedisEndpoint config) {
            this.Host = config.Host;
            this.Port = config.Port;
            this.ConnectTimeout = config.ConnectTimeout;
            this.SendTimeout = config.SendTimeout;
            this.ReceiveTimeout = config.ReceiveTimeout;
            this.RetryTimeout = config.RetryTimeout;
            this.Password = config.Password;
            this.NamespacePrefix = config.NamespacePrefix;
            this.Client = config.Client;
            this.Db = config.Db;
            this.Ssl = config.Ssl;
            this.SslProtocols = config.SslProtocols;
            this.IdleTimeoutSecs = config.IdleTimeoutSecs;
            ServerVersionNumber = RedisConfig.AssumeServerVersion.GetValueOrDefault();
            this._logPrefix = "#" + this.ClientId + " ";
        }

        ~RedisNativeClient() {
            this.Dispose(false);
        }

        protected virtual void Dispose(bool disposing) {
            if (this.ClientManager != null) {
                this.ClientManager.DisposeClient(this);
                return;
            }

            if (disposing) {
                // dispose un managed resources
                this.DisposeConnection();
            }
        }

        internal void DisposeConnection() {
            if (this.IsDisposed) {
                return;
            }

            this.IsDisposed = true;

            if (this.Socket == null) {
                return;
            }

            try {
                this.Quit();
            } catch (Exception ex) {
                _logger.Error(ex, "Error when trying to Quit()");
            } finally {
                this.SafeConnectionClose();
            }
        }

        private void SafeConnectionClose() {
            try {
                // workaround for a .net bug: http://support.microsoft.com/kb/821625
                this.BStream?.Close();
            } catch {
                //
            }

            try {
                this.SslStream?.Close();
            } catch {
                //
            }

            try {
                this.Socket?.Close();
            } catch {
                //
            }

            this.BStream = null;
            this.SslStream = null;
            this.Socket = null;
        }

        #region Common Operations

        private long _db;

        /// <inheritdoc />
        public long Db {
            get => this._db;

            set {
                this._db = value;

                if (this.HasConnected) {
                    this.ChangeDb(this._db);
                }
            }
        }

        public void ChangeDb(long db) {
            this._db = db;
            this.SendExpectSuccess(Commands.Select, db.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public long DbSize => this.SendExpectLong(Commands.DbSize);

        /// <inheritdoc />
        public DateTime LastSave {
            get {
                var t = this.SendExpectLong(Commands.LastSave);
                return t.FromUnixTime();
            }
        }

        /// <inheritdoc />
        public Dictionary<string, string> Info {
            get {
                var lines = this.SendExpectString(Commands.Info);
                var info = new Dictionary<string, string>();

                foreach (var line in lines
                    .Split(new[] { "\r\n" }, StringSplitOptions.RemoveEmptyEntries)) {
                    var p = line.IndexOf(':');
                    if (p == -1) {
                        continue;
                    }

                    info.Add(line.Substring(0, p), line.Substring(p + 1));
                }

                return info;
            }
        }

        public string ServerVersion {
            get {
                this.Info.TryGetValue("redis_version", out var version);
                return version;
            }
        }

        /// <inheritdoc />
        public RedisData RawCommand(params object[] cmdWithArgs) {
            var byteArgs = new List<byte[]>();

            foreach (var arg in cmdWithArgs) {
                if (arg == null) {
                    byteArgs.Add(Array.Empty<byte>());
                    continue;
                }

                if (arg is byte[] bytes) {
                    byteArgs.Add(bytes);
                } else if (arg.GetType().IsUserType()) {
                    var json = arg.ToJson();
                    byteArgs.Add(json.ToUtf8Bytes());
                } else {
                    var str = arg.ToString();
                    byteArgs.Add(str.ToUtf8Bytes());
                }
            }

            var data = this.SendExpectComplexResponse(byteArgs.ToArray());
            return data;
        }

        /// <inheritdoc />
        public RedisData RawCommand(params byte[][] cmdWithBinaryArgs) {
            return this.SendExpectComplexResponse(cmdWithBinaryArgs);
        }

        /// <inheritdoc />
        public bool Ping() {
            return this.SendExpectCode(Commands.Ping) == "PONG";
        }

        /// <inheritdoc />
        public string Echo(string text) {
            return this.SendExpectData(Commands.Echo, text.ToUtf8Bytes()).FromUtf8Bytes();
        }

        /// <inheritdoc />
        public void SlaveOf(string hostname, int port) {
            this.SendExpectSuccess(Commands.SlaveOf, hostname.ToUtf8Bytes(), port.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public void SlaveOfNoOne() {
            this.SendExpectSuccess(Commands.SlaveOf, Commands.No, Commands.One);
        }

        /// <inheritdoc />
        public byte[][] ConfigGet(string pattern) {
            return this.SendExpectMultiData(Commands.Config, Commands.Get, pattern.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public void ConfigSet(string item, byte[] value) {
            this.SendExpectSuccess(Commands.Config, Commands.Set, item.ToUtf8Bytes(), value);
        }

        /// <inheritdoc />
        public void ConfigResetStat() {
            this.SendExpectSuccess(Commands.Config, Commands.ResetStat);
        }

        /// <inheritdoc />
        public void ConfigRewrite() {
            this.SendExpectSuccess(Commands.Config, Commands.Rewrite);
        }

        /// <inheritdoc />
        public byte[][] Time() {
            return this.SendExpectMultiData(Commands.Time);
        }

        /// <inheritdoc />
        public void DebugSegfault() {
            this.SendExpectSuccess(Commands.Debug, Commands.Segfault);
        }

        /// <inheritdoc />
        public void DebugSleep(double durationSecs) {
            this.SendExpectSuccess(Commands.Debug, Commands.Sleep, durationSecs.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public byte[] Dump(string key) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectData(Commands.Dump, key.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public byte[] Restore(string key, long expireMs, byte[] dumpValue) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectData(Commands.Restore, key.ToUtf8Bytes(), expireMs.ToUtf8Bytes(), dumpValue);
        }

        /// <inheritdoc />
        public void Migrate(string host, int port, string key, int destinationDb, long timeoutMs) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            this.SendExpectSuccess(Commands.Migrate,
                host.ToUtf8Bytes(),
                port.ToUtf8Bytes(),
                key.ToUtf8Bytes(),
                destinationDb.ToUtf8Bytes(),
                timeoutMs.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public bool Move(string key, int db) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectLong(Commands.Move, key.ToUtf8Bytes(), db.ToUtf8Bytes()) == Success;
        }

        /// <inheritdoc />
        public long ObjectIdleTime(string key) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectLong(Commands.Object, Commands.IdleTime, key.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public string Type(string key) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectCode(Commands.Type, key.ToUtf8Bytes());
        }

        public RedisKeyType GetEntryType(string key) {
            var type = this.Type(key);
            switch (type) {
                case "none":
                    return RedisKeyType.None;
                case "string":
                    return RedisKeyType.String;
                case "set":
                    return RedisKeyType.Set;
                case "list":
                    return RedisKeyType.List;
                case "zset":
                    return RedisKeyType.SortedSet;
                case "hash":
                    return RedisKeyType.Hash;
            }

            throw this.CreateResponseError($"Invalid Type '{type}'");
        }

        /// <inheritdoc />
        public long StrLen(string key) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectLong(Commands.StrLen, key.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public void Set(string key, byte[] value) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            value = value ?? Array.Empty<byte>();

            if (value.Length > OneGb) {
                throw new ArgumentException("value exceeds 1G", nameof(value));
            }

            this.SendExpectSuccess(Commands.Set, key.ToUtf8Bytes(), value);
        }

        public void Set(string key, byte[] value, int expirySeconds, long expiryMs = 0) {
            this.Set(key.ToUtf8Bytes(), value, expirySeconds, expiryMs);
        }

        public void Set(byte[] key, byte[] value, int expirySeconds, long expiryMs = 0) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            value = value ?? Array.Empty<byte>();

            if (value.Length > OneGb) {
                throw new ArgumentException("value exceeds 1G", nameof(value));
            }

            if (expirySeconds > 0) {
                this.SendExpectSuccess(Commands.Set, key, value, Commands.Ex, expirySeconds.ToUtf8Bytes());
            } else if (expiryMs > 0) {
                this.SendExpectSuccess(Commands.Set, key, value, Commands.Px, expiryMs.ToUtf8Bytes());
            } else {
                this.SendExpectSuccess(Commands.Set, key, value);
            }
        }

        public bool Set(string key, byte[] value, bool exists, int expirySeconds = 0, long expiryMs = 0) {
            var entryExists = exists ? Commands.Xx : Commands.Nx;

            if (expirySeconds > 0) {
                return this.SendExpectString(Commands.Set,
                           key.ToUtf8Bytes(),
                           value,
                           Commands.Ex,
                           expirySeconds.ToUtf8Bytes(),
                           entryExists) == _ok;
            }

            if (expiryMs > 0) {
                return this.SendExpectString(Commands.Set, key.ToUtf8Bytes(), value, Commands.Px, expiryMs.ToUtf8Bytes(), entryExists) ==
                       _ok;
            }

            return this.SendExpectString(Commands.Set, key.ToUtf8Bytes(), value, entryExists) == _ok;
        }

        /// <inheritdoc />
        public void SetEx(string key, int expireInSeconds, byte[] value) {
            this.SetEx(key.ToUtf8Bytes(), expireInSeconds, value);
        }

        public void SetEx(byte[] key, int expireInSeconds, byte[] value) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            value = value ?? Array.Empty<byte>();

            if (value.Length > OneGb) {
                throw new ArgumentException("value exceeds 1G", nameof(value));
            }

            this.SendExpectSuccess(Commands.SetEx, key, expireInSeconds.ToUtf8Bytes(), value);
        }

        /// <inheritdoc />
        public bool Persist(string key) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectLong(Commands.Persist, key.ToUtf8Bytes()) == Success;
        }

        /// <inheritdoc />
        public void PSetEx(string key, long expireInMs, byte[] value) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            this.SendExpectSuccess(Commands.PSetEx, key.ToUtf8Bytes(), expireInMs.ToUtf8Bytes(), value);
        }

        /// <inheritdoc />
        public long SetNX(string key, byte[] value) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            value = value ?? Array.Empty<byte>();

            if (value.Length > OneGb) {
                throw new ArgumentException("value exceeds 1G", nameof(value));
            }

            return this.SendExpectLong(Commands.SetNx, key.ToUtf8Bytes(), value);
        }

        /// <inheritdoc />
        public void MSet(byte[][] keys, byte[][] values) {
            var keysAndValues = MergeCommandWithKeysAndValues(Commands.MSet, keys, values);

            this.SendExpectSuccess(keysAndValues);
        }

        /// <inheritdoc />
        public void MSet(string[] keys, byte[][] values) {
            this.MSet(keys.ToMultiByteArray(), values);
        }

        /// <inheritdoc />
        public bool MSetNx(byte[][] keys, byte[][] values) {
            var keysAndValues = MergeCommandWithKeysAndValues(Commands.MSet, keys, values);

            return this.SendExpectLong(keysAndValues) == Success;
        }

        /// <inheritdoc />
        public bool MSetNx(string[] keys, byte[][] values) {
            return this.MSetNx(keys.ToMultiByteArray(), values);
        }

        /// <inheritdoc />
        public byte[] Get(string key) {
            return this.GetBytes(key);
        }

        public byte[] Get(byte[] key) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectData(Commands.Get, key);
        }

        public object[] Slowlog(int? top) {
            if (top.HasValue) {
                return this.SendExpectDeeplyNestedMultiData(Commands.Slowlog, Commands.Get, top.Value.ToUtf8Bytes());
            }

            return this.SendExpectDeeplyNestedMultiData(Commands.Slowlog, Commands.Get);
        }

        public void SlowlogReset() {
            this.SendExpectSuccess(Commands.Slowlog, "RESET".ToUtf8Bytes());
        }

        public byte[] GetBytes(string key) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectData(Commands.Get, key.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public byte[] GetSet(string key, byte[] value) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            value = value ?? Array.Empty<byte>();

            if (value.Length > OneGb) {
                throw new ArgumentException("value exceeds 1G", nameof(value));
            }

            return this.SendExpectData(Commands.GetSet, key.ToUtf8Bytes(), value);
        }

        /// <inheritdoc />
        public long Exists(string key) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectLong(Commands.Exists, key.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public long Del(string key) {
            return this.Del(key.ToUtf8Bytes());
        }

        public long Del(byte[] key) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectLong(Commands.Del, key);
        }

        /// <inheritdoc />
        public long Del(params string[] keys) {
            if (keys == null) {
                throw new ArgumentNullException(nameof(keys));
            }

            var cmdWithArgs = MergeCommandWithArgs(Commands.Del, keys);
            return this.SendExpectLong(cmdWithArgs);
        }

        /// <inheritdoc />
        public long Incr(string key) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectLong(Commands.Incr, key.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public long IncrBy(string key, int count) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectLong(Commands.IncrBy, key.ToUtf8Bytes(), count.ToUtf8Bytes());
        }

        public long IncrBy(string key, long count) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectLong(Commands.IncrBy, key.ToUtf8Bytes(), count.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public double IncrByFloat(string key, double incrBy) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectDouble(Commands.IncrByFloat, key.ToUtf8Bytes(), incrBy.ToUtf8Bytes());
        }

        public long Decr(string key) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectLong(Commands.Decr, key.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public long DecrBy(string key, int count) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectLong(Commands.DecrBy, key.ToUtf8Bytes(), count.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public long Append(string key, byte[] value) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectLong(Commands.Append, key.ToUtf8Bytes(), value);
        }

        /// <inheritdoc />
        public byte[] GetRange(string key, int fromIndex, int toIndex) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectData(Commands.GetRange, key.ToUtf8Bytes(), fromIndex.ToUtf8Bytes(), toIndex.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public long SetRange(string key, int offset, byte[] value) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectLong(Commands.SetRange, key.ToUtf8Bytes(), offset.ToUtf8Bytes(), value);
        }

        /// <inheritdoc />
        public long GetBit(string key, int offset) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectLong(Commands.GetBit, key.ToUtf8Bytes(), offset.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public long SetBit(string key, int offset, int value) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            if (value > 1 || value < 0) {
                throw new ArgumentException("value is out of range");
            }

            return this.SendExpectLong(Commands.SetBit, key.ToUtf8Bytes(), offset.ToUtf8Bytes(), value.ToUtf8Bytes());
        }

        public long BitCount(string key) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectLong(Commands.BitCount, key.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public string RandomKey() {
            return this.SendExpectData(Commands.RandomKey).FromUtf8Bytes();
        }

        /// <inheritdoc />
        public void Rename(string oldKeyname, string newKeyname) {
            if (oldKeyname == null) {
                throw new ArgumentNullException(nameof(oldKeyname));
            }

            if (newKeyname == null) {
                throw new ArgumentNullException(nameof(newKeyname));
            }

            this.SendExpectSuccess(Commands.Rename, oldKeyname.ToUtf8Bytes(), newKeyname.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public bool RenameNx(string oldKeyname, string newKeyname) {
            if (oldKeyname == null) {
                throw new ArgumentNullException(nameof(oldKeyname));
            }

            if (newKeyname == null) {
                throw new ArgumentNullException(nameof(newKeyname));
            }

            return this.SendExpectLong(Commands.RenameNx, oldKeyname.ToUtf8Bytes(), newKeyname.ToUtf8Bytes()) == Success;
        }

        /// <inheritdoc />
        public bool Expire(string key, int seconds) {
            return this.Expire(key.ToUtf8Bytes(), seconds);
        }

        public bool Expire(byte[] key, int seconds) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectLong(Commands.Expire, key, seconds.ToUtf8Bytes()) == Success;
        }

        /// <inheritdoc />
        public bool PExpire(string key, long ttlMs) {
            return this.PExpire(key.ToUtf8Bytes(), ttlMs);
        }

        public bool PExpire(byte[] key, long ttlMs) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectLong(Commands.PExpire, key, ttlMs.ToUtf8Bytes()) == Success;
        }

        /// <inheritdoc />
        public bool ExpireAt(string key, long unixTime) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectLong(Commands.ExpireAt, key.ToUtf8Bytes(), unixTime.ToUtf8Bytes()) == Success;
        }

        /// <inheritdoc />
        public bool PExpireAt(string key, long unixTimeMs) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectLong(Commands.PExpireAt, key.ToUtf8Bytes(), unixTimeMs.ToUtf8Bytes()) == Success;
        }

        /// <inheritdoc />
        public long Ttl(string key) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectLong(Commands.Ttl, key.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public long PTtl(string key) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return this.SendExpectLong(Commands.PTtl, key.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public void Save() {
            this.SendExpectSuccess(Commands.Save);
        }

        public void SaveAsync() {
            this.BgSave();
        }

        /// <inheritdoc />
        public void BgSave() {
            this.SendExpectSuccess(Commands.BgSave);
        }

        /// <inheritdoc />
        public void Shutdown() {
            this.SendWithoutRead(Commands.Shutdown);
        }

        public void ShutdownNoSave() {
            this.SendWithoutRead(Commands.Shutdown, Commands.NoSave);
        }

        /// <inheritdoc />
        public void BgRewriteAof() {
            this.SendExpectSuccess(Commands.BgRewriteAof);
        }

        /// <inheritdoc />
        public void Quit() {
            this.SendWithoutRead(Commands.Quit);
        }

        /// <inheritdoc />
        public void FlushDb() {
            this.SendExpectSuccess(Commands.FlushDb);
        }

        /// <inheritdoc />
        public void FlushAll() {
            this.SendExpectSuccess(Commands.FlushAll);
        }

        public RedisText Role() {
            return this.SendExpectComplexResponse(Commands.Role).ToRedisText();
        }

        /// <inheritdoc />
        public string ClientGetName() {
            return this.SendExpectString(Commands.Client, Commands.GetName);
        }

        /// <inheritdoc />
        public void ClientSetName(string name) {
            if (string.IsNullOrEmpty(name)) {
                throw new ArgumentException("Name cannot be null or empty");
            }

            if (name.Contains(" ")) {
                throw new ArgumentException("Name cannot contain spaces");
            }

            this.SendExpectSuccess(Commands.Client, Commands.SetName, name.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public void ClientPause(int timeoutMs) {
            this.SendExpectSuccess(Commands.Client, Commands.Pause, timeoutMs.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public byte[] ClientList() {
            return this.SendExpectData(Commands.Client, Commands.List);
        }

        /// <inheritdoc />
        public void ClientKill(string clientAddr) {
            this.SendExpectSuccess(Commands.Client, Commands.Kill, clientAddr.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public long ClientKill(string addr = null, string id = null, string type = null, string skipMe = null) {
            var cmdWithArgs = new List<byte[]> {
                Commands.Client,
                Commands.Kill
            };

            if (addr != null) {
                cmdWithArgs.Add(Commands.Addr);
                cmdWithArgs.Add(addr.ToUtf8Bytes());
            }

            if (id != null) {
                cmdWithArgs.Add(Commands.Id);
                cmdWithArgs.Add(id.ToUtf8Bytes());
            }

            if (type != null) {
                cmdWithArgs.Add(Commands.Type);
                cmdWithArgs.Add(type.ToUtf8Bytes());
            }

            if (skipMe != null) {
                cmdWithArgs.Add(Commands.SkipMe);
                cmdWithArgs.Add(skipMe.ToUtf8Bytes());
            }

            return this.SendExpectLong(cmdWithArgs.ToArray());
        }

        /// <inheritdoc />
        public byte[][] Keys(string pattern) {
            if (pattern == null) {
                throw new ArgumentNullException(nameof(pattern));
            }

            return this.SendExpectMultiData(Commands.Keys, pattern.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public byte[][] MGet(params byte[][] keys) {
            if (keys == null || keys.Length == 0) {
                throw new ArgumentNullException(nameof(keys));
            }

            var cmdWithArgs = MergeCommandWithArgs(Commands.MGet, keys);

            return this.SendExpectMultiData(cmdWithArgs);
        }

        /// <inheritdoc />
        /// <inheritdoc />
        public byte[][] MGet(params string[] keys) {
            if (keys == null || keys.Length == 0) {
                throw new ArgumentNullException(nameof(keys));
            }

            var cmdWithArgs = MergeCommandWithArgs(Commands.MGet, keys);

            return this.SendExpectMultiData(cmdWithArgs);
        }

        public void Watch(params string[] keys) {
            if (keys == null || keys.Length == 0) {
                throw new ArgumentNullException(nameof(keys));
            }

            var cmdWithArgs = MergeCommandWithArgs(Commands.Watch, keys);

            this.SendExpectCode(cmdWithArgs);

        }

        /// <inheritdoc />
        public void UnWatch() {
            this.SendExpectCode(Commands.UnWatch);
        }

        internal void Multi() {
            // make sure socket is connected. Otherwise, fetch of server info will interfere
            // with pipeline
            this.AssertConnectedSocket();
            this.SendWithoutRead(Commands.Multi);
        }

        /// <summary>
        ///     Requires custom result parsing
        /// </summary>
        /// <returns>Number of results</returns>
        internal void Exec() {
            this.SendWithoutRead(Commands.Exec);
        }

        internal void Discard() {
            this.SendExpectSuccess(Commands.Discard);
        }

        /// <inheritdoc />
        public ScanResult Scan(ulong cursor, int count = 10, string match = null) {
            if (match == null) {
                return this.SendExpectScanResult(Commands.Scan,
                    cursor.ToUtf8Bytes(),
                    Commands.Count,
                    count.ToUtf8Bytes());
            }

            return this.SendExpectScanResult(Commands.Scan,
                cursor.ToUtf8Bytes(),
                Commands.Match,
                match.ToUtf8Bytes(),
                Commands.Count,
                count.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public ScanResult SScan(string setId, ulong cursor, int count = 10, string match = null) {
            if (match == null) {
                return this.SendExpectScanResult(Commands.SScan,
                    setId.ToUtf8Bytes(),
                    cursor.ToUtf8Bytes(),
                    Commands.Count,
                    count.ToUtf8Bytes());
            }

            return this.SendExpectScanResult(Commands.SScan,
                setId.ToUtf8Bytes(),
                cursor.ToUtf8Bytes(),
                Commands.Match,
                match.ToUtf8Bytes(),
                Commands.Count,
                count.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public ScanResult ZScan(string setId, ulong cursor, int count = 10, string match = null) {
            if (match == null) {
                return this.SendExpectScanResult(Commands.ZScan,
                    setId.ToUtf8Bytes(),
                    cursor.ToUtf8Bytes(),
                    Commands.Count,
                    count.ToUtf8Bytes());
            }

            return this.SendExpectScanResult(Commands.ZScan,
                setId.ToUtf8Bytes(),
                cursor.ToUtf8Bytes(),
                Commands.Match,
                match.ToUtf8Bytes(),
                Commands.Count,
                count.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public ScanResult HScan(string hashId, ulong cursor, int count = 10, string match = null) {
            if (match == null) {
                return this.SendExpectScanResult(Commands.HScan,
                    hashId.ToUtf8Bytes(),
                    cursor.ToUtf8Bytes(),
                    Commands.Count,
                    count.ToUtf8Bytes());
            }

            return this.SendExpectScanResult(Commands.HScan,
                hashId.ToUtf8Bytes(),
                cursor.ToUtf8Bytes(),
                Commands.Match,
                match.ToUtf8Bytes(),
                Commands.Count,
                count.ToUtf8Bytes());
        }

        internal ScanResult SendExpectScanResult(byte[] cmd, params byte[][] args) {
            var cmdWithArgs = MergeCommandWithArgs(cmd, args);
            var multiData = this.SendExpectDeeplyNestedMultiData(cmdWithArgs);
            var counterBytes = (byte[])multiData[0];

            var ret = new ScanResult {
                Cursor = ulong.Parse(counterBytes.FromUtf8Bytes()),
                Results = new List<byte[]>()
            };
            var keysBytes = (object[])multiData[1];

            foreach (var keyBytes in keysBytes) {
                ret.Results.Add((byte[])keyBytes);
            }

            return ret;
        }

        /// <inheritdoc />
        public bool PfAdd(string key, params byte[][] elements) {
            var cmdWithArgs = MergeCommandWithArgs(Commands.PfAdd, key.ToUtf8Bytes(), elements);
            return this.SendExpectLong(cmdWithArgs) == 1;
        }

        /// <inheritdoc />
        public long PfCount(string key) {
            var cmdWithArgs = MergeCommandWithArgs(Commands.PfCount, key.ToUtf8Bytes());
            return this.SendExpectLong(cmdWithArgs);
        }

        /// <inheritdoc />
        public void PfMerge(string toKeyId, params string[] fromKeys) {
            var fromKeyBytes = fromKeys.Select(x => x.ToUtf8Bytes()).ToArray();
            var cmdWithArgs = MergeCommandWithArgs(Commands.PfMerge, toKeyId.ToUtf8Bytes(), fromKeyBytes);
            this.SendExpectSuccess(cmdWithArgs);
        }

        #endregion

        #region Set Operations

        /// <inheritdoc />
        public byte[][] SMembers(string setId) {
            return this.SendExpectMultiData(Commands.SMembers, setId.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public long SAdd(string setId, byte[] value) {
            AssertSetIdAndValue(setId, value);

            return this.SendExpectLong(Commands.SAdd, setId.ToUtf8Bytes(), value);
        }

        /// <inheritdoc />
        public long SAdd(string setId, byte[][] values) {
            if (setId == null) {
                throw new ArgumentNullException(nameof(setId));
            }

            if (values == null || values.Length == 0) {
                throw new ArgumentNullException(nameof(values));
            }

            var cmdWithArgs = MergeCommandWithArgs(Commands.SAdd, setId.ToUtf8Bytes(), values);
            return this.SendExpectLong(cmdWithArgs);
        }

        /// <inheritdoc />
        public long SRem(string setId, byte[] value) {
            AssertSetIdAndValue(setId, value);

            return this.SendExpectLong(Commands.SRem, setId.ToUtf8Bytes(), value);
        }

        public long SRem(string setId, byte[][] values) {
            if (setId == null) {
                throw new ArgumentNullException(nameof(setId));
            }

            if (values == null || values.Length == 0) {
                throw new ArgumentNullException(nameof(values));
            }

            var cmdWithArgs = MergeCommandWithArgs(Commands.SRem, setId.ToUtf8Bytes(), values);
            return this.SendExpectLong(cmdWithArgs);
        }

        /// <inheritdoc />
        public byte[] SPop(string setId) {
            if (setId == null) {
                throw new ArgumentNullException(nameof(setId));
            }

            return this.SendExpectData(Commands.SPop, setId.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public byte[][] SPop(string setId, int count) {
            if (setId == null) {
                throw new ArgumentNullException(nameof(setId));
            }

            return this.SendExpectMultiData(Commands.SPop, setId.ToUtf8Bytes(), count.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public void SMove(string fromSetId, string toSetId, byte[] value) {
            if (fromSetId == null) {
                throw new ArgumentNullException(nameof(fromSetId));
            }

            if (toSetId == null) {
                throw new ArgumentNullException(nameof(toSetId));
            }

            this.SendExpectSuccess(Commands.SMove, fromSetId.ToUtf8Bytes(), toSetId.ToUtf8Bytes(), value);
        }

        /// <inheritdoc />
        public long SCard(string setId) {
            if (setId == null) {
                throw new ArgumentNullException(nameof(setId));
            }

            return this.SendExpectLong(Commands.SCard, setId.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public long SIsMember(string setId, byte[] value) {
            if (setId == null) {
                throw new ArgumentNullException(nameof(setId));
            }

            return this.SendExpectLong(Commands.SIsMember, setId.ToUtf8Bytes(), value);
        }

        /// <inheritdoc />
        public byte[][] SInter(params string[] setIds) {
            var cmdWithArgs = MergeCommandWithArgs(Commands.SInter, setIds);
            return this.SendExpectMultiData(cmdWithArgs);
        }

        /// <inheritdoc />
        public void SInterStore(string intoSetId, params string[] setIds) {
            var setIdsList = new List<string>(setIds);
            setIdsList.Insert(0, intoSetId);

            var cmdWithArgs = MergeCommandWithArgs(Commands.SInterStore, setIdsList.ToArray());
            this.SendExpectSuccess(cmdWithArgs);
        }

        /// <inheritdoc />
        public byte[][] SUnion(params string[] setIds) {
            var cmdWithArgs = MergeCommandWithArgs(Commands.SUnion, setIds);
            return this.SendExpectMultiData(cmdWithArgs);
        }

        /// <inheritdoc />
        public void SUnionStore(string intoSetId, params string[] setIds) {
            var setIdsList = new List<string>(setIds);
            setIdsList.Insert(0, intoSetId);

            var cmdWithArgs = MergeCommandWithArgs(Commands.SUnionStore, setIdsList.ToArray());
            this.SendExpectSuccess(cmdWithArgs);
        }

        /// <inheritdoc />
        public byte[][] SDiff(string fromSetId, params string[] withSetIds) {
            var setIdsList = new List<string>(withSetIds);
            setIdsList.Insert(0, fromSetId);

            var cmdWithArgs = MergeCommandWithArgs(Commands.SDiff, setIdsList.ToArray());
            return this.SendExpectMultiData(cmdWithArgs);
        }

        /// <inheritdoc />
        public void SDiffStore(string intoSetId, string fromSetId, params string[] withSetIds) {
            var setIdsList = new List<string>(withSetIds);
            setIdsList.Insert(0, fromSetId);
            setIdsList.Insert(0, intoSetId);

            var cmdWithArgs = MergeCommandWithArgs(Commands.SDiffStore, setIdsList.ToArray());
            this.SendExpectSuccess(cmdWithArgs);
        }

        /// <inheritdoc />
        public byte[] SRandMember(string setId) {
            return this.SendExpectData(Commands.SRandMember, setId.ToUtf8Bytes());
        }

        public byte[][] SRandMember(string setId, int count) {
            return this.SendExpectMultiData(Commands.SRandMember, setId.ToUtf8Bytes(), count.ToUtf8Bytes());
        }

        #endregion

        #region List Operations

        /// <inheritdoc />
        public byte[][] LRange(string listId, int startingFrom, int endingAt) {
            if (listId == null) {
                throw new ArgumentNullException(nameof(listId));
            }

            return this.SendExpectMultiData(Commands.LRange, listId.ToUtf8Bytes(), startingFrom.ToUtf8Bytes(), endingAt.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public byte[][] Sort(string listOrSetId, SortOptions sortOptions) {
            var cmdWithArgs = new List<byte[]> {
                Commands.Sort,
                listOrSetId.ToUtf8Bytes()
            };

            if (sortOptions.SortPattern != null) {
                cmdWithArgs.Add(Commands.By);
                cmdWithArgs.Add(sortOptions.SortPattern.ToUtf8Bytes());
            }

            if (sortOptions.Skip.HasValue || sortOptions.Take.HasValue) {
                cmdWithArgs.Add(Commands.Limit);
                cmdWithArgs.Add(sortOptions.Skip.GetValueOrDefault(0).ToUtf8Bytes());
                cmdWithArgs.Add(sortOptions.Take.GetValueOrDefault(0).ToUtf8Bytes());
            }

            if (sortOptions.GetPattern != null) {
                cmdWithArgs.Add(Commands.Get);
                cmdWithArgs.Add(sortOptions.GetPattern.ToUtf8Bytes());
            }

            if (sortOptions.SortDesc) {
                cmdWithArgs.Add(Commands.Desc);
            }

            if (sortOptions.SortAlpha) {
                cmdWithArgs.Add(Commands.Alpha);
            }

            if (sortOptions.StoreAtKey != null) {
                cmdWithArgs.Add(Commands.Store);
                cmdWithArgs.Add(sortOptions.StoreAtKey.ToUtf8Bytes());
            }

            return this.SendExpectMultiData(cmdWithArgs.ToArray());
        }

        /// <inheritdoc />
        public long RPush(string listId, byte[] value) {
            AssertListIdAndValue(listId, value);

            return this.SendExpectLong(Commands.RPush, listId.ToUtf8Bytes(), value);
        }

        public long RPush(string listId, byte[][] values) {
            if (listId == null) {
                throw new ArgumentNullException(nameof(listId));
            }

            if (values == null || values.Length == 0) {
                throw new ArgumentNullException(nameof(values));
            }

            var cmdWithArgs = MergeCommandWithArgs(Commands.RPush, listId.ToUtf8Bytes(), values);
            return this.SendExpectLong(cmdWithArgs);
        }

        /// <inheritdoc />
        public long RPushX(string listId, byte[] value) {
            AssertListIdAndValue(listId, value);

            return this.SendExpectLong(Commands.RPushX, listId.ToUtf8Bytes(), value);
        }

        /// <inheritdoc />
        public long LPush(string listId, byte[] value) {
            AssertListIdAndValue(listId, value);

            return this.SendExpectLong(Commands.LPush, listId.ToUtf8Bytes(), value);
        }

        public long LPush(string listId, byte[][] values) {
            if (listId == null) {
                throw new ArgumentNullException(nameof(listId));
            }

            if (values == null || values.Length == 0) {
                throw new ArgumentNullException(nameof(values));
            }

            var cmdWithArgs = MergeCommandWithArgs(Commands.LPush, listId.ToUtf8Bytes(), values);
            return this.SendExpectLong(cmdWithArgs);
        }

        /// <inheritdoc />
        public long LPushX(string listId, byte[] value) {
            AssertListIdAndValue(listId, value);

            return this.SendExpectLong(Commands.LPushX, listId.ToUtf8Bytes(), value);
        }

        /// <inheritdoc />
        public void LTrim(string listId, int keepStartingFrom, int keepEndingAt) {
            if (listId == null) {
                throw new ArgumentNullException(nameof(listId));
            }

            this.SendExpectSuccess(Commands.LTrim, listId.ToUtf8Bytes(), keepStartingFrom.ToUtf8Bytes(), keepEndingAt.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public long LRem(string listId, int removeNoOfMatches, byte[] value) {
            if (listId == null) {
                throw new ArgumentNullException(nameof(listId));
            }

            return this.SendExpectLong(Commands.LRem, listId.ToUtf8Bytes(), removeNoOfMatches.ToUtf8Bytes(), value);
        }

        /// <inheritdoc />
        public long LLen(string listId) {
            if (listId == null) {
                throw new ArgumentNullException(nameof(listId));
            }

            return this.SendExpectLong(Commands.LLen, listId.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public byte[] LIndex(string listId, int listIndex) {
            if (listId == null) {
                throw new ArgumentNullException(nameof(listId));
            }

            return this.SendExpectData(Commands.LIndex, listId.ToUtf8Bytes(), listIndex.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public void LInsert(string listId, bool insertBefore, byte[] pivot, byte[] value) {
            if (listId == null) {
                throw new ArgumentNullException(nameof(listId));
            }

            var position = insertBefore ? Commands.Before : Commands.After;

            this.SendExpectSuccess(Commands.LInsert, listId.ToUtf8Bytes(), position, pivot, value);
        }

        /// <inheritdoc />
        public void LSet(string listId, int listIndex, byte[] value) {
            if (listId == null) {
                throw new ArgumentNullException(nameof(listId));
            }

            this.SendExpectSuccess(Commands.LSet, listId.ToUtf8Bytes(), listIndex.ToUtf8Bytes(), value);
        }

        /// <inheritdoc />
        public byte[] LPop(string listId) {
            if (listId == null) {
                throw new ArgumentNullException(nameof(listId));
            }

            return this.SendExpectData(Commands.LPop, listId.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public byte[] RPop(string listId) {
            if (listId == null) {
                throw new ArgumentNullException(nameof(listId));
            }

            return this.SendExpectData(Commands.RPop, listId.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public byte[][] BLPop(string listId, int timeoutSecs) {
            if (listId == null) {
                throw new ArgumentNullException(nameof(listId));
            }

            return this.SendExpectMultiData(Commands.BLPop, listId.ToUtf8Bytes(), timeoutSecs.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public byte[][] BLPop(string[] listIds, int timeoutSecs) {
            if (listIds == null) {
                throw new ArgumentNullException(nameof(listIds));
            }

            var args = new List<byte[]> { Commands.BLPop };
            args.AddRange(listIds.Select(listId => listId.ToUtf8Bytes()));
            args.Add(timeoutSecs.ToUtf8Bytes());
            return this.SendExpectMultiData(args.ToArray());
        }

        /// <inheritdoc />
        public byte[] BLPopValue(string listId, int timeoutSecs) {
            var blockingResponse = this.BLPop(new[] { listId }, timeoutSecs);
            return blockingResponse.Length == 0
                ? null
                : blockingResponse[1];
        }

        /// <inheritdoc />
        public byte[][] BLPopValue(string[] listIds, int timeoutSecs) {
            var blockingResponse = this.BLPop(listIds, timeoutSecs);
            return blockingResponse.Length == 0
                ? null
                : blockingResponse;
        }

        /// <inheritdoc />
        public byte[][] BRPop(string listId, int timeoutSecs) {
            if (listId == null) {
                throw new ArgumentNullException(nameof(listId));
            }

            return this.SendExpectMultiData(Commands.BRPop, listId.ToUtf8Bytes(), timeoutSecs.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public byte[][] BRPop(string[] listIds, int timeoutSecs) {
            if (listIds == null) {
                throw new ArgumentNullException(nameof(listIds));
            }

            var args = new List<byte[]> { Commands.BRPop };
            args.AddRange(listIds.Select(listId => listId.ToUtf8Bytes()));
            args.Add(timeoutSecs.ToUtf8Bytes());
            return this.SendExpectMultiData(args.ToArray());
        }

        /// <inheritdoc />
        public byte[] BRPopValue(string listId, int timeoutSecs) {
            var blockingResponse = this.BRPop(new[] { listId }, timeoutSecs);
            return blockingResponse.Length == 0
                ? null
                : blockingResponse[1];
        }

        /// <inheritdoc />
        public byte[][] BRPopValue(string[] listIds, int timeoutSecs) {
            var blockingResponse = this.BRPop(listIds, timeoutSecs);
            return blockingResponse.Length == 0
                ? null
                : blockingResponse;
        }

        /// <inheritdoc />
        public byte[] RPopLPush(string fromListId, string toListId) {
            if (fromListId == null) {
                throw new ArgumentNullException(nameof(fromListId));
            }

            if (toListId == null) {
                throw new ArgumentNullException(nameof(toListId));
            }

            return this.SendExpectData(Commands.RPopLPush, fromListId.ToUtf8Bytes(), toListId.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public byte[] BRPopLPush(string fromListId, string toListId, int timeoutSecs) {
            if (fromListId == null) {
                throw new ArgumentNullException(nameof(fromListId));
            }

            if (toListId == null) {
                throw new ArgumentNullException(nameof(toListId));
            }

            var result = this.SendExpectMultiData(Commands.BRPopLPush,
                fromListId.ToUtf8Bytes(),
                toListId.ToUtf8Bytes(),
                timeoutSecs.ToUtf8Bytes());
            return result.Length == 0 ? null : result[1];
        }

        #endregion

        #region Sentinel

        public List<Dictionary<string, string>> SentinelMasters() {
            var args = new List<byte[]> {
                Commands.Sentinel,
                Commands.Masters
            };
            return this.SendExpectStringDictionaryList(args.ToArray());
        }

        public Dictionary<string, string> SentinelMaster(string masterName) {
            var args = new List<byte[]> {
                Commands.Sentinel,
                Commands.Master,
                masterName.ToUtf8Bytes()
            };
            var results = this.SendExpectComplexResponse(args.ToArray());
            return ToDictionary(results);
        }

        public List<Dictionary<string, string>> SentinelSentinels(string masterName) {
            var args = new List<byte[]> {
                Commands.Sentinel,
                Commands.Sentinels,
                masterName.ToUtf8Bytes()
            };
            return this.SendExpectStringDictionaryList(args.ToArray());
        }

        public List<Dictionary<string, string>> SentinelSlaves(string masterName) {
            var args = new List<byte[]> {
                Commands.Sentinel,
                Commands.Slaves,
                masterName.ToUtf8Bytes()
            };
            return this.SendExpectStringDictionaryList(args.ToArray());
        }

        public List<string> SentinelGetMasterAddrByName(string masterName) {
            var args = new List<byte[]> {
                Commands.Sentinel,
                Commands.GetMasterAddrByName,
                masterName.ToUtf8Bytes()
            };
            return this.SendExpectMultiData(args.ToArray()).ToStringList();
        }

        public void SentinelFailover(string masterName) {
            var args = new List<byte[]> {
                Commands.Sentinel,
                Commands.Failover,
                masterName.ToUtf8Bytes()
            };

            this.SendExpectSuccess(args.ToArray());
        }

        #endregion

        #region Sorted Set Operations

        private static void AssertSetIdAndValue(string setId, byte[] value) {
            if (setId == null) {
                throw new ArgumentNullException(nameof(setId));
            }

            if (value == null) {
                throw new ArgumentNullException(nameof(value));
            }
        }

        /// <inheritdoc />
        public long ZAdd(string setId, double score, byte[] value) {
            AssertSetIdAndValue(setId, value);

            return this.SendExpectLong(Commands.ZAdd, setId.ToUtf8Bytes(), score.ToFastUtf8Bytes(), value);
        }

        /// <inheritdoc />
        public long ZAdd(string setId, long score, byte[] value) {
            AssertSetIdAndValue(setId, value);

            return this.SendExpectLong(Commands.ZAdd, setId.ToUtf8Bytes(), score.ToUtf8Bytes(), value);
        }

        public long ZAdd(string setId, List<KeyValuePair<byte[], double>> pairs) {
            if (setId == null) {
                throw new ArgumentNullException(nameof(setId));
            }

            if (pairs == null) {
                throw new ArgumentNullException(nameof(pairs));
            }

            if (pairs.Count == 0) {
                throw new ArgumentOutOfRangeException(nameof(pairs));
            }

            var mergedBytes = new byte[2 + pairs.Count * 2][];
            mergedBytes[0] = Commands.ZAdd;
            mergedBytes[1] = setId.ToUtf8Bytes();
            for (var i = 0; i < pairs.Count; i++) {
                mergedBytes[i * 2 + 2] = pairs[i].Value.ToFastUtf8Bytes();
                mergedBytes[i * 2 + 3] = pairs[i].Key;
            }

            return this.SendExpectLong(mergedBytes);
        }

        public long ZAdd(string setId, List<KeyValuePair<byte[], long>> pairs) {
            if (setId == null) {
                throw new ArgumentNullException(nameof(setId));
            }

            if (pairs == null) {
                throw new ArgumentNullException(nameof(pairs));
            }

            if (pairs.Count == 0) {
                throw new ArgumentOutOfRangeException(nameof(pairs));
            }

            var mergedBytes = new byte[2 + pairs.Count * 2][];
            mergedBytes[0] = Commands.ZAdd;
            mergedBytes[1] = setId.ToUtf8Bytes();
            for (var i = 0; i < pairs.Count; i++) {
                mergedBytes[i * 2 + 2] = pairs[i].Value.ToUtf8Bytes();
                mergedBytes[i * 2 + 3] = pairs[i].Key;
            }

            return this.SendExpectLong(mergedBytes);
        }

        /// <inheritdoc />
        public long ZRem(string setId, byte[] value) {
            AssertSetIdAndValue(setId, value);

            return this.SendExpectLong(Commands.ZRem, setId.ToUtf8Bytes(), value);
        }

        /// <inheritdoc />
        public long ZRem(string setId, byte[][] values) {
            if (setId == null) {
                throw new ArgumentNullException(nameof(setId));
            }

            if (values == null || values.Length == 0) {
                throw new ArgumentNullException(nameof(values));
            }

            var cmdWithArgs = MergeCommandWithArgs(Commands.ZRem, setId.ToUtf8Bytes(), values);
            return this.SendExpectLong(cmdWithArgs);
        }

        /// <inheritdoc />
        public double ZIncrBy(string setId, double incrBy, byte[] value) {
            AssertSetIdAndValue(setId, value);

            return this.SendExpectDouble(Commands.ZIncrBy, setId.ToUtf8Bytes(), incrBy.ToFastUtf8Bytes(), value);
        }

        /// <inheritdoc />
        public double ZIncrBy(string setId, long incrBy, byte[] value) {
            AssertSetIdAndValue(setId, value);

            return this.SendExpectDouble(Commands.ZIncrBy, setId.ToUtf8Bytes(), incrBy.ToUtf8Bytes(), value);
        }

        /// <inheritdoc />
        public long ZRank(string setId, byte[] value) {
            AssertSetIdAndValue(setId, value);

            return this.SendExpectLong(Commands.ZRank, setId.ToUtf8Bytes(), value);
        }

        /// <inheritdoc />
        public long ZRevRank(string setId, byte[] value) {
            AssertSetIdAndValue(setId, value);

            return this.SendExpectLong(Commands.ZRevRank, setId.ToUtf8Bytes(), value);
        }

        private byte[][] GetRange(byte[] commandBytes, string setId, int min, int max, bool withScores) {
            if (string.IsNullOrEmpty(setId)) {
                throw new ArgumentNullException(nameof(setId));
            }

            var cmdWithArgs = new List<byte[]> {
                commandBytes,
                setId.ToUtf8Bytes(),
                min.ToUtf8Bytes(),
                max.ToUtf8Bytes()
            };

            if (withScores) {
                cmdWithArgs.Add(Commands.WithScores);
            }

            return this.SendExpectMultiData(cmdWithArgs.ToArray());
        }

        /// <inheritdoc />
        public byte[][] ZRange(string setId, int min, int max) {
            return this.SendExpectMultiData(Commands.ZRange, setId.ToUtf8Bytes(), min.ToUtf8Bytes(), max.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public byte[][] ZRangeWithScores(string setId, int min, int max) {
            return this.GetRange(Commands.ZRange, setId, min, max, true);
        }

        /// <inheritdoc />
        public byte[][] ZRevRange(string setId, int min, int max) {
            return this.GetRange(Commands.ZRevRange, setId, min, max, false);
        }

        /// <inheritdoc />
        public byte[][] ZRevRangeWithScores(string setId, int min, int max) {
            return this.GetRange(Commands.ZRevRange, setId, min, max, true);
        }

        private byte[][] GetRangeByScore(byte[] commandBytes,
            string setId, double min, double max, int? skip, int? take, bool withScores) {
            if (setId == null) {
                throw new ArgumentNullException(nameof(setId));
            }

            var cmdWithArgs = new List<byte[]> {
                commandBytes,
                setId.ToUtf8Bytes(),
                min.ToFastUtf8Bytes(),
                max.ToFastUtf8Bytes()
            };

            if (skip.HasValue || take.HasValue) {
                cmdWithArgs.Add(Commands.Limit);
                cmdWithArgs.Add(skip.GetValueOrDefault(0).ToUtf8Bytes());
                cmdWithArgs.Add(take.GetValueOrDefault(0).ToUtf8Bytes());
            }

            if (withScores) {
                cmdWithArgs.Add(Commands.WithScores);
            }

            return this.SendExpectMultiData(cmdWithArgs.ToArray());
        }

        private byte[][] GetRangeByScore(byte[] commandBytes,
            string setId, long min, long max, int? skip, int? take, bool withScores) {
            if (setId == null) {
                throw new ArgumentNullException(nameof(setId));
            }

            var cmdWithArgs = new List<byte[]> {
                commandBytes,
                setId.ToUtf8Bytes(),
                min.ToUtf8Bytes(),
                max.ToUtf8Bytes()
            };

            if (skip.HasValue || take.HasValue) {
                cmdWithArgs.Add(Commands.Limit);
                cmdWithArgs.Add(skip.GetValueOrDefault(0).ToUtf8Bytes());
                cmdWithArgs.Add(take.GetValueOrDefault(0).ToUtf8Bytes());
            }

            if (withScores) {
                cmdWithArgs.Add(Commands.WithScores);
            }

            return this.SendExpectMultiData(cmdWithArgs.ToArray());
        }

        /// <inheritdoc />
        public byte[][] ZRangeByScore(string setId, double min, double max, int? skip, int? take) {
            return this.GetRangeByScore(Commands.ZRangeByScore, setId, min, max, skip, take, false);
        }

        /// <inheritdoc />
        public byte[][] ZRangeByScore(string setId, long min, long max, int? skip, int? take) {
            return this.GetRangeByScore(Commands.ZRangeByScore, setId, min, max, skip, take, false);
        }

        /// <inheritdoc />
        public byte[][] ZRangeByScoreWithScores(string setId, double min, double max, int? skip, int? take) {
            return this.GetRangeByScore(Commands.ZRangeByScore, setId, min, max, skip, take, true);
        }

        /// <inheritdoc />
        public byte[][] ZRangeByScoreWithScores(string setId, long min, long max, int? skip, int? take) {
            return this.GetRangeByScore(Commands.ZRangeByScore, setId, min, max, skip, take, true);
        }

        /// <inheritdoc />
        public byte[][] ZRevRangeByScore(string setId, double min, double max, int? skip, int? take) {
            // Note: http://redis.io/commands/zrevrangebyscore has max, min in the wrong other
            return this.GetRangeByScore(Commands.ZRevRangeByScore, setId, max, min, skip, take, false);
        }

        /// <inheritdoc />
        public byte[][] ZRevRangeByScore(string setId, long min, long max, int? skip, int? take) {
            // Note: http://redis.io/commands/zrevrangebyscore has max, min in the wrong other
            return this.GetRangeByScore(Commands.ZRevRangeByScore, setId, max, min, skip, take, false);
        }

        /// <inheritdoc />
        public byte[][] ZRevRangeByScoreWithScores(string setId, double min, double max, int? skip, int? take) {
            // Note: http://redis.io/commands/zrevrangebyscore has max, min in the wrong other
            return this.GetRangeByScore(Commands.ZRevRangeByScore, setId, max, min, skip, take, true);
        }

        /// <inheritdoc />
        public byte[][] ZRevRangeByScoreWithScores(string setId, long min, long max, int? skip, int? take) {
            // Note: http://redis.io/commands/zrevrangebyscore has max, min in the wrong other
            return this.GetRangeByScore(Commands.ZRevRangeByScore, setId, max, min, skip, take, true);
        }

        /// <inheritdoc />
        public long ZRemRangeByRank(string setId, int min, int max) {
            if (setId == null) {
                throw new ArgumentNullException(nameof(setId));
            }

            return this.SendExpectLong(Commands.ZRemRangeByRank,
                setId.ToUtf8Bytes(),
                min.ToUtf8Bytes(),
                max.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public long ZRemRangeByScore(string setId, double fromScore, double toScore) {
            if (setId == null) {
                throw new ArgumentNullException(nameof(setId));
            }

            return this.SendExpectLong(Commands.ZRemRangeByScore,
                setId.ToUtf8Bytes(),
                fromScore.ToFastUtf8Bytes(),
                toScore.ToFastUtf8Bytes());
        }

        /// <inheritdoc />
        public long ZRemRangeByScore(string setId, long fromScore, long toScore) {
            if (setId == null) {
                throw new ArgumentNullException(nameof(setId));
            }

            return this.SendExpectLong(Commands.ZRemRangeByScore,
                setId.ToUtf8Bytes(),
                fromScore.ToUtf8Bytes(),
                toScore.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public long ZCard(string setId) {
            if (setId == null) {
                throw new ArgumentNullException(nameof(setId));
            }

            return this.SendExpectLong(Commands.ZCard, setId.ToUtf8Bytes());
        }

        public long ZCount(string setId, double min, double max) {
            if (setId == null) {
                throw new ArgumentNullException(nameof(setId));
            }

            return this.SendExpectLong(Commands.ZCount, setId.ToUtf8Bytes(), min.ToUtf8Bytes(), max.ToUtf8Bytes());
        }

        public long ZCount(string setId, long min, long max) {
            if (setId == null) {
                throw new ArgumentNullException(nameof(setId));
            }

            return this.SendExpectLong(Commands.ZCount, setId.ToUtf8Bytes(), min.ToUtf8Bytes(), max.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public double ZScore(string setId, byte[] value) {
            if (setId == null) {
                throw new ArgumentNullException(nameof(setId));
            }

            return this.SendExpectDouble(Commands.ZScore, setId.ToUtf8Bytes(), value);
        }

        /// <inheritdoc />
        public long ZUnionStore(string intoSetId, params string[] setIds) {
            var setIdsList = new List<string>(setIds);
            setIdsList.Insert(0, setIds.Length.ToString());
            setIdsList.Insert(0, intoSetId);

            var cmdWithArgs = MergeCommandWithArgs(Commands.ZUnionStore, setIdsList.ToArray());
            return this.SendExpectLong(cmdWithArgs);
        }

        public long ZUnionStore(string intoSetId, string[] setIds, string[] args) {
            var totalArgs = new List<string>(setIds);
            totalArgs.Insert(0, setIds.Length.ToString());
            totalArgs.Insert(0, intoSetId);
            totalArgs.AddRange(args);

            var cmdWithArgs = MergeCommandWithArgs(Commands.ZUnionStore, totalArgs.ToArray());
            return this.SendExpectLong(cmdWithArgs);
        }

        /// <inheritdoc />
        public long ZInterStore(string intoSetId, params string[] setIds) {
            var setIdsList = new List<string>(setIds);
            setIdsList.Insert(0, setIds.Length.ToString());
            setIdsList.Insert(0, intoSetId);

            var cmdWithArgs = MergeCommandWithArgs(Commands.ZInterStore, setIdsList.ToArray());
            return this.SendExpectLong(cmdWithArgs);
        }

        public long ZInterStore(string intoSetId, string[] setIds, string[] args) {
            var totalArgs = new List<string>(setIds);
            totalArgs.Insert(0, setIds.Length.ToString());
            totalArgs.Insert(0, intoSetId);
            totalArgs.AddRange(args);

            var cmdWithArgs = MergeCommandWithArgs(Commands.ZInterStore, totalArgs.ToArray());
            return this.SendExpectLong(cmdWithArgs);
        }

        /// <inheritdoc />
        public byte[][] ZRangeByLex(string setId, string min, string max, int? skip = null, int? take = null) {
            if (setId == null) {
                throw new ArgumentNullException(nameof(setId));
            }

            var cmdWithArgs = new List<byte[]> {
                Commands.ZRangeByLex,
                setId.ToUtf8Bytes(),
                min.ToUtf8Bytes(),
                max.ToUtf8Bytes()
            };

            if (skip.HasValue || take.HasValue) {
                cmdWithArgs.Add(Commands.Limit);
                cmdWithArgs.Add(skip.GetValueOrDefault(0).ToUtf8Bytes());
                cmdWithArgs.Add(take.GetValueOrDefault(0).ToUtf8Bytes());
            }

            return this.SendExpectMultiData(cmdWithArgs.ToArray());
        }

        /// <inheritdoc />
        public long ZLexCount(string setId, string min, string max) {
            if (setId == null) {
                throw new ArgumentNullException(nameof(setId));
            }

            return this.SendExpectLong(
                Commands.ZLexCount,
                setId.ToUtf8Bytes(),
                min.ToUtf8Bytes(),
                max.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public long ZRemRangeByLex(string setId, string min, string max) {
            if (setId == null) {
                throw new ArgumentNullException(nameof(setId));
            }

            return this.SendExpectLong(
                Commands.ZRemRangeByLex,
                setId.ToUtf8Bytes(),
                min.ToUtf8Bytes(),
                max.ToUtf8Bytes());
        }

        #endregion

        #region Hash Operations

        private static void AssertHashIdAndKey(object hashId, byte[] key) {
            if (hashId == null) {
                throw new ArgumentNullException(nameof(hashId));
            }

            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }
        }

        /// <inheritdoc />
        public long HSet(string hashId, byte[] key, byte[] value) {
            return this.HSet(hashId.ToUtf8Bytes(), key, value);
        }

        public long HSet(byte[] hashId, byte[] key, byte[] value) {
            AssertHashIdAndKey(hashId, key);

            return this.SendExpectLong(Commands.HSet, hashId, key, value);
        }

        /// <inheritdoc />
        public long HSetNX(string hashId, byte[] key, byte[] value) {
            AssertHashIdAndKey(hashId, key);

            return this.SendExpectLong(Commands.HSetNx, hashId.ToUtf8Bytes(), key, value);
        }

        /// <inheritdoc />
        public void HMSet(string hashId, byte[][] keys, byte[][] values) {
            if (hashId == null) {
                throw new ArgumentNullException(nameof(hashId));
            }

            var cmdArgs = MergeCommandWithKeysAndValues(Commands.HMSet, hashId.ToUtf8Bytes(), keys, values);

            this.SendExpectSuccess(cmdArgs);
        }

        /// <inheritdoc />
        public long HIncrby(string hashId, byte[] key, int incrementBy) {
            AssertHashIdAndKey(hashId, key);

            return this.SendExpectLong(Commands.HIncrBy, hashId.ToUtf8Bytes(), key, incrementBy.ToString().ToUtf8Bytes());
        }

        public long HIncrby(string hashId, byte[] key, long incrementBy) {
            AssertHashIdAndKey(hashId, key);

            return this.SendExpectLong(Commands.HIncrBy, hashId.ToUtf8Bytes(), key, incrementBy.ToString().ToUtf8Bytes());
        }

        /// <inheritdoc />
        public double HIncrbyFloat(string hashId, byte[] key, double incrementBy) {
            AssertHashIdAndKey(hashId, key);

            return this.SendExpectDouble(Commands.HIncrByFloat,
                hashId.ToUtf8Bytes(),
                key,
                incrementBy.ToString(CultureInfo.InvariantCulture).ToUtf8Bytes());
        }

        /// <inheritdoc />
        public byte[] HGet(string hashId, byte[] key) {
            return this.HGet(hashId.ToUtf8Bytes(), key);
        }

        public byte[] HGet(byte[] hashId, byte[] key) {
            AssertHashIdAndKey(hashId, key);

            return this.SendExpectData(Commands.HGet, hashId, key);
        }

        /// <inheritdoc />
        public byte[][] HMGet(string hashId, params byte[][] keys) {
            if (hashId == null) {
                throw new ArgumentNullException(nameof(hashId));
            }

            if (keys.Length == 0) {
                throw new ArgumentNullException(nameof(keys));
            }

            var cmdArgs = MergeCommandWithArgs(Commands.HMGet, hashId.ToUtf8Bytes(), keys);

            return this.SendExpectMultiData(cmdArgs);
        }

        /// <inheritdoc />
        public long HDel(string hashId, byte[] key) {
            return this.HDel(hashId.ToUtf8Bytes(), key);
        }

        public long HDel(byte[] hashId, byte[] key) {
            AssertHashIdAndKey(hashId, key);

            return this.SendExpectLong(Commands.HDel, hashId, key);
        }

        public long HDel(string hashId, byte[][] keys) {
            if (hashId == null) {
                throw new ArgumentNullException(nameof(hashId));
            }

            if (keys == null || keys.Length == 0) {
                throw new ArgumentNullException(nameof(keys));
            }

            var cmdWithArgs = MergeCommandWithArgs(Commands.HDel, hashId.ToUtf8Bytes(), keys);
            return this.SendExpectLong(cmdWithArgs);
        }

        /// <inheritdoc />
        public long HExists(string hashId, byte[] key) {
            AssertHashIdAndKey(hashId, key);

            return this.SendExpectLong(Commands.HExists, hashId.ToUtf8Bytes(), key);
        }

        /// <inheritdoc />
        public long HLen(string hashId) {
            if (string.IsNullOrEmpty(hashId)) {
                throw new ArgumentNullException(nameof(hashId));
            }

            return this.SendExpectLong(Commands.HLen, hashId.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public byte[][] HKeys(string hashId) {
            if (hashId == null) {
                throw new ArgumentNullException(nameof(hashId));
            }

            return this.SendExpectMultiData(Commands.HKeys, hashId.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public byte[][] HVals(string hashId) {
            if (hashId == null) {
                throw new ArgumentNullException(nameof(hashId));
            }

            return this.SendExpectMultiData(Commands.HVals, hashId.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public byte[][] HGetAll(string hashId) {
            if (hashId == null) {
                throw new ArgumentNullException(nameof(hashId));
            }

            return this.SendExpectMultiData(Commands.HGetAll, hashId.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public long Publish(string toChannel, byte[] message) {
            return this.SendExpectLong(Commands.Publish, toChannel.ToUtf8Bytes(), message);
        }

        /// <inheritdoc />
        public byte[][] ReceiveMessages() {
            return this.ReadMultiData();
        }

        /// <inheritdoc />
        public virtual IRedisSubscription CreateSubscription() {
            return new RedisSubscription(this);
        }

        /// <inheritdoc />
        public byte[][] Subscribe(params string[] toChannels) {
            if (toChannels.Length == 0) {
                throw new ArgumentNullException(nameof(toChannels));
            }

            var cmdWithArgs = MergeCommandWithArgs(Commands.Subscribe, toChannels);
            return this.SendExpectMultiData(cmdWithArgs);
        }

        /// <inheritdoc />
        public byte[][] UnSubscribe(params string[] fromChannels) {
            var cmdWithArgs = MergeCommandWithArgs(Commands.UnSubscribe, fromChannels);
            return this.SendExpectMultiData(cmdWithArgs);
        }

        /// <inheritdoc />
        public byte[][] PSubscribe(params string[] toChannelsMatchingPatterns) {
            if (toChannelsMatchingPatterns.Length == 0) {
                throw new ArgumentNullException(nameof(toChannelsMatchingPatterns));
            }

            var cmdWithArgs = MergeCommandWithArgs(Commands.PSubscribe, toChannelsMatchingPatterns);
            return this.SendExpectMultiData(cmdWithArgs);
        }

        /// <inheritdoc />
        public byte[][] PUnSubscribe(params string[] fromChannelsMatchingPatterns) {
            var cmdWithArgs = MergeCommandWithArgs(Commands.PUnSubscribe, fromChannelsMatchingPatterns);
            return this.SendExpectMultiData(cmdWithArgs);
        }

        public RedisPipelineCommand CreatePipelineCommand() {
            this.AssertConnectedSocket();
            return new RedisPipelineCommand(this);
        }

        #endregion

        #region GEO Operations

        /// <inheritdoc />
        public long GeoAdd(string key, double longitude, double latitude, string member) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            if (member == null) {
                throw new ArgumentNullException(nameof(member));
            }

            return this.SendExpectLong(Commands.GeoAdd,
                key.ToUtf8Bytes(),
                longitude.ToUtf8Bytes(),
                latitude.ToUtf8Bytes(),
                member.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public long GeoAdd(string key, params RedisGeo[] geoPoints) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            var members = new byte[geoPoints.Length * 3][];
            for (var i = 0; i < geoPoints.Length; i++) {
                var geoPoint = geoPoints[i];
                members[i * 3 + 0] = geoPoint.Longitude.ToUtf8Bytes();
                members[i * 3 + 1] = geoPoint.Latitude.ToUtf8Bytes();
                members[i * 3 + 2] = geoPoint.Member.ToUtf8Bytes();
            }

            var cmdWithArgs = MergeCommandWithArgs(Commands.GeoAdd, key.ToUtf8Bytes(), members);
            return this.SendExpectLong(cmdWithArgs);
        }

        /// <inheritdoc />
        public double GeoDist(string key, string fromMember, string toMember, string unit = null) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            return unit == null
                ? this.SendExpectDouble(Commands.GeoDist, key.ToUtf8Bytes(), fromMember.ToUtf8Bytes(), toMember.ToUtf8Bytes())
                : this.SendExpectDouble(Commands.GeoDist,
                    key.ToUtf8Bytes(),
                    fromMember.ToUtf8Bytes(),
                    toMember.ToUtf8Bytes(),
                    unit.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public string[] GeoHash(string key, params string[] members) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            var cmdWithArgs =
                MergeCommandWithArgs(Commands.GeoHash, key.ToUtf8Bytes(), members.Select(x => x.ToUtf8Bytes()).ToArray());
            return this.SendExpectMultiData(cmdWithArgs).ToStringArray();
        }

        /// <inheritdoc />
        public List<RedisGeo> GeoPos(string key, params string[] members) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            var cmdWithArgs = MergeCommandWithArgs(Commands.GeoPos, key.ToUtf8Bytes(), members.Select(x => x.ToUtf8Bytes()).ToArray());
            var data = this.SendExpectComplexResponse(cmdWithArgs);
            var to = new List<RedisGeo>();

            for (var i = 0; i < members.Length; i++) {
                if (data.Children.Count <= i) {
                    break;
                }

                var entry = data.Children[i];

                var children = entry.Children;
                if (children.Count == 0) {
                    continue;
                }

                to.Add(new RedisGeo {
                    Longitude = children[0].ToDouble(),
                    Latitude = children[1].ToDouble(),
                    Member = members[i]
                });
            }

            return to;
        }

        /// <inheritdoc />
        public List<RedisGeoResult> GeoRadius(string key, double longitude, double latitude, double radius, string unit,
            bool withCoords = false, bool withDist = false, bool withHash = false, int? count = null, bool? asc = null) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            var args = new List<byte[]> {
                longitude.ToUtf8Bytes(),
                latitude.ToUtf8Bytes(),
                radius.ToUtf8Bytes(),
                Commands.GetUnit(unit)
            };

            if (withCoords) {
                args.Add(Commands.WithCoord);
            }

            if (withDist) {
                args.Add(Commands.WithDist);
            }

            if (withHash) {
                args.Add(Commands.WithHash);
            }

            if (count != null) {
                args.Add(Commands.Count);
                args.Add(count.Value.ToUtf8Bytes());
            }

            if (asc == true) {
                args.Add(Commands.Asc);
            } else if (asc == false) {
                args.Add(Commands.Desc);
            }

            var cmdWithArgs = MergeCommandWithArgs(Commands.GeoRadius, key.ToUtf8Bytes(), args.ToArray());

            var to = new List<RedisGeoResult>();

            if (!(withCoords || withDist || withHash)) {
                var members = this.SendExpectMultiData(cmdWithArgs).ToStringArray();
                foreach (var member in members) {
                    to.Add(new RedisGeoResult { Member = member });
                }
            } else {
                var data = this.SendExpectComplexResponse(cmdWithArgs);

                foreach (var child in data.Children) {
                    var i = 0;
                    var result = new RedisGeoResult { Unit = unit, Member = child.Children[i++].Data.FromUtf8Bytes() };

                    if (withDist) {
                        result.Distance = child.Children[i++].ToDouble();
                    }

                    if (withHash) {
                        result.Hash = child.Children[i++].ToInt64();
                    }

                    if (withCoords) {
                        var children = child.Children[i].Children;
                        result.Longitude = children[0].ToDouble();
                        result.Latitude = children[1].ToDouble();
                    }

                    to.Add(result);
                }
            }

            return to;
        }

        /// <inheritdoc />
        public List<RedisGeoResult> GeoRadiusByMember(string key, string member, double radius, string unit,
            bool withCoords = false, bool withDist = false, bool withHash = false, int? count = null, bool? asc = null) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }

            var args = new List<byte[]> {
                member.ToUtf8Bytes(),
                radius.ToUtf8Bytes(),
                Commands.GetUnit(unit)
            };

            if (withCoords) {
                args.Add(Commands.WithCoord);
            }

            if (withDist) {
                args.Add(Commands.WithDist);
            }

            if (withHash) {
                args.Add(Commands.WithHash);
            }

            if (count != null) {
                args.Add(Commands.Count);
                args.Add(count.Value.ToUtf8Bytes());
            }

            if (asc == true) {
                args.Add(Commands.Asc);
            } else if (asc == false) {
                args.Add(Commands.Desc);
            }

            var cmdWithArgs = MergeCommandWithArgs(Commands.GeoRadiusByMember, key.ToUtf8Bytes(), args.ToArray());

            var to = new List<RedisGeoResult>();

            if (!(withCoords || withDist || withHash)) {
                var members = this.SendExpectMultiData(cmdWithArgs).ToStringArray();
                foreach (var x in members) {
                    to.Add(new RedisGeoResult { Member = x });
                }
            } else {
                var data = this.SendExpectComplexResponse(cmdWithArgs);

                foreach (var child in data.Children) {
                    var i = 0;
                    var result = new RedisGeoResult { Unit = unit, Member = child.Children[i++].Data.FromUtf8Bytes() };

                    if (withDist) {
                        result.Distance = child.Children[i++].ToDouble();
                    }

                    if (withHash) {
                        result.Hash = child.Children[i++].ToInt64();
                    }

                    if (withCoords) {
                        var children = child.Children[i].Children;
                        result.Longitude = children[0].ToDouble();
                        result.Latitude = children[1].ToDouble();
                    }

                    to.Add(result);
                }
            }

            return to;
        }

        #endregion

    }

}
