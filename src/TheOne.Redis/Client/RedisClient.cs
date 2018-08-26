using System;
using System.Collections.Generic;
using System.Linq;
using TheOne.Redis.Client.Internal;
using TheOne.Redis.Common;
using TheOne.Redis.Pipeline;
using TheOne.Redis.PubSub;

namespace TheOne.Redis.Client {

    /// <summary>
    ///     The client wraps the native redis operations into a more readable c# API.
    ///     Where possible these operations are also exposed in common c# interfaces,
    ///     e.g. RedisClient.Lists => IList[string]
    ///     RedisClient.Sets => ICollection[string]
    /// </summary>
    public partial class RedisClient : RedisNativeClient {

        internal static readonly HashSet<Type> UniqueTypes = new HashSet<Type>();

        public static Func<RedisClient> NewFactoryFn = () => new RedisClient();

        /// <summary>
        ///     can't contain null values
        /// </summary>
        public static Func<object, Dictionary<string, string>> ConvertToHashFn =
            x => {
                // ignore null value by default
                return x.ToJson().FromJson<Dictionary<string, string>>()
                        .Where(pair => pair.Value != null)
                        .ToDictionary(pair => pair.Key, pair => pair.Value);
            };

        public RedisClient() {
            this.Init();
        }

        public RedisClient(string host) : base(host) {
            this.Init();
        }

        public RedisClient(RedisEndpoint config) : base(config) {
            this.Init();
        }

        public RedisClient(string host, int port) : base(host, port) {
            this.Init();
        }

        public RedisClient(string host, int port, string password = null, long db = RedisConfig.DefaultDb) :
            base(host, port, password, db) {
            this.Init();
        }

        public RedisClient(Uri uri)
            : base(uri.Host, uri.Port) {
            var password = !string.IsNullOrEmpty(uri.UserInfo) ? uri.UserInfo.Split(':').Last() : null;
            this.Password = password;
            this.Init();
        }

        /// <inheritdoc />
        public string this[string key] {
            get => this.GetValue(key);
            set => this.SetValue(key, value);
        }

        /// <inheritdoc />
        public RedisText Custom(params object[] cmdWithArgs) {
            RedisData data = this.RawCommand(cmdWithArgs);
            RedisText ret = data.ToRedisText();
            return ret;
        }

        /// <inheritdoc />
        public void RewriteAppendOnlyFileAsync() {
            this.BgRewriteAof();
        }

        /// <inheritdoc />
        public List<string> GetAllKeys() {
            return this.SearchKeys("*");
        }

        /// <inheritdoc />
        public void SetValue(string key, string value) {
            byte[] bytesValue = value?.ToUtf8Bytes();
            base.Set(key, bytesValue);
        }

        /// <inheritdoc />
        public void SetValue(string key, string value, TimeSpan expireIn) {
            byte[] bytesValue = value?.ToUtf8Bytes();

            if (this.AssertServerVersionNumber() >= 2610) {
                if (expireIn.Milliseconds > 0) {
                    base.Set(key, bytesValue, 0, (long)expireIn.TotalMilliseconds);
                } else {
                    this.Set(key, bytesValue, (int)expireIn.TotalSeconds);
                }
            } else {
                this.SetEx(key, (int)expireIn.TotalSeconds, bytesValue);
            }
        }

        /// <inheritdoc />
        public bool SetValueIfExists(string key, string value) {
            byte[] bytesValue = value?.ToUtf8Bytes();
            return base.Set(key, bytesValue, true);
        }

        /// <inheritdoc />
        public bool SetValueIfNotExists(string key, string value) {
            byte[] bytesValue = value?.ToUtf8Bytes();
            return base.Set(key, bytesValue, false);
        }

        /// <inheritdoc />
        public void SetValues(Dictionary<string, string> map) {
            this.SetAll(map);
        }

        /// <inheritdoc />
        public void SetAll(IEnumerable<string> keys, IEnumerable<string> values) {
            if (keys == null || values == null) {
                return;
            }

            string[] keyArray = keys.ToArray();
            string[] valueArray = values.ToArray();

            if (keyArray.Length != valueArray.Length) {
                throw new Exception(string.Format("Key length != Value Length. {0}/{1}", keyArray.Length, valueArray.Length));
            }

            if (keyArray.Length == 0) {
                return;
            }

            var keyBytes = new byte[keyArray.Length][];
            var valBytes = new byte[keyArray.Length][];
            for (var i = 0; i < keyArray.Length; i++) {
                keyBytes[i] = keyArray[i].ToUtf8Bytes();
                valBytes[i] = valueArray[i].ToUtf8Bytes();
            }

            this.MSet(keyBytes, valBytes);
        }

        /// <inheritdoc />
        public void SetAll(Dictionary<string, string> map) {
            if (map == null || map.Count == 0) {
                return;
            }

            var keyBytes = new byte[map.Count][];
            var valBytes = new byte[map.Count][];

            var i = 0;
            foreach (var key in map.Keys) {
                var val = map[key];
                keyBytes[i] = key.ToUtf8Bytes();
                valBytes[i] = val.ToUtf8Bytes();
                i++;
            }

            this.MSet(keyBytes, valBytes);
        }

        /// <inheritdoc />
        public string GetValue(string key) {
            byte[] bytes = this.Get(key);
            return bytes?.FromUtf8Bytes();
        }

        /// <inheritdoc />
        public string GetAndSetValue(string key, string value) {
            return this.GetSet(key, value.ToUtf8Bytes()).FromUtf8Bytes();
        }

        /// <inheritdoc />
        public bool ContainsKey(string key) {
            return this.Exists(key) == Success;
        }

        /// <inheritdoc />
        public bool Remove(string key) {
            return this.Del(key) == Success;
        }

        /// <inheritdoc />
        public bool RemoveEntry(params string[] keys) {
            if (keys.Length == 0) {
                return false;
            }

            return this.Del(keys) == Success;
        }

        /// <inheritdoc />
        public long IncrementValue(string key) {
            return this.Incr(key);
        }

        /// <inheritdoc />
        public long IncrementValueBy(string key, int count) {
            return this.IncrBy(key, count);
        }

        /// <inheritdoc />
        public long IncrementValueBy(string key, long count) {
            return this.IncrBy(key, count);
        }

        /// <inheritdoc />
        public double IncrementValueBy(string key, double count) {
            return this.IncrByFloat(key, count);
        }

        /// <inheritdoc />
        public long DecrementValue(string key) {
            return this.Decr(key);
        }

        /// <inheritdoc />
        public long DecrementValueBy(string key, int count) {
            return this.DecrBy(key, count);
        }

        /// <inheritdoc />
        public long AppendToValue(string key, string value) {
            return this.Append(key, value.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public void RenameKey(string fromName, string toName) {
            this.Rename(fromName, toName);
        }

        /// <inheritdoc />
        public long GetStringCount(string key) {
            return this.StrLen(key);
        }

        /// <inheritdoc />
        public string GetRandomKey() {
            return this.RandomKey();
        }

        /// <inheritdoc />
        public bool ExpireEntryIn(string key, TimeSpan expireIn) {
            if (this.AssertServerVersionNumber() >= 2600) {
                if (expireIn.Milliseconds > 0) {
                    return this.PExpire(key, (long)expireIn.TotalMilliseconds);
                }
            }

            return this.Expire(key, (int)expireIn.TotalSeconds);
        }

        /// <inheritdoc />
        public bool ExpireEntryAt(string key, DateTime expireAt) {
            if (this.AssertServerVersionNumber() >= 2600) {
                return this.PExpireAt(key, this.ConvertToServerDate(expireAt).ToUnixTimeMs());
            }

            return this.ExpireAt(key, this.ConvertToServerDate(expireAt).ToUnixTime());
        }

        /// <inheritdoc />
        public TimeSpan? GetTimeToLive(string key) {
            var ttlSecs = this.Ttl(key);
            if (ttlSecs == -1) {
                return TimeSpan.MaxValue; // no expiry set
            }

            if (ttlSecs == -2) {
                return null; // key does not exist
            }

            return TimeSpan.FromSeconds(ttlSecs);
        }

        /// <inheritdoc />
        public IRedisTypedClient<T> As<T>() {
            try {
                var typedClient = new RedisTypedClient<T>(this);
                return typedClient;
            } catch (TypeInitializationException ex) {
                throw GetInnerMostException(ex);
            }
        }

        /// <inheritdoc />
        public IDisposable AcquireLock(string key) {
            return new RedisLock(this, key, null);
        }

        /// <inheritdoc />
        public IDisposable AcquireLock(string key, TimeSpan timeout) {
            return new RedisLock(this, key, timeout);
        }

        /// <inheritdoc />
        public IRedisTransaction CreateTransaction() {
            this.AssertServerVersionNumber(); // pre-fetch call to INFO before transaction if needed
            return new RedisTransaction(this);
        }

        /// <inheritdoc />
        public IRedisPipeline CreatePipeline() {
            return new RedisAllPurposePipeline(this);
        }

        /// <inheritdoc />
        public List<string> SearchKeys(string pattern) {
            byte[][] multiDataList = this.Keys(pattern);
            return multiDataList.ToStringList();
        }

        /// <inheritdoc />
        public List<string> GetValues(List<string> keys) {
            if (keys == null) {
                throw new ArgumentNullException(nameof(keys));
            }

            if (keys.Count == 0) {
                return new List<string>();
            }

            byte[][] resultBytesArray = this.MGet(keys.ToArray());

            var results = new List<string>();
            foreach (byte[] resultBytes in resultBytesArray) {
                if (resultBytes == null) {
                    continue;
                }

                var resultString = resultBytes.FromUtf8Bytes();
                results.Add(resultString);
            }

            return results;
        }

        /// <inheritdoc />
        public List<T> GetValues<T>(List<string> keys) {
            if (keys == null) {
                throw new ArgumentNullException(nameof(keys));
            }

            if (keys.Count == 0) {
                return new List<T>();
            }

            byte[][] resultBytesArray = this.MGet(keys.ToArray());

            var results = new List<T>();
            foreach (byte[] resultBytes in resultBytesArray) {
                if (resultBytes == null) {
                    continue;
                }

                var resultString = resultBytes.FromUtf8Bytes();
                var result = resultString.FromJson<T>();
                results.Add(result);
            }

            return results;
        }

        /// <inheritdoc />
        public Dictionary<string, string> GetValuesMap(List<string> keys) {
            if (keys == null) {
                throw new ArgumentNullException(nameof(keys));
            }

            if (keys.Count == 0) {
                return new Dictionary<string, string>();
            }

            string[] keysArray = keys.ToArray();
            byte[][] resultBytesArray = this.MGet(keysArray);

            var results = new Dictionary<string, string>();
            for (var i = 0; i < resultBytesArray.Length; i++) {
                var key = keysArray[i];

                byte[] resultBytes = resultBytesArray[i];
                if (resultBytes == null) {
                    results.Add(key, null);
                } else {
                    var resultString = resultBytes.FromUtf8Bytes();
                    results.Add(key, resultString);
                }
            }

            return results;
        }

        /// <inheritdoc />
        public Dictionary<string, T> GetValuesMap<T>(List<string> keys) {
            if (keys == null) {
                throw new ArgumentNullException(nameof(keys));
            }

            if (keys.Count == 0) {
                return new Dictionary<string, T>();
            }

            string[] keysArray = keys.ToArray();
            byte[][] resultBytesArray = this.MGet(keysArray);

            var results = new Dictionary<string, T>();
            for (var i = 0; i < resultBytesArray.Length; i++) {
                var key = keysArray[i];

                byte[] resultBytes = resultBytesArray[i];
                if (resultBytes == null) {
                    results.Add(key, default);
                } else {
                    var resultString = resultBytes.FromUtf8Bytes();
                    var result = resultString.FromJson<T>();
                    results.Add(key, result);
                }
            }

            return results;
        }

        public override IRedisSubscription CreateSubscription() {
            return new RedisSubscription(this);
        }

        /// <inheritdoc />
        public long PublishMessage(string toChannel, string message) {
            return this.Publish(toChannel, message.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public IEnumerable<string> ScanAllKeys(string pattern = null, int pageSize = 1000) {
            var ret = new ScanResult();
            while (true) {
                ret = pattern != null
                    ? this.Scan(ret.Cursor, pageSize, pattern)
                    : this.Scan(ret.Cursor, pageSize);

                foreach (byte[] key in ret.Results) {
                    yield return key.FromUtf8Bytes();
                }

                if (ret.Cursor == 0) {
                    break;
                }
            }
        }

        /// <inheritdoc />
        public IEnumerable<string> ScanAllSetItems(string setId, string pattern = null, int pageSize = 1000) {
            var ret = new ScanResult();
            while (true) {
                ret = pattern != null
                    ? this.SScan(setId, ret.Cursor, pageSize, pattern)
                    : this.SScan(setId, ret.Cursor, pageSize);

                foreach (byte[] key in ret.Results) {
                    yield return key.FromUtf8Bytes();
                }

                if (ret.Cursor == 0) {
                    break;
                }
            }
        }

        /// <inheritdoc />
        public IEnumerable<KeyValuePair<string, double>> ScanAllSortedSetItems(string setId, string pattern = null, int pageSize = 1000) {
            var ret = new ScanResult();
            while (true) {
                ret = pattern != null
                    ? this.ZScan(setId, ret.Cursor, pageSize, pattern)
                    : this.ZScan(setId, ret.Cursor, pageSize);

                foreach (KeyValuePair<string, double> entry in ret.AsItemsWithScores()) {
                    yield return entry;
                }

                if (ret.Cursor == 0) {
                    break;
                }
            }
        }

        /// <inheritdoc />
        public IEnumerable<KeyValuePair<string, string>> ScanAllHashEntries(string hashId, string pattern = null, int pageSize = 1000) {
            var ret = new ScanResult();
            while (true) {
                ret = pattern != null
                    ? this.HScan(hashId, ret.Cursor, pageSize, pattern)
                    : this.HScan(hashId, ret.Cursor, pageSize);

                foreach (KeyValuePair<string, string> entry in ret.AsKeyValues()) {
                    yield return entry;
                }

                if (ret.Cursor == 0) {
                    break;
                }
            }
        }

        /// <inheritdoc />
        public bool AddToHyperLog(string key, params string[] elements) {
            return this.PfAdd(key, elements.Select(x => x.ToUtf8Bytes()).ToArray());
        }

        /// <inheritdoc />
        public long CountHyperLog(string key) {
            return this.PfCount(key);
        }

        /// <inheritdoc />
        public void MergeHyperLogs(string toKey, params string[] fromKeys) {
            this.PfMerge(toKey, fromKeys);
        }

        /// <inheritdoc />
        public RedisServerRole GetServerRole() {
            if (this.AssertServerVersionNumber() >= 2812) {
                RedisText text = this.Role();
                var roleName = text.Children[0].Text;
                return ToServerRole(roleName);
            }

            this.Info.TryGetValue("role", out var role);
            return ToServerRole(role);
        }

        /// <summary>
        ///     Creates a new instance of the Redis Client from NewFactoryFn.
        /// </summary>
        public static RedisClient New() {
            return NewFactoryFn();
        }

        public void Init() {
            this.Lists = new RedisClientLists(this);
            this.Sets = new RedisClientSets(this);
            this.SortedSets = new RedisClientSortedSets(this);
            this.Hashes = new RedisClientHashes(this);
        }

        /// <inheritdoc />
        protected override void OnConnected() { }

        public DateTime ConvertToServerDate(DateTime expiresAt) {
            return expiresAt;
        }

        public string GetTypeSequenceKey<T>() {
            return string.Concat(this.NamespacePrefix, "seq:", typeof(T).Name);
        }

        public string GetTypeIdsSetKey<T>() {
            return string.Concat(this.NamespacePrefix, "ids:", typeof(T).Name);
        }

        public string GetTypeIdsSetKey(Type type) {
            return string.Concat(this.NamespacePrefix, "ids:", type.Name);
        }

        public bool SetValue(byte[] key, byte[] value, TimeSpan expireIn) {
            if (this.AssertServerVersionNumber() >= 2600) {
                this.Exec(r => r.Set(key, value, 0, (long)expireIn.TotalMilliseconds));
            } else {
                this.Exec(r => r.SetEx(key, (int)expireIn.TotalSeconds, value));
            }

            return true;
        }

        public bool SetValueIfExists(string key, string value, TimeSpan expireIn) {
            byte[] bytesValue = value?.ToUtf8Bytes();

            if (expireIn.Milliseconds > 0) {
                return base.Set(key, bytesValue, true, expiryMs: (long)expireIn.TotalMilliseconds);
            }

            return base.Set(key, bytesValue, true, (int)expireIn.TotalSeconds);
        }

        public bool SetValueIfNotExists(string key, string value, TimeSpan expireIn) {
            byte[] bytesValue = value?.ToUtf8Bytes();

            if (expireIn.Milliseconds > 0) {
                return base.Set(key, bytesValue, false, expiryMs: (long)expireIn.TotalMilliseconds);
            }

            return base.Set(key, bytesValue, false, (int)expireIn.TotalSeconds);
        }

        public bool Remove(byte[] key) {
            return this.Del(key) == Success;
        }

        public bool ExpireEntryIn(byte[] key, TimeSpan expireIn) {
            if (this.AssertServerVersionNumber() >= 2600) {
                if (expireIn.Milliseconds > 0) {
                    return this.PExpire(key, (long)expireIn.TotalMilliseconds);
                }
            }

            return this.Expire(key, (int)expireIn.TotalSeconds);
        }

        private static Exception GetInnerMostException(Exception ex) {
            // Extract true exception from static intializers (e.g. LicenseException)
            while (ex.InnerException != null) {
                ex = ex.InnerException;
            }

            return ex;
        }

        public void AssertNotInTransaction() {
            if (this.Transaction != null) {
                throw new NotSupportedException("Only atomic redis-server operations are supported in a transaction");
            }
        }

        private static RedisServerRole ToServerRole(string roleName) {
            if (string.IsNullOrEmpty(roleName)) {
                return RedisServerRole.Unknown;
            }

            switch (roleName) {
                case "master":
                    return RedisServerRole.Master;
                case "slave":
                    return RedisServerRole.Slave;
                case "sentinel":
                    return RedisServerRole.Sentinel;
                default:
                    return RedisServerRole.Unknown;
            }
        }

        internal RedisClient LimitAccessToThread(int originalThreadId, string originalStackTrace) {
            this.TrackThread = new TrackThread(originalThreadId, originalStackTrace);
            return this;
        }

    }

}
