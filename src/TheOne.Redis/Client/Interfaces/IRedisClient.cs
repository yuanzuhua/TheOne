using System;
using System.Collections.Generic;
using TheOne.Redis.Pipeline;

namespace TheOne.Redis.Client {

    public partial interface IRedisClient : IEntityStore, ICacheClientExtended {

        long Db { get; set; }
        long DbSize { get; }
        Dictionary<string, string> Info { get; }
        DateTime LastSave { get; }
        string Host { get; }
        int Port { get; }
        int ConnectTimeout { get; set; }
        int RetryTimeout { get; set; }
        int RetryCount { get; set; }
        int SendTimeout { get; set; }
        string Password { get; set; }
        bool HadExceptions { get; }

        string this[string key] { get; set; }

        IHasNamed<IRedisList> Lists { get; set; }
        IHasNamed<IRedisSet> Sets { get; set; }
        IHasNamed<IRedisSortedSet> SortedSets { get; set; }
        IHasNamed<IRedisHash> Hashes { get; set; }
        DateTime GetServerTime();

        bool Ping();
        string Echo(string text);

        RedisText Custom(params object[] cmdWithArgs);

        void Save();
        void SaveAsync();
        void Shutdown();
        void ShutdownNoSave();
        void RewriteAppendOnlyFileAsync();
        void FlushDb();

        RedisServerRole GetServerRole();
        RedisText GetServerRoleInfo();
        string GetConfig(string item);
        void SetConfig(string item, string value);
        void SaveConfig();
        void ResetInfoStats();

        string GetClient();
        void SetClient(string name);
        void KillClient(string address);
        long KillClients(string fromAddress = null, string withId = null, RedisClientType? ofType = null, bool? skipMe = null);
        List<Dictionary<string, string>> GetClientsInfo();
        void PauseAllClients(TimeSpan duration);

        List<string> GetAllKeys();

        string UrnKey<T>(T value);
        string UrnKey<T>(object id);
        string UrnKey(Type type, object id);

        void SetAll(IEnumerable<string> keys, IEnumerable<string> values);
        void SetAll(Dictionary<string, string> map);
        void SetValues(Dictionary<string, string> map);

        void SetValue(string key, string value);
        void SetValue(string key, string value, TimeSpan expireIn);
        bool SetValueIfNotExists(string key, string value);
        bool SetValueIfExists(string key, string value);

        string GetValue(string key);
        string GetAndSetValue(string key, string value);

        List<string> GetValues(List<string> keys);
        List<T> GetValues<T>(List<string> keys);
        Dictionary<string, string> GetValuesMap(List<string> keys);
        Dictionary<string, T> GetValuesMap<T>(List<string> keys);
        long AppendToValue(string key, string value);
        void RenameKey(string fromName, string toName);

        T GetFromHash<T>(object id);
        void StoreAsHash<T>(T entity);

        object StoreObject(object entity);

        bool ContainsKey(string key);
        bool RemoveEntry(params string[] args);
        long IncrementValue(string key);
        long IncrementValueBy(string key, int count);
        long IncrementValueBy(string key, long count);
        double IncrementValueBy(string key, double count);
        long DecrementValue(string key);
        long DecrementValueBy(string key, int count);
        List<string> SearchKeys(string pattern);

        string Type(string key);
        RedisKeyType GetEntryType(string key);
        long GetStringCount(string key);
        string GetRandomKey();
        bool ExpireEntryIn(string key, TimeSpan expireIn);
        bool ExpireEntryAt(string key, DateTime expireAt);
        List<string> GetSortedEntryValues(string key, int startingFrom, int endingAt);

        // Store entities without registering entity ids
        void WriteAll<TEntity>(IEnumerable<TEntity> entities);

        /// <summary>
        ///     Returns a high-level typed client API
        /// </summary>
        IRedisTypedClient<T> As<T>();

        IRedisTransaction CreateTransaction();
        IRedisPipeline CreatePipeline();

        IDisposable AcquireLock(string key);
        IDisposable AcquireLock(string key, TimeSpan timeout);

    }

}
