using System.Collections.Generic;

namespace TheOne.Redis.Client {

    public partial interface IRedisClient {

        bool HashContainsEntry(string hashId, string key);

        bool SetEntryInHash(string hashId, string key, string value);

        bool SetEntryInHashIfNotExists(string hashId, string key, string value);

        void SetRangeInHash(string hashId, IEnumerable<KeyValuePair<string, string>> keyValuePairs);

        long IncrementValueInHash(string hashId, string key, int incrementBy);

        double IncrementValueInHash(string hashId, string key, double incrementBy);

        string GetValueFromHash(string hashId, string key);

        List<string> GetValuesFromHash(string hashId, params string[] keys);

        bool RemoveEntryFromHash(string hashId, string key);

        long GetHashCount(string hashId);

        List<string> GetHashKeys(string hashId);

        List<string> GetHashValues(string hashId);

        Dictionary<string, string> GetAllEntriesFromHash(string hashId);

    }

}
