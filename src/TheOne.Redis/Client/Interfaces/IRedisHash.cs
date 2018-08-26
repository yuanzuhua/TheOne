using System.Collections.Generic;

namespace TheOne.Redis.Client {

    public interface IRedisHash : IDictionary<string, string>, IHasRedisStringId {

        bool AddIfNotExists(KeyValuePair<string, string> item);
        void AddRange(IEnumerable<KeyValuePair<string, string>> items);
        long IncrementValue(string key, int incrementBy);

    }

    public interface IRedisHash<TKey, TValue> : IDictionary<TKey, TValue>, IHasRedisStringId {

        Dictionary<TKey, TValue> GetAll();

    }

}
