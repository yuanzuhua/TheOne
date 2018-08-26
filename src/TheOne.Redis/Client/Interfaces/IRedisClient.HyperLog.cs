namespace TheOne.Redis.Client {

    public partial interface IRedisClient {

        bool AddToHyperLog(string key, params string[] elements);

        long CountHyperLog(string key);

        void MergeHyperLogs(string toKey, params string[] fromKeys);

    }

}
