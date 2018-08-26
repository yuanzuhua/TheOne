using System.Collections.Generic;

namespace TheOne.Redis.Client {

    public partial interface IRedisClient {

        IEnumerable<string> ScanAllKeys(string pattern = null, int pageSize = 1000);

        IEnumerable<string> ScanAllSetItems(string setId, string pattern = null, int pageSize = 1000);

        IEnumerable<KeyValuePair<string, double>> ScanAllSortedSetItems(string setId, string pattern = null, int pageSize = 1000);

        IEnumerable<KeyValuePair<string, string>> ScanAllHashEntries(string hashId, string pattern = null, int pageSize = 1000);

    }

}
