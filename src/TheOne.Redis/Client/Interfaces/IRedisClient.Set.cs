using System.Collections.Generic;

namespace TheOne.Redis.Client {

    public partial interface IRedisClient {

        #region Set operations

        HashSet<string> GetAllItemsFromSet(string setId);

        void AddItemToSet(string setId, string item);

        void AddRangeToSet(string setId, List<string> items);

        void RemoveItemFromSet(string setId, string item);

        string PopItemFromSet(string setId);

        List<string> PopItemsFromSet(string setId, int count);

        void MoveBetweenSets(string fromSetId, string toSetId, string item);

        long GetSetCount(string setId);

        bool SetContainsItem(string setId, string item);

        HashSet<string> GetIntersectFromSets(params string[] setIds);

        void StoreIntersectFromSets(string intoSetId, params string[] setIds);

        HashSet<string> GetUnionFromSets(params string[] setIds);

        void StoreUnionFromSets(string intoSetId, params string[] setIds);

        HashSet<string> GetDifferencesFromSet(string fromSetId, params string[] withSetIds);

        void StoreDifferencesFromSet(string intoSetId, string fromSetId, params string[] withSetIds);

        string GetRandomItemFromSet(string setId);

        #endregion

    }

}
