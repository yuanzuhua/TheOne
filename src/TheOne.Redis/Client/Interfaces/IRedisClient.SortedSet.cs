using System.Collections.Generic;

namespace TheOne.Redis.Client {

    public partial interface IRedisClient {

        bool AddItemToSortedSet(string setId, string value);

        bool AddItemToSortedSet(string setId, string value, double score);

        bool AddRangeToSortedSet(string setId, List<string> values, double score);

        bool AddRangeToSortedSet(string setId, List<string> values, long score);

        bool RemoveItemFromSortedSet(string setId, string value);

        long RemoveItemsFromSortedSet(string setId, List<string> values);

        string PopItemWithLowestScoreFromSortedSet(string setId);

        string PopItemWithHighestScoreFromSortedSet(string setId);

        bool SortedSetContainsItem(string setId, string value);

        double IncrementItemInSortedSet(string setId, string value, double incrementBy);

        double IncrementItemInSortedSet(string setId, string value, long incrementBy);

        long GetItemIndexInSortedSet(string setId, string value);

        long GetItemIndexInSortedSetDesc(string setId, string value);

        List<string> GetAllItemsFromSortedSet(string setId);

        List<string> GetAllItemsFromSortedSetDesc(string setId);

        List<string> GetRangeFromSortedSet(string setId, int fromRank, int toRank);

        List<string> GetRangeFromSortedSetDesc(string setId, int fromRank, int toRank);

        IDictionary<string, double> GetAllWithScoresFromSortedSet(string setId);

        IDictionary<string, double> GetRangeWithScoresFromSortedSet(string setId, int fromRank, int toRank);

        IDictionary<string, double> GetRangeWithScoresFromSortedSetDesc(string setId, int fromRank, int toRank);

        List<string> GetRangeFromSortedSetByLowestScore(string setId, string fromStringScore, string toStringScore);

        List<string> GetRangeFromSortedSetByLowestScore(string setId, string fromStringScore, string toStringScore, int? skip, int? take);

        List<string> GetRangeFromSortedSetByLowestScore(string setId, double fromScore, double toScore);

        List<string> GetRangeFromSortedSetByLowestScore(string setId, long fromScore, long toScore);

        List<string> GetRangeFromSortedSetByLowestScore(string setId, double fromScore, double toScore, int? skip, int? take);

        List<string> GetRangeFromSortedSetByLowestScore(string setId, long fromScore, long toScore, int? skip, int? take);

        IDictionary<string, double>
            GetRangeWithScoresFromSortedSetByLowestScore(string setId, string fromStringScore, string toStringScore);

        IDictionary<string, double> GetRangeWithScoresFromSortedSetByLowestScore(string setId, string fromStringScore, string toStringScore,
            int? skip, int? take);

        IDictionary<string, double> GetRangeWithScoresFromSortedSetByLowestScore(string setId, double fromScore, double toScore);

        IDictionary<string, double> GetRangeWithScoresFromSortedSetByLowestScore(string setId, long fromScore, long toScore);

        IDictionary<string, double> GetRangeWithScoresFromSortedSetByLowestScore(string setId, double fromScore, double toScore, int? skip,
            int? take);

        IDictionary<string, double> GetRangeWithScoresFromSortedSetByLowestScore(string setId, long fromScore, long toScore, int? skip,
            int? take);

        List<string> GetRangeFromSortedSetByHighestScore(string setId, string fromStringScore, string toStringScore);

        List<string> GetRangeFromSortedSetByHighestScore(string setId, string fromStringScore, string toStringScore, int? skip, int? take);

        List<string> GetRangeFromSortedSetByHighestScore(string setId, double fromScore, double toScore);

        List<string> GetRangeFromSortedSetByHighestScore(string setId, long fromScore, long toScore);

        List<string> GetRangeFromSortedSetByHighestScore(string setId, double fromScore, double toScore, int? skip, int? take);

        List<string> GetRangeFromSortedSetByHighestScore(string setId, long fromScore, long toScore, int? skip, int? take);

        IDictionary<string, double> GetRangeWithScoresFromSortedSetByHighestScore(string setId, string fromStringScore,
            string toStringScore);

        IDictionary<string, double> GetRangeWithScoresFromSortedSetByHighestScore(string setId, string fromStringScore,
            string toStringScore, int? skip, int? take);

        IDictionary<string, double> GetRangeWithScoresFromSortedSetByHighestScore(string setId, double fromScore, double toScore);

        IDictionary<string, double> GetRangeWithScoresFromSortedSetByHighestScore(string setId, long fromScore, long toScore);

        IDictionary<string, double> GetRangeWithScoresFromSortedSetByHighestScore(string setId, double fromScore, double toScore, int? skip,
            int? take);

        IDictionary<string, double> GetRangeWithScoresFromSortedSetByHighestScore(string setId, long fromScore, long toScore, int? skip,
            int? take);

        long RemoveRangeFromSortedSet(string setId, int minRank, int maxRank);

        long RemoveRangeFromSortedSetByScore(string setId, double fromScore, double toScore);

        long RemoveRangeFromSortedSetByScore(string setId, long fromScore, long toScore);

        long GetSortedSetCount(string setId);

        long GetSortedSetCount(string setId, string fromStringScore, string toStringScore);

        long GetSortedSetCount(string setId, long fromScore, long toScore);

        long GetSortedSetCount(string setId, double fromScore, double toScore);

        double GetItemScoreInSortedSet(string setId, string value);

        long StoreIntersectFromSortedSets(string intoSetId, params string[] setIds);

        long StoreIntersectFromSortedSets(string intoSetId, string[] setIds, string[] args);

        long StoreUnionFromSortedSets(string intoSetId, params string[] setIds);

        long StoreUnionFromSortedSets(string intoSetId, string[] setIds, string[] args);

        List<string> SearchSortedSet(string setId, string start = null, string end = null, int? skip = null, int? take = null);

        long SearchSortedSetCount(string setId, string start = null, string end = null);

        long RemoveRangeFromSortedSetBySearch(string setId, string start = null, string end = null);

    }

}
