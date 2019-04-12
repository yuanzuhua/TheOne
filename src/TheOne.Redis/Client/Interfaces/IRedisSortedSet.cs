using System.Collections.Generic;

namespace TheOne.Redis.Client {

    public interface IRedisSortedSet : ICollection<string>, IHasRedisStringId {

        List<string> GetAll();
        List<string> GetRange(int startingRank, int endingRank);
        List<string> GetRangeByScore(string fromStringScore, string toStringScore);
        List<string> GetRangeByScore(string fromStringScore, string toStringScore, int? skip, int? take);
        List<string> GetRangeByScore(double fromScore, double toScore);
        List<string> GetRangeByScore(double fromScore, double toScore, int? skip, int? take);
        void RemoveRange(int fromRank, int toRank);
        void RemoveRangeByScore(double fromScore, double toScore);
        void StoreFromIntersect(params IRedisSortedSet[] ofSets);
        void StoreFromUnion(params IRedisSortedSet[] ofSets);
        long GetItemIndex(string value);
        double GetItemScore(string value);
        void IncrementItemScore(string value, double incrementByScore);
        string PopItemWithHighestScore();
        string PopItemWithLowestScore();

    }

    public interface IRedisSortedSet<T> : ICollection<T>, IHasRedisStringId {

        void Add(T item, double score);
        T PopItemWithHighestScore();
        T PopItemWithLowestScore();
        double IncrementItem(T item, double incrementBy);
        int IndexOf(T item);
        long IndexOfDescending(T item);
        List<T> GetAll();
        List<T> GetAllDescending();
        List<T> GetRange(int fromRank, int toRank);
        List<T> GetRangeByLowestScore(double fromScore, double toScore);
        List<T> GetRangeByLowestScore(double fromScore, double toScore, int? skip, int? take);
        List<T> GetRangeByHighestScore(double fromScore, double toScore);
        List<T> GetRangeByHighestScore(double fromScore, double toScore, int? skip, int? take);
        long RemoveRange(int minRank, int maxRank);
        long RemoveRangeByScore(double fromScore, double toScore);
        double GetItemScore(T item);
        long PopulateWithIntersectOf(params IRedisSortedSet<T>[] setIds);
        long PopulateWithIntersectOf(IRedisSortedSet<T>[] setIds, string[] args);
        long PopulateWithUnionOf(params IRedisSortedSet<T>[] setIds);
        long PopulateWithUnionOf(IRedisSortedSet<T>[] setIds, string[] args);

    }

}
