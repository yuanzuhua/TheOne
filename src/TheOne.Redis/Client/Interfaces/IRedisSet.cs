using System.Collections.Generic;

namespace TheOne.Redis.Client {

    public interface IRedisSet : ICollection<string>, IHasRedisStringId {

        List<string> GetRangeFromSortedSet(int startingFrom, int endingAt);
        HashSet<string> GetAll();
        string Pop();
        void Move(string value, IRedisSet toSet);
        HashSet<string> Intersect(params IRedisSet[] withSets);
        void StoreIntersect(params IRedisSet[] withSets);
        HashSet<string> Union(params IRedisSet[] withSets);
        void StoreUnion(params IRedisSet[] withSets);
        HashSet<string> Diff(IRedisSet[] withSets);
        void StoreDiff(IRedisSet fromSet, params IRedisSet[] withSets);
        string GetRandomEntry();

    }

    public interface IRedisSet<T> : ICollection<T>, IHasRedisStringId {

        List<T> Sort(int startingFrom, int endingAt);
        HashSet<T> GetAll();
        T PopRandomItem();
        T GetRandomItem();
        void MoveTo(T item, IRedisSet<T> toSet);
        void PopulateWithIntersectOf(params IRedisSet<T>[] sets);
        void PopulateWithUnionOf(params IRedisSet<T>[] sets);
        void GetDifferences(params IRedisSet<T>[] withSets);
        void PopulateWithDifferencesOf(IRedisSet<T> fromSet, params IRedisSet<T>[] withSets);

    }

}
