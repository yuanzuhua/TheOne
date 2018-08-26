using System;
using System.Collections.Generic;

namespace TheOne.Redis.Client {

    public interface IRedisList : IList<string>, IHasRedisStringId {

        List<string> GetAll();
        List<string> GetRange(int startingFrom, int endingAt);
        List<string> GetRangeFromSortedList(int startingFrom, int endingAt);
        void RemoveAll();
        void Trim(int keepStartingFrom, int keepEndingAt);
        long RemoveValue(string value);
        long RemoveValue(string value, int noOfMatches);

        void Prepend(string value);
        void Append(string value);
        string RemoveStart();
        string BlockingRemoveStart(TimeSpan? timeout);
        string RemoveEnd();

        void Enqueue(string value);
        string Dequeue();
        string BlockingDequeue(TimeSpan? timeout);

        void Push(string value);
        string Pop();
        string BlockingPop(TimeSpan? timeout);
        string PopAndPush(IRedisList toList);

    }

    /// <summary>
    ///     Wrap the common redis list operations under a IList[string] interface.
    /// </summary>
    public interface IRedisList<T> : IList<T>, IHasRedisStringId {

        List<T> GetAll();
        List<T> GetRange(int startingFrom, int endingAt);
        List<T> GetRangeFromSortedList(int startingFrom, int endingAt);
        void RemoveAll();
        void Trim(int keepStartingFrom, int keepEndingAt);
        long RemoveValue(T value);
        long RemoveValue(T value, int noOfMatches);

        void AddRange(IEnumerable<T> values);
        void Append(T value);
        void Prepend(T value);
        T RemoveStart();
        T BlockingRemoveStart(TimeSpan? timeout);
        T RemoveEnd();

        void Enqueue(T value);
        T Dequeue();
        T BlockingDequeue(TimeSpan? timeout);

        void Push(T value);
        T Pop();
        T BlockingPop(TimeSpan? timeout);
        T PopAndPush(IRedisList<T> toList);

    }

}
