using System;
using System.Collections.Generic;

namespace TheOne.Redis.Client {

    public partial interface IRedisClient {

        List<string> GetAllItemsFromList(string listId);

        List<string> GetRangeFromList(string listId, int startingFrom, int endingAt);

        List<string> GetRangeFromSortedList(string listId, int startingFrom, int endingAt);

        List<string> GetSortedItemsFromList(string listId, SortOptions sortOptions);

        void AddItemToList(string listId, string value);

        void AddRangeToList(string listId, List<string> values);

        void PrependItemToList(string listId, string value);

        void PrependRangeToList(string listId, List<string> values);

        void RemoveAllFromList(string listId);

        string RemoveStartFromList(string listId);

        string BlockingRemoveStartFromList(string listId, TimeSpan? timeout);

        ItemRef BlockingRemoveStartFromLists(string[] listIds, TimeSpan? timeout);

        string RemoveEndFromList(string listId);

        void TrimList(string listId, int keepStartingFrom, int keepEndingAt);

        long RemoveItemFromList(string listId, string value);

        long RemoveItemFromList(string listId, string value, int noOfMatches);

        long GetListCount(string listId);

        string GetItemFromList(string listId, int listIndex);

        void SetItemInList(string listId, int listIndex, string value);

        void EnqueueItemOnList(string listId, string value);

        string DequeueItemFromList(string listId);

        string BlockingDequeueItemFromList(string listId, TimeSpan? timeout);

        ItemRef BlockingDequeueItemFromLists(string[] listIds, TimeSpan? timeout);

        void PushItemToList(string listId, string value);

        string PopItemFromList(string listId);

        string BlockingPopItemFromList(string listId, TimeSpan? timeout);

        ItemRef BlockingPopItemFromLists(string[] listIds, TimeSpan? timeout);

        string PopAndPushItemBetweenLists(string fromListId, string toListId);

        string BlockingPopAndPushItemBetweenLists(string fromListId, string toListId, TimeSpan? timeout);

    }

}
