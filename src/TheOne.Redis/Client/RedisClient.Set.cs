using System;
using System.Collections.Generic;
using System.Linq;
using TheOne.Redis.Client.Internal;
using TheOne.Redis.Common;
using TheOne.Redis.Pipeline;

namespace TheOne.Redis.Client {

    public partial class RedisClient {

        public IHasNamed<IRedisSet> Sets { get; set; }

        public List<string> GetSortedEntryValues(string setId, int startingFrom, int endingAt) {
            var sortOptions = new SortOptions { Skip = startingFrom, Take = endingAt };
            byte[][] multiDataList = this.Sort(setId, sortOptions);
            return multiDataList.ToStringList();
        }

        public long AddGeoMember(string key, double longitude, double latitude, string member) {
            return this.GeoAdd(key, longitude, latitude, member);
        }

        public long AddGeoMembers(string key, params RedisGeo[] geoPoints) {
            return this.GeoAdd(key, geoPoints);
        }

        public double CalculateDistanceBetweenGeoMembers(string key, string fromMember, string toMember, string unit = null) {
            return this.GeoDist(key, fromMember, toMember, unit);
        }

        public string[] GetGeohashes(string key, params string[] members) {
            return this.GeoHash(key, members);
        }

        public List<RedisGeo> GetGeoCoordinates(string key, params string[] members) {
            return this.GeoPos(key, members);
        }

        public string[] FindGeoMembersInRadius(string key, double longitude, double latitude, double radius, string unit) {
            List<RedisGeoResult> results = this.GeoRadius(key, longitude, latitude, radius, unit);
            var to = new string[results.Count];
            for (var i = 0; i < results.Count; i++) {
                to[i] = results[i].Member;
            }

            return to;
        }

        public List<RedisGeoResult> FindGeoResultsInRadius(string key, double longitude, double latitude, double radius, string unit,
            int? count = null, bool? sortByNearest = null) {
            return this.GeoRadius(key, longitude, latitude, radius, unit, true, true, true, count, sortByNearest);
        }

        public string[] FindGeoMembersInRadius(string key, string member, double radius, string unit) {
            List<RedisGeoResult> results = this.GeoRadiusByMember(key, member, radius, unit);
            var to = new string[results.Count];
            for (var i = 0; i < results.Count; i++) {
                to[i] = results[i].Member;
            }

            return to;
        }

        public List<RedisGeoResult> FindGeoResultsInRadius(string key, string member, double radius, string unit, int? count = null,
            bool? sortByNearest = null) {
            return this.GeoRadiusByMember(key, member, radius, unit, true, true, true, count, sortByNearest);
        }

        public HashSet<string> GetAllItemsFromSet(string setId) {
            byte[][] multiDataList = this.SMembers(setId);
            return CreateHashSet(multiDataList);
        }

        public void AddItemToSet(string setId, string item) {
            this.SAdd(setId, item.ToUtf8Bytes());
        }

        public void AddRangeToSet(string setId, List<string> items) {
            if (string.IsNullOrEmpty(setId)) {
                throw new ArgumentNullException(nameof(setId));
            }

            if (items == null) {
                throw new ArgumentNullException(nameof(items));
            }

            if (items.Count == 0) {
                return;
            }

            if (this.Transaction != null || this.Pipeline != null) {
                IRedisQueueableOperation queueable = this.Transaction as IRedisQueueableOperation
                                                     ?? this.Pipeline as IRedisQueueableOperation;

                if (queueable == null) {
                    throw new NotSupportedException("Cannot AddRangeToSet() when Transaction is: " +
                                                    (this.Transaction?.GetType().Name ?? this.Pipeline?.GetType().Name));
                }

                // Complete the first QueuedCommand()
                this.AddItemToSet(setId, items[0]);

                // Add subsequent queued commands
                for (var i = 1; i < items.Count; i++) {
                    var item = items[i];
                    queueable.QueueCommand(c => c.AddItemToSet(setId, item));
                }
            } else {
                byte[] uSetId = setId.ToUtf8Bytes();
                RedisPipelineCommand pipeline = this.CreatePipelineCommand();
                foreach (var item in items) {
                    pipeline.WriteCommand(Commands.SAdd, uSetId, item.ToUtf8Bytes());
                }

                pipeline.Flush();

                // the number of items after 
                /*List<long> intResults = */
                pipeline.ReadAllAsInts();
            }
        }

        public void RemoveItemFromSet(string setId, string item) {
            this.SRem(setId, item.ToUtf8Bytes());
        }

        public string PopItemFromSet(string setId) {
            return this.SPop(setId).FromUtf8Bytes();
        }

        public List<string> PopItemsFromSet(string setId, int count) {
            return this.SPop(setId, count).ToStringList();
        }

        public void MoveBetweenSets(string fromSetId, string toSetId, string item) {
            this.SMove(fromSetId, toSetId, item.ToUtf8Bytes());
        }

        public long GetSetCount(string setId) {
            return this.SCard(setId);
        }

        public bool SetContainsItem(string setId, string item) {
            return this.SIsMember(setId, item.ToUtf8Bytes()) == 1;
        }

        public HashSet<string> GetIntersectFromSets(params string[] setIds) {
            if (setIds.Length == 0) {
                return new HashSet<string>();
            }

            byte[][] multiDataList = this.SInter(setIds);
            return CreateHashSet(multiDataList);
        }

        public void StoreIntersectFromSets(string intoSetId, params string[] setIds) {
            if (setIds.Length == 0) {
                return;
            }

            this.SInterStore(intoSetId, setIds);
        }

        public HashSet<string> GetUnionFromSets(params string[] setIds) {
            if (setIds.Length == 0) {
                return new HashSet<string>();
            }

            byte[][] multiDataList = this.SUnion(setIds);
            return CreateHashSet(multiDataList);
        }

        public void StoreUnionFromSets(string intoSetId, params string[] setIds) {
            if (setIds.Length == 0) {
                return;
            }

            this.SUnionStore(intoSetId, setIds);
        }

        public HashSet<string> GetDifferencesFromSet(string fromSetId, params string[] withSetIds) {
            if (withSetIds.Length == 0) {
                return new HashSet<string>();
            }

            byte[][] multiDataList = this.SDiff(fromSetId, withSetIds);
            return CreateHashSet(multiDataList);
        }

        public void StoreDifferencesFromSet(string intoSetId, string fromSetId, params string[] withSetIds) {
            if (withSetIds.Length == 0) {
                return;
            }

            this.SDiffStore(intoSetId, fromSetId, withSetIds);
        }

        public string GetRandomItemFromSet(string setId) {
            return this.SRandMember(setId).FromUtf8Bytes();
        }

        public IEnumerable<string> GetKeysByPattern(string pattern) {
            return this.ScanAllKeys(pattern);
        }

        private static HashSet<string> CreateHashSet(byte[][] multiDataList) {
            var results = new HashSet<string>();
            foreach (byte[] multiData in multiDataList) {
                results.Add(multiData.FromUtf8Bytes());
            }

            return results;
        }

        internal class RedisClientSets : IHasNamed<IRedisSet> {

            private readonly RedisClient _client;

            public RedisClientSets(RedisClient client) {
                this._client = client;
            }

            public IRedisSet this[string setId] {
                get => new RedisClientSet(this._client, setId);
                set {
                    IRedisSet col = this[setId];
                    col.Clear();
                    col.CopyTo(value.ToArray(), 0);
                }
            }

        }

    }

}
