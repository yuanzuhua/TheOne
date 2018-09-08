using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using TheOne.Redis.Client.Internal;
using TheOne.Redis.Common;
using TheOne.Redis.Pipeline;

namespace TheOne.Redis.Client {

    public partial class RedisClient {

        /// <inheritdoc />
        public IHasNamed<IRedisSortedSet> SortedSets { get; set; }

        /// <inheritdoc />
        public bool AddItemToSortedSet(string setId, string value) {
            return this.AddItemToSortedSet(setId, value, GetLexicalScore(value));
        }

        /// <inheritdoc />
        public bool AddItemToSortedSet(string setId, string value, double score) {
            return this.ZAdd(setId, score, value.ToUtf8Bytes()) == Success;
        }

        /// <inheritdoc />
        public bool AddRangeToSortedSet(string setId, List<string> values, double score) {
            RedisPipelineCommand pipeline = this.CreatePipelineCommand();
            byte[] uSetId = setId.ToUtf8Bytes();
            byte[] uScore = score.ToFastUtf8Bytes();

            foreach (var value in values) {
                pipeline.WriteCommand(Commands.ZAdd, uSetId, uScore, value.ToUtf8Bytes());
            }

            pipeline.Flush();

            var success = pipeline.ReadAllAsIntsHaveSuccess();
            return success;
        }

        /// <inheritdoc />
        public bool AddRangeToSortedSet(string setId, List<string> values, long score) {
            RedisPipelineCommand pipeline = this.CreatePipelineCommand();
            byte[] uSetId = setId.ToUtf8Bytes();
            byte[] uScore = score.ToUtf8Bytes();

            foreach (var value in values) {
                pipeline.WriteCommand(Commands.ZAdd, uSetId, uScore, value.ToUtf8Bytes());
            }

            pipeline.Flush();

            var success = pipeline.ReadAllAsIntsHaveSuccess();
            return success;
        }

        /// <inheritdoc />
        public bool RemoveItemFromSortedSet(string setId, string value) {
            return this.ZRem(setId, value.ToUtf8Bytes()) == Success;
        }

        /// <inheritdoc />
        public long RemoveItemsFromSortedSet(string setId, List<string> values) {
            return this.ZRem(setId, values.Select(x => x.ToUtf8Bytes()).ToArray());
        }

        /// <inheritdoc />
        public string PopItemWithLowestScoreFromSortedSet(string setId) {
            // TODO this should be atomic
            byte[][] topScoreItemBytes = this.ZRange(setId, _firstElement, 1);
            if (topScoreItemBytes.Length == 0) {
                return null;
            }

            this.ZRem(setId, topScoreItemBytes[0]);
            return topScoreItemBytes[0].FromUtf8Bytes();
        }

        /// <inheritdoc />
        public string PopItemWithHighestScoreFromSortedSet(string setId) {
            // TODO this should be atomic
            byte[][] topScoreItemBytes = this.ZRevRange(setId, _firstElement, 1);
            if (topScoreItemBytes.Length == 0) {
                return null;
            }

            this.ZRem(setId, topScoreItemBytes[0]);
            return topScoreItemBytes[0].FromUtf8Bytes();
        }

        /// <inheritdoc />
        public bool SortedSetContainsItem(string setId, string value) {
            return this.ZRank(setId, value.ToUtf8Bytes()) != -1;
        }

        /// <inheritdoc />
        public double IncrementItemInSortedSet(string setId, string value, double incrementBy) {
            return this.ZIncrBy(setId, incrementBy, value.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public double IncrementItemInSortedSet(string setId, string value, long incrementBy) {
            return this.ZIncrBy(setId, incrementBy, value.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public long GetItemIndexInSortedSet(string setId, string value) {
            return this.ZRank(setId, value.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public long GetItemIndexInSortedSetDesc(string setId, string value) {
            return this.ZRevRank(setId, value.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public List<string> GetAllItemsFromSortedSet(string setId) {
            byte[][] multiDataList = this.ZRange(setId, _firstElement, _lastElement);
            return multiDataList.ToStringList();
        }

        /// <inheritdoc />
        public List<string> GetAllItemsFromSortedSetDesc(string setId) {
            byte[][] multiDataList = this.ZRevRange(setId, _firstElement, _lastElement);
            return multiDataList.ToStringList();
        }

        /// <inheritdoc />
        public List<string> GetRangeFromSortedSet(string setId, int fromRank, int toRank) {
            byte[][] multiDataList = this.ZRange(setId, fromRank, toRank);
            return multiDataList.ToStringList();
        }

        /// <inheritdoc />
        public List<string> GetRangeFromSortedSetDesc(string setId, int fromRank, int toRank) {
            byte[][] multiDataList = this.ZRevRange(setId, fromRank, toRank);
            return multiDataList.ToStringList();
        }

        /// <inheritdoc />
        public IDictionary<string, double> GetAllWithScoresFromSortedSet(string setId) {
            byte[][] multiDataList = this.ZRangeWithScores(setId, _firstElement, _lastElement);
            return CreateSortedScoreMap(multiDataList);
        }

        /// <inheritdoc />
        public IDictionary<string, double> GetRangeWithScoresFromSortedSet(string setId, int fromRank, int toRank) {
            byte[][] multiDataList = this.ZRangeWithScores(setId, fromRank, toRank);
            return CreateSortedScoreMap(multiDataList);
        }

        /// <inheritdoc />
        public IDictionary<string, double> GetRangeWithScoresFromSortedSetDesc(string setId, int fromRank, int toRank) {
            byte[][] multiDataList = this.ZRevRangeWithScores(setId, fromRank, toRank);
            return CreateSortedScoreMap(multiDataList);
        }

        /// <inheritdoc />
        public List<string> GetRangeFromSortedSetByLowestScore(string setId, string fromStringScore, string toStringScore) {
            return this.GetRangeFromSortedSetByLowestScore(setId, fromStringScore, toStringScore, null, null);
        }

        /// <inheritdoc />
        public List<string> GetRangeFromSortedSetByLowestScore(string setId, string fromStringScore, string toStringScore, int? skip,
            int? take) {
            var fromScore = GetLexicalScore(fromStringScore);
            var toScore = GetLexicalScore(toStringScore);
            return this.GetRangeFromSortedSetByLowestScore(setId, fromScore, toScore, skip, take);
        }

        /// <inheritdoc />
        public List<string> GetRangeFromSortedSetByLowestScore(string setId, double fromScore, double toScore) {
            return this.GetRangeFromSortedSetByLowestScore(setId, fromScore, toScore, null, null);
        }

        /// <inheritdoc />
        public List<string> GetRangeFromSortedSetByLowestScore(string setId, long fromScore, long toScore) {
            return this.GetRangeFromSortedSetByLowestScore(setId, fromScore, toScore, null, null);
        }

        /// <inheritdoc />
        public List<string> GetRangeFromSortedSetByLowestScore(string setId, double fromScore, double toScore, int? skip, int? take) {
            byte[][] multiDataList = this.ZRangeByScore(setId, fromScore, toScore, skip, take);
            return multiDataList.ToStringList();
        }

        /// <inheritdoc />
        public List<string> GetRangeFromSortedSetByLowestScore(string setId, long fromScore, long toScore, int? skip, int? take) {
            byte[][] multiDataList = this.ZRangeByScore(setId, fromScore, toScore, skip, take);
            return multiDataList.ToStringList();
        }

        /// <inheritdoc />
        public IDictionary<string, double> GetRangeWithScoresFromSortedSetByLowestScore(string setId, string fromStringScore,
            string toStringScore) {
            return this.GetRangeWithScoresFromSortedSetByLowestScore(setId, fromStringScore, toStringScore, null, null);
        }

        /// <inheritdoc />
        public IDictionary<string, double> GetRangeWithScoresFromSortedSetByLowestScore(string setId, string fromStringScore,
            string toStringScore, int? skip, int? take) {
            var fromScore = GetLexicalScore(fromStringScore);
            var toScore = GetLexicalScore(toStringScore);
            return this.GetRangeWithScoresFromSortedSetByLowestScore(setId, fromScore, toScore, skip, take);
        }

        /// <inheritdoc />
        public IDictionary<string, double> GetRangeWithScoresFromSortedSetByLowestScore(string setId, double fromScore, double toScore) {
            return this.GetRangeWithScoresFromSortedSetByLowestScore(setId, fromScore, toScore, null, null);
        }

        /// <inheritdoc />
        public IDictionary<string, double> GetRangeWithScoresFromSortedSetByLowestScore(string setId, long fromScore, long toScore) {
            return this.GetRangeWithScoresFromSortedSetByLowestScore(setId, fromScore, toScore, null, null);
        }

        /// <inheritdoc />
        public IDictionary<string, double> GetRangeWithScoresFromSortedSetByLowestScore(string setId, double fromScore, double toScore,
            int? skip, int? take) {
            byte[][] multiDataList = this.ZRangeByScoreWithScores(setId, fromScore, toScore, skip, take);
            return CreateSortedScoreMap(multiDataList);
        }

        /// <inheritdoc />
        public IDictionary<string, double> GetRangeWithScoresFromSortedSetByLowestScore(string setId, long fromScore, long toScore,
            int? skip, int? take) {
            byte[][] multiDataList = this.ZRangeByScoreWithScores(setId, fromScore, toScore, skip, take);
            return CreateSortedScoreMap(multiDataList);
        }

        /// <inheritdoc />
        public List<string> GetRangeFromSortedSetByHighestScore(string setId, string fromStringScore, string toStringScore) {
            return this.GetRangeFromSortedSetByHighestScore(setId, fromStringScore, toStringScore, null, null);
        }

        /// <inheritdoc />
        public List<string> GetRangeFromSortedSetByHighestScore(string setId, string fromStringScore, string toStringScore, int? skip,
            int? take) {
            var fromScore = GetLexicalScore(fromStringScore);
            var toScore = GetLexicalScore(toStringScore);
            return this.GetRangeFromSortedSetByHighestScore(setId, fromScore, toScore, skip, take);
        }

        /// <inheritdoc />
        public List<string> GetRangeFromSortedSetByHighestScore(string setId, double fromScore, double toScore) {
            return this.GetRangeFromSortedSetByHighestScore(setId, fromScore, toScore, null, null);
        }

        /// <inheritdoc />
        public List<string> GetRangeFromSortedSetByHighestScore(string setId, long fromScore, long toScore) {
            return this.GetRangeFromSortedSetByHighestScore(setId, fromScore, toScore, null, null);
        }

        /// <inheritdoc />
        public List<string> GetRangeFromSortedSetByHighestScore(string setId, double fromScore, double toScore, int? skip, int? take) {
            byte[][] multiDataList = this.ZRevRangeByScore(setId, fromScore, toScore, skip, take);
            return multiDataList.ToStringList();
        }

        /// <inheritdoc />
        public List<string> GetRangeFromSortedSetByHighestScore(string setId, long fromScore, long toScore, int? skip, int? take) {
            byte[][] multiDataList = this.ZRevRangeByScore(setId, fromScore, toScore, skip, take);
            return multiDataList.ToStringList();
        }

        /// <inheritdoc />
        public IDictionary<string, double> GetRangeWithScoresFromSortedSetByHighestScore(string setId, string fromStringScore,
            string toStringScore) {
            return this.GetRangeWithScoresFromSortedSetByHighestScore(setId, fromStringScore, toStringScore, null, null);
        }

        /// <inheritdoc />
        public IDictionary<string, double> GetRangeWithScoresFromSortedSetByHighestScore(string setId, string fromStringScore,
            string toStringScore, int? skip, int? take) {
            var fromScore = GetLexicalScore(fromStringScore);
            var toScore = GetLexicalScore(toStringScore);
            return this.GetRangeWithScoresFromSortedSetByHighestScore(setId, fromScore, toScore, skip, take);
        }

        /// <inheritdoc />
        public IDictionary<string, double> GetRangeWithScoresFromSortedSetByHighestScore(string setId, double fromScore, double toScore) {
            return this.GetRangeWithScoresFromSortedSetByHighestScore(setId, fromScore, toScore, null, null);
        }

        /// <inheritdoc />
        public IDictionary<string, double> GetRangeWithScoresFromSortedSetByHighestScore(string setId, long fromScore, long toScore) {
            return this.GetRangeWithScoresFromSortedSetByHighestScore(setId, fromScore, toScore, null, null);
        }

        /// <inheritdoc />
        public IDictionary<string, double> GetRangeWithScoresFromSortedSetByHighestScore(string setId, double fromScore, double toScore,
            int? skip, int? take) {
            byte[][] multiDataList = this.ZRevRangeByScoreWithScores(setId, fromScore, toScore, skip, take);
            return CreateSortedScoreMap(multiDataList);
        }

        /// <inheritdoc />
        public IDictionary<string, double> GetRangeWithScoresFromSortedSetByHighestScore(string setId, long fromScore, long toScore,
            int? skip, int? take) {
            byte[][] multiDataList = this.ZRevRangeByScoreWithScores(setId, fromScore, toScore, skip, take);
            return CreateSortedScoreMap(multiDataList);
        }

        /// <inheritdoc />
        public long RemoveRangeFromSortedSet(string setId, int minRank, int maxRank) {
            return this.ZRemRangeByRank(setId, minRank, maxRank);
        }

        /// <inheritdoc />
        public long RemoveRangeFromSortedSetByScore(string setId, double fromScore, double toScore) {
            return this.ZRemRangeByScore(setId, fromScore, toScore);
        }

        /// <inheritdoc />
        public long RemoveRangeFromSortedSetByScore(string setId, long fromScore, long toScore) {
            return this.ZRemRangeByScore(setId, fromScore, toScore);
        }

        /// <inheritdoc />
        public long GetSortedSetCount(string setId) {
            return this.ZCard(setId);
        }

        /// <inheritdoc />
        public long GetSortedSetCount(string setId, string fromStringScore, string toStringScore) {
            var fromScore = GetLexicalScore(fromStringScore);
            var toScore = GetLexicalScore(toStringScore);
            return this.GetSortedSetCount(setId, fromScore, toScore);
        }

        /// <inheritdoc />
        public long GetSortedSetCount(string setId, double fromScore, double toScore) {
            return this.ZCount(setId, fromScore, toScore);
        }

        /// <inheritdoc />
        public long GetSortedSetCount(string setId, long fromScore, long toScore) {
            return this.ZCount(setId, fromScore, toScore);
        }

        /// <inheritdoc />
        public double GetItemScoreInSortedSet(string setId, string value) {
            return this.ZScore(setId, value.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public long StoreIntersectFromSortedSets(string intoSetId, params string[] setIds) {
            return this.ZInterStore(intoSetId, setIds);
        }

        /// <inheritdoc />
        public long StoreIntersectFromSortedSets(string intoSetId, string[] setIds, string[] args) {
            return this.ZInterStore(intoSetId, setIds, args);
        }

        /// <inheritdoc />
        public long StoreUnionFromSortedSets(string intoSetId, params string[] setIds) {
            return this.ZUnionStore(intoSetId, setIds);
        }

        /// <inheritdoc />
        public long StoreUnionFromSortedSets(string intoSetId, string[] setIds, string[] args) {
            return this.ZUnionStore(intoSetId, setIds, args);
        }

        /// <inheritdoc />
        public List<string> SearchSortedSet(string setId, string start = null, string end = null, int? skip = null, int? take = null) {
            start = GetSearchStart(start);
            end = GetSearchEnd(end);

            byte[][] ret = this.ZRangeByLex(setId, start, end, skip, take);
            return ret.ToStringList();
        }

        /// <inheritdoc />
        public long SearchSortedSetCount(string setId, string start = null, string end = null) {
            return this.ZLexCount(setId, GetSearchStart(start), GetSearchEnd(end));
        }

        /// <inheritdoc />
        public long RemoveRangeFromSortedSetBySearch(string setId, string start = null, string end = null) {
            return this.ZRemRangeByLex(setId, GetSearchStart(start), GetSearchEnd(end));
        }

        public static double GetLexicalScore(string value) {
            if (string.IsNullOrEmpty(value)) {
                return 0;
            }

            var lexicalValue = 0;
            if (value.Length >= 1) {
                lexicalValue += value[0] * (int)Math.Pow(256, 3);
            }

            if (value.Length >= 2) {
                lexicalValue += value[1] * (int)Math.Pow(256, 2);
            }

            if (value.Length >= 3) {
                lexicalValue += value[2] * (int)Math.Pow(256, 1);
            }

            if (value.Length >= 4) {
                lexicalValue += value[3];
            }

            return lexicalValue;
        }

        public bool AddItemToSortedSet(string setId, string value, long score) {
            return this.ZAdd(setId, score, value.ToUtf8Bytes()) == Success;
        }

        private static IDictionary<string, double> CreateSortedScoreMap(byte[][] multiDataList) {
            var map = new OrderedDictionary<string, double>();

            for (var i = 0; i < multiDataList.Length; i += 2) {
                var key = multiDataList[i].FromUtf8Bytes();
                double.TryParse(multiDataList[i + 1].FromUtf8Bytes(), NumberStyles.Any, CultureInfo.InvariantCulture, out var value);
                map[key] = value;
            }

            return map;
        }

        private static string GetSearchStart(string start) {
            return start == null
                ? "-"
                : start.IndexOfAny("[", "(", "-") != 0
                    ? "[" + start
                    : start;
        }

        private static string GetSearchEnd(string end) {
            return end == null
                ? "+"
                : end.IndexOfAny("[", "(", "+") != 0
                    ? "[" + end
                    : end;
        }

        /// <inheritdoc />
        internal class RedisClientSortedSets : IHasNamed<IRedisSortedSet> {

            private readonly RedisClient _client;

            public RedisClientSortedSets(RedisClient client) {
                this._client = client;
            }

            /// <inheritdoc />
            public IRedisSortedSet this[string setId] {
                get => new RedisClientSortedSet(this._client, setId);
                set {
                    IRedisSortedSet col = this[setId];
                    col.Clear();
                    col.CopyTo(value.ToArray(), 0);
                }
            }

        }

    }

}
