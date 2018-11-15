using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Common;
using TheOne.Redis.Tests.Extensions;
#if NET46
using System.Threading;

#endif


namespace TheOne.Redis.Tests.Client {

    [TestFixture]
    internal sealed class RedisClientSortedSetTests : RedisClientTestsBase {

        #region Models

        public class Article {

            public int Id { get; set; }
            public string Title { get; set; }
            public DateTime ModifiedDate { get; set; }

        }

        #endregion

        private const string _setIdSuffix = "testzset";
        private List<string> _storeMembers;
        private Dictionary<string, double> _stringDoubleMap;
        private string SetId => this.PrefixedKey(_setIdSuffix);

        public override void SetUp() {
            base.SetUp();
            this.Redis.NamespacePrefix = "RedisClientSortedSetTests";
            this._storeMembers = new List<string> { "one", "two", "three", "four" };
            this._stringDoubleMap = new Dictionary<string, double> {
                { "one", 1 },
                { "two", 2 },
                { "three", 3 },
                { "four", 4 }
            };
        }

        [Test]
        public void AddToSet_with_same_score_is_still_returned_in_lexical_order_score() {
            this._storeMembers.ForEach(x => this.Redis.AddItemToSortedSet(this.SetId, x, 1));

            List<string> members = this.Redis.GetAllItemsFromSortedSet(this.SetId);

            this._storeMembers.Sort((x, y) => string.Compare(x, y, StringComparison.Ordinal));
            Assert.That(members.EquivalentTo(this._storeMembers));
        }

        [Test]
        public void AddToSet_without_score_adds_an_implicit_lexical_order_score() {
            this._storeMembers.ForEach(x => this.Redis.AddItemToSortedSet(this.SetId, x));

            List<string> members = this.Redis.GetAllItemsFromSortedSet(this.SetId);

            this._storeMembers.Sort((x, y) => string.Compare(x, y, StringComparison.Ordinal));
            Assert.That(members.EquivalentTo(this._storeMembers), Is.True);
        }

        [Test]
        public void Can_add_large_score_to_SortedSet() {
            this.Redis.AddItemToSortedSet(this.SetId, "value", 12345678901234567890d);
            var score = this.Redis.GetItemScoreInSortedSet(this.SetId, "value");

            Assert.That(score, Is.EqualTo(12345678901234567890d));
        }

        [Test]
        public void Can_Add_to_ICollection_Set() {
            IRedisSortedSet list = this.Redis.SortedSets[this.SetId];
            this._storeMembers.ForEach(list.Add);

            List<string> members = list.ToList();
            Assert.That(members, Is.EquivalentTo(this._storeMembers));
        }

        [Test]
        public void Can_AddItemToSortedSet_and_GetAllFromSet() {
            var i = 0;
            this._storeMembers.ForEach(x => this.Redis.AddItemToSortedSet(this.SetId, x, i++));

            List<string> members = this.Redis.GetAllItemsFromSortedSet(this.SetId);
            Assert.That(members.EquivalentTo(this._storeMembers), Is.True);
        }

        [Test]
        public void Can_AddRangeToSortedSet_and_GetAllFromSet() {
            var success = this.Redis.AddRangeToSortedSet(this.SetId, this._storeMembers, 1);
            Assert.That(success, Is.True);

            List<string> members = this.Redis.GetAllItemsFromSortedSet(this.SetId);
            Assert.That(members, Is.EquivalentTo(this._storeMembers));
        }

        [Test]
        public void Can_Clear_ICollection_Set() {
            IRedisSortedSet list = this.Redis.SortedSets[this.SetId];
            this._storeMembers.ForEach(list.Add);

            Assert.That(list.Count, Is.EqualTo(this._storeMembers.Count));

            list.Clear();

            Assert.That(list.Count, Is.EqualTo(0));
        }

        [Test]
        public void Can_enumerate_large_ICollection_Set() {

            const int setSize = 2500;

            this._storeMembers = new List<string>();
            for (var i = 0; i < setSize; i++) {
                this.Redis.AddItemToSortedSet(this.SetId, i.ToString());
                this._storeMembers.Add(i.ToString());
            }

            var members = new List<string>();
            foreach (var item in this.Redis.SortedSets[this.SetId]) {
                members.Add(item);
            }

            members.Sort((x, y) => int.Parse(x).CompareTo(int.Parse(y)));
            Assert.That(members.Count, Is.EqualTo(this._storeMembers.Count));
            Assert.That(members, Is.EquivalentTo(this._storeMembers));
        }

        [Test]
        public void Can_enumerate_small_ICollection_Set() {
            this._storeMembers.ForEach(x => this.Redis.AddItemToSortedSet(this.SetId, x));

            var members = new List<string>();
            foreach (var item in this.Redis.SortedSets[this.SetId]) {
                members.Add(item);
            }

            members.Sort();
            Assert.That(members.Count, Is.EqualTo(this._storeMembers.Count));
            Assert.That(members, Is.EquivalentTo(this._storeMembers));
        }

        [Test]
        public void Can_get_index_and_score_from_SortedSet() {
            this._storeMembers = new List<string> { "a", "b", "c", "d" };
            const double initialScore = 10d;
            var i = initialScore;
            this._storeMembers.ForEach(x => this.Redis.AddItemToSortedSet(this.SetId, x, i++));

            Assert.That(this.Redis.GetItemIndexInSortedSet(this.SetId, "a"), Is.EqualTo(0));
            Assert.That(this.Redis.GetItemIndexInSortedSetDesc(this.SetId, "a"), Is.EqualTo(this._storeMembers.Count - 1));

            Assert.That(this.Redis.GetItemScoreInSortedSet(this.SetId, "a"), Is.EqualTo(initialScore));
            Assert.That(this.Redis.GetItemScoreInSortedSet(this.SetId, "d"), Is.EqualTo(initialScore + this._storeMembers.Count - 1));
        }

        [Test]
        public void Can_GetItemIndexInSortedSet_in_Asc_and_Desc() {
            var i = 10;
            this._storeMembers.ForEach(x => this.Redis.AddItemToSortedSet(this.SetId, x, i++));

            Assert.That(this.Redis.GetItemIndexInSortedSet(this.SetId, "one"), Is.EqualTo(0));
            Assert.That(this.Redis.GetItemIndexInSortedSet(this.SetId, "two"), Is.EqualTo(1));
            Assert.That(this.Redis.GetItemIndexInSortedSet(this.SetId, "three"), Is.EqualTo(2));
            Assert.That(this.Redis.GetItemIndexInSortedSet(this.SetId, "four"), Is.EqualTo(3));

            Assert.That(this.Redis.GetItemIndexInSortedSetDesc(this.SetId, "one"), Is.EqualTo(3));
            Assert.That(this.Redis.GetItemIndexInSortedSetDesc(this.SetId, "two"), Is.EqualTo(2));
            Assert.That(this.Redis.GetItemIndexInSortedSetDesc(this.SetId, "three"), Is.EqualTo(1));
            Assert.That(this.Redis.GetItemIndexInSortedSetDesc(this.SetId, "four"), Is.EqualTo(0));
        }

        [Test]
        public void Can_GetRangeFromSortedSetByHighestScore_from_sorted_set() {
            this._storeMembers.ForEach(x => this.Redis.AddItemToSortedSet(this.SetId, x));

            this._storeMembers.Sort((x, y) => string.Compare(y, x, StringComparison.Ordinal));
            List<string> memberRage = this._storeMembers.Where(x =>
                string.Compare(x, "four", StringComparison.Ordinal) >= 0 &&
                string.Compare(x, "three", StringComparison.Ordinal) <= 0).ToList();

            List<string> range = this.Redis.GetRangeFromSortedSetByHighestScore(this.SetId, "four", "three");
            Assert.That(range.EquivalentTo(memberRage));
        }

        [Test]
        public void Can_GetRangeFromSortedSetByLowestScore_from_sorted_set() {
            this._storeMembers.ForEach(x => this.Redis.AddItemToSortedSet(this.SetId, x));

            this._storeMembers.Sort((x, y) => string.Compare(x, y, StringComparison.Ordinal));
            List<string> memberRage = this._storeMembers.Where(x =>
                                              string.Compare(x, "four", StringComparison.Ordinal) >= 0 &&
                                              string.Compare(x, "three", StringComparison.Ordinal) <= 0)
                                          .ToList();

            List<string> range = this.Redis.GetRangeFromSortedSetByLowestScore(this.SetId, "four", "three");
            Assert.That(range.EquivalentTo(memberRage));
        }

        [Test]
        public void Can_GetSetCount() {
            this._storeMembers.ForEach(x => this.Redis.AddItemToSortedSet(this.SetId, x));

            var setCount = this.Redis.GetSortedSetCount(this.SetId);

            Assert.That(setCount, Is.EqualTo(this._storeMembers.Count));
        }

        [Test]
        public void Can_GetSetCountByScores() {
            var scores = new List<double>();

            this._storeMembers.ForEach(x => {
                this.Redis.AddItemToSortedSet(this.SetId, x);
                scores.Add(RedisClient.GetLexicalScore(x));
            });

            Assert.That(this.Redis.GetSortedSetCount(this.SetId, scores.Min(), scores.Max()), Is.EqualTo(this._storeMembers.Count()));
            Assert.That(this.Redis.GetSortedSetCount(this.SetId, scores.Min(), scores.Min()), Is.EqualTo(1));
        }

        [Test]
        public void Can_IncrementItemInSortedSet() {
            foreach (KeyValuePair<string, double> value in this._stringDoubleMap) {
                this.Redis.AddItemToSortedSet(this.SetId, value.Key, value.Value);
            }

            var currentScore = this.Redis.IncrementItemInSortedSet(this.SetId, "one", 3);
            this._stringDoubleMap["one"] = this._stringDoubleMap["one"] + 3;
            Assert.That(currentScore, Is.EqualTo(this._stringDoubleMap["one"]));

            currentScore = this.Redis.IncrementItemInSortedSet(this.SetId, "four", -3);
            this._stringDoubleMap["four"] = this._stringDoubleMap["four"] - 3;
            Assert.That(currentScore, Is.EqualTo(this._stringDoubleMap["four"]));

            IDictionary<string, double> map = this.Redis.GetAllWithScoresFromSortedSet(this.SetId);

            bool UnorderedEquivalentTo<TKey, TValue>(IDictionary<TKey, TValue> thisMap, IDictionary<TKey, TValue> otherMap) {
                if (thisMap == null || otherMap == null) {
                    return thisMap == otherMap;
                }

                if (thisMap.Count != otherMap.Count) {
                    return false;
                }

                foreach (KeyValuePair<TKey, TValue> entry in thisMap) {
                    if (!otherMap.TryGetValue(entry.Key, out TValue otherValue)) {
                        return false;
                    }

                    if (!Equals(entry.Value, otherValue)) {
                        return false;
                    }
                }

                return true;
            }

            Assert.That(UnorderedEquivalentTo(this._stringDoubleMap, map));
        }

        [Test]
        public void Can_pop_items_with_lowest_and_highest_scores_from_sorted_set() {
            this._storeMembers.ForEach(x => this.Redis.AddItemToSortedSet(this.SetId, x));

            this._storeMembers.Sort((x, y) => string.Compare(x, y, StringComparison.Ordinal));

            var lowestScore = this.Redis.PopItemWithLowestScoreFromSortedSet(this.SetId);
            Assert.That(lowestScore, Is.EqualTo(this._storeMembers.First()));

            var highestScore = this.Redis.PopItemWithHighestScoreFromSortedSet(this.SetId);
            Assert.That(highestScore, Is.EqualTo(this._storeMembers[this._storeMembers.Count - 1]));
        }

        [Test]
        public void Can_PopFromSet() {
            var i = 0;
            this._storeMembers.ForEach(x => this.Redis.AddItemToSortedSet(this.SetId, x, i++));

            var member = this.Redis.PopItemWithHighestScoreFromSortedSet(this.SetId);

            Assert.That(member, Is.EqualTo("four"));
        }

        [Test]
        public void Can_Remove_value_from_ICollection_Set() {
            IRedisSortedSet list = this.Redis.SortedSets[this.SetId];
            this._storeMembers.ForEach(list.Add);

            this._storeMembers.Remove("two");
            list.Remove("two");

            List<string> members = list.ToList();

            Assert.That(members, Is.EquivalentTo(this._storeMembers));
        }

        [Test]
        public void Can_RemoveFromSet() {
            const string removeMember = "two";

            this._storeMembers.ForEach(x => this.Redis.AddItemToSortedSet(this.SetId, x));

            this.Redis.RemoveItemFromSortedSet(this.SetId, removeMember);

            this._storeMembers.Remove(removeMember);

            List<string> members = this.Redis.GetAllItemsFromSortedSet(this.SetId);
            Assert.That(members, Is.EquivalentTo(this._storeMembers));
        }

        [Test]
        public void Can_RemoveItemsFromSortedSet() {
            var removeMembers = new[] { "two", "four", "six" };

            this._storeMembers.ForEach(x => this.Redis.AddItemToSortedSet(this.SetId, x));

            var removeCount = this.Redis.RemoveItemsFromSortedSet(this.SetId, removeMembers.ToList());
            Assert.That(removeCount, Is.EqualTo(2));

            foreach (var value in removeMembers) {
                this._storeMembers.Remove(value);
            }

            List<string> members = this.Redis.GetAllItemsFromSortedSet(this.SetId);
            Assert.That(members, Is.EquivalentTo(this._storeMembers));
        }

        [Test]
        public void Can_Store_IntersectBetweenSets() {
            var set1Name = this.PrefixedKey("testintersectset1");
            var set2Name = this.PrefixedKey("testintersectset2");
            var storeSetName = this.PrefixedKey("testintersectsetstore");
            var set1Members = new List<string> { "one", "two", "three", "four", "five" };
            var set2Members = new List<string> { "four", "five", "six", "seven" };

            set1Members.ForEach(x => this.Redis.AddItemToSortedSet(set1Name, x));
            set2Members.ForEach(x => this.Redis.AddItemToSortedSet(set2Name, x));

            this.Redis.StoreIntersectFromSortedSets(storeSetName, set1Name, set2Name);

            List<string> intersectingMembers = this.Redis.GetAllItemsFromSortedSet(storeSetName);

            Assert.That(intersectingMembers, Is.EquivalentTo(new List<string> { "four", "five" }));
        }

        [Test]
        public void Can_Store_UnionBetweenSets() {
            var set1Name = this.PrefixedKey("testunionset1");
            var set2Name = this.PrefixedKey("testunionset2");
            var storeSetName = this.PrefixedKey("testunionsetstore");
            var set1Members = new List<string> { "one", "two", "three", "four", "five" };
            var set2Members = new List<string> { "four", "five", "six", "seven" };

            set1Members.ForEach(x => this.Redis.AddItemToSortedSet(set1Name, x));
            set2Members.ForEach(x => this.Redis.AddItemToSortedSet(set2Name, x));

            this.Redis.StoreUnionFromSortedSets(storeSetName, set1Name, set2Name);

            List<string> unionMembers = this.Redis.GetAllItemsFromSortedSet(storeSetName);

            Assert.That(unionMembers,
                Is.EquivalentTo(
                    new List<string> { "one", "two", "three", "four", "five", "six", "seven" }));
        }

        [Test]
        public void Can_Test_Contains_in_ICollection_Set() {
            IRedisSortedSet list = this.Redis.SortedSets[this.SetId];
            this._storeMembers.ForEach(list.Add);

            Assert.That(list.Contains("two"), Is.True);
            Assert.That(list.Contains("five"), Is.False);
        }

        [Test]
        public void Can_use_SortedIndex_to_store_articles_by_Date() {
            IRedisTypedClient<Article> redisArticles = this.Redis.As<Article>();

            var articles = new[] {
                new Article { Id = 1, Title = "Article 1", ModifiedDate = new DateTime(2015, 01, 02) },
                new Article { Id = 2, Title = "Article 2", ModifiedDate = new DateTime(2015, 01, 01) },
                new Article { Id = 3, Title = "Article 3", ModifiedDate = new DateTime(2015, 01, 03) }
            };

            redisArticles.StoreAll(articles);

            const string latestArticlesSet = "urn:Article:modified";

            foreach (Article article in articles) {
                this.Redis.AddItemToSortedSet(latestArticlesSet, article.Id.ToString(), article.ModifiedDate.Ticks);
            }

            List<string> articleIds = this.Redis.GetAllItemsFromSortedSetDesc(latestArticlesSet);
            Console.WriteLine(articleIds.ToJson());

            IList<Article> latestArticles = redisArticles.GetByIds(articleIds);
            Console.WriteLine(latestArticles.ToJson());
        }

        [Test]
        public void Can_WorkInSortedSetUnderDifferentCulture() {
#if NETCOREAPP2_1
            CultureInfo prevCulture = CultureInfo.CurrentCulture;
            CultureInfo.CurrentCulture = new CultureInfo("ru-RU");
#else
            CultureInfo prevCulture = Thread.CurrentThread.CurrentCulture;
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("ru-RU");
#endif
            this.Redis.AddItemToSortedSet(this.SetId, "key", 123.22);

            IDictionary<string, double> map = this.Redis.GetAllWithScoresFromSortedSet(this.SetId);

            Assert.AreEqual(123.22, map["key"]);

#if NETCOREAPP2_1
            CultureInfo.CurrentCulture = prevCulture;
#else
            Thread.CurrentThread.CurrentCulture = prevCulture;
#endif
        }

        [Test]
        public void Does_SortedSetContainsValue() {
            const string existingMember = "two";
            const string nonExistingMember = "five";

            this._storeMembers.ForEach(x => this.Redis.AddItemToSortedSet(this.SetId, x));

            Assert.That(this.Redis.SortedSetContainsItem(this.SetId, existingMember), Is.True);
            Assert.That(this.Redis.SortedSetContainsItem(this.SetId, nonExistingMember), Is.False);
        }

        [Test]
        public void Score_from_non_existent_item_returns_NaN() {
            var score = this.Redis.GetItemScoreInSortedSet("nonexistentset", "value");

            Assert.That(score, Is.EqualTo(double.NaN));
        }

    }

}
