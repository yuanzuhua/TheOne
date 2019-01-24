using System;
using System.Linq;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Common;

namespace TheOne.Redis.Tests.Basic {

    [TestFixture]
    internal sealed class LexTests : RedisClientTestsBase {

        private readonly string[] _values = "a,b,c,d,e,f,g".Split(',');

        /// <inheritdoc />
        public override void SetUp() {
            base.SetUp();
            foreach (var value in this._values) {
                this.Redis.ZAdd("zset", 0, value.ToUtf8Bytes());
            }
        }

        [Test]
        public void Can_Calculate_Lexical_Score() {
            const string minScore = "AAAA";
            const string nextMinScore = "AAAB";
            const string maxScore = "ZZZZ";

            Assert.That(RedisClient.GetLexicalScore(minScore),
                Is.LessThan(RedisClient.GetLexicalScore(nextMinScore)));

            Assert.That(RedisClient.GetLexicalScore(nextMinScore),
                Is.LessThan(RedisClient.GetLexicalScore(maxScore)));

            Console.WriteLine("Lexical Score of '{0}' is: {1}", minScore, RedisClient.GetLexicalScore(minScore));
            Console.WriteLine("Lexical Score of '{0}' is: {1}", nextMinScore, RedisClient.GetLexicalScore(nextMinScore));
            Console.WriteLine("Lexical Score of '{0}' is: {1}", maxScore, RedisClient.GetLexicalScore(maxScore));
        }

        [Test]
        public void Can_RemoveRangeFromSortedSetBySearch() {
            var removed = this.Redis.RemoveRangeFromSortedSetBySearch("zset", "[aaa", "(g");
            Assert.That(removed, Is.EqualTo(5));

            var remainder = this.Redis.SearchSortedSet("zset");
            Assert.That(remainder, Is.EqualTo(new[] { "a", "g" }));
        }

        [Test]
        public void Can_SearchSortedSet() {
            Assert.That(this.Redis.SearchSortedSet("zset"), Is.EquivalentTo(this._values));
            Assert.That(this.Redis.SearchSortedSet("zset", "-"), Is.EquivalentTo(this._values));
            Assert.That(this.Redis.SearchSortedSet("zset", end: "+"), Is.EquivalentTo(this._values));

            Assert.That(this.Redis.SearchSortedSet("zset", "[aaa").Count, Is.EqualTo(this._values.Length - 1));
            Assert.That(this.Redis.SearchSortedSet("zset", end: "(g").Count, Is.EqualTo(this._values.Length - 1));
            Assert.That(this.Redis.SearchSortedSet("zset", "[aaa", "(g").Count, Is.EqualTo(this._values.Length - 2));

            Assert.That(this.Redis.SearchSortedSet("zset", "a", "c").Count, Is.EqualTo(3));
            Assert.That(this.Redis.SearchSortedSet("zset", "[a", "[c").Count, Is.EqualTo(3));
            Assert.That(this.Redis.SearchSortedSet("zset", "a", "(c").Count, Is.EqualTo(2));
            Assert.That(this.Redis.SearchSortedSet("zset", "(a", "(c").Count, Is.EqualTo(1));
        }

        [Test]
        public void Can_SearchSortedSetCount() {
            Assert.That(this.Redis.SearchSortedSet("zset"), Is.EquivalentTo(this._values));
            Assert.That(this.Redis.SearchSortedSetCount("zset", "-"), Is.EqualTo(this._values.Length));
            Assert.That(this.Redis.SearchSortedSetCount("zset", end: "+"), Is.EqualTo(this._values.Length));

            Assert.That(this.Redis.SearchSortedSetCount("zset", "[aaa"), Is.EqualTo(this._values.Length - 1));
            Assert.That(this.Redis.SearchSortedSetCount("zset", end: "(g"), Is.EqualTo(this._values.Length - 1));
            Assert.That(this.Redis.SearchSortedSetCount("zset", "[aaa", "(g"), Is.EqualTo(this._values.Length - 2));

            Assert.That(this.Redis.SearchSortedSetCount("zset", "a", "c"), Is.EqualTo(3));
            Assert.That(this.Redis.SearchSortedSetCount("zset", "[a", "[c"), Is.EqualTo(3));
            Assert.That(this.Redis.SearchSortedSetCount("zset", "a", "(c"), Is.EqualTo(2));
            Assert.That(this.Redis.SearchSortedSetCount("zset", "(a", "(c"), Is.EqualTo(1));
        }

        [Test]
        public void Can_ZlexCount() {
            var total = this.Redis.ZLexCount("zset", "-", "+");
            Assert.That(total, Is.EqualTo(this._values.Length));

            Assert.That(this.Redis.ZLexCount("zset", "-", "[c"), Is.EqualTo(3));
            Assert.That(this.Redis.ZLexCount("zset", "-", "(c"), Is.EqualTo(2));
        }

        [Test]
        public void Can_ZRangeByLex_all_entries() {
            var results = this.Redis.ZRangeByLex("zset", "-", "+");

            Assert.That(results.Select(x => x.FromUtf8Bytes()).ToArray(), Is.EquivalentTo(this._values));

            results = this.Redis.ZRangeByLex("zset", "-", "+", 1, 3);
            Assert.That(results.Select(x => x.FromUtf8Bytes()).ToArray(), Is.EquivalentTo(new[] { "b", "c", "d" }));
        }

        [Test]
        public void Can_ZRangeByLex_Desc() {
            var descInclusive = this.Redis.ZRangeByLex("zset", "-", "[c");
            Assert.That(descInclusive.Select(x => x.FromUtf8Bytes()).ToArray(), Is.EquivalentTo(new[] { "a", "b", "c" }));

            var descExclusive = this.Redis.ZRangeByLex("zset", "-", "(c");
            Assert.That(descExclusive.Select(x => x.FromUtf8Bytes()).ToArray(), Is.EquivalentTo(new[] { "a", "b" }));
        }

        [Test]
        public void Can_ZRangeByLex_Min_and_Max() {
            var range = this.Redis.ZRangeByLex("zset", "[aaa", "(g");
            Assert.That(range.Select(x => x.FromUtf8Bytes()).ToArray(),
                Is.EquivalentTo(new[] { "b", "c", "d", "e", "f" }));
        }

        [Test]
        public void Can_ZRemRangeByLex() {
            var removed = this.Redis.ZRemRangeByLex("zset", "[aaa", "(g");
            Assert.That(removed, Is.EqualTo(5));

            var remainder = this.Redis.ZRangeByLex("zset", "-", "+");
            Assert.That(remainder.Select(x => x.FromUtf8Bytes()).ToArray(), Is.EqualTo(new[] { "a", "g" }));
        }

    }

}
