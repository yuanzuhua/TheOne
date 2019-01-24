using System;
using System.Collections.Generic;
using NUnit.Framework;

namespace TheOne.Redis.Tests.Examples {

    [TestFixture]
    internal sealed class AllTypesDataTests : RedisClientTestsBase {

        #region Models

        public class Article {

            public int Id { get; set; }
            public string Title { get; set; }
            public DateTime ModifiedDate { get; set; }

        }

        #endregion

        private void AddLists() {
            var storeMembers = new List<string> { "one", "two", "three", "four" };
            storeMembers.ForEach(x => this.Redis.AddItemToList("testlist", x));
        }

        private void AddSets() {
            var storeMembers = new List<string> { "one", "two", "three", "four" };
            storeMembers.ForEach(x => this.Redis.AddItemToSet("testset", x));
        }

        private void AddHashes() {
            var stringMap = new Dictionary<string, string> {
                { "one", "a" },
                { "two", "b" },
                { "three", "c" },
                { "four", "d" }
            };
            var stringIntMap = new Dictionary<string, int> {
                { "one", 1 },
                { "two", 2 },
                { "three", 3 },
                { "four", 4 }
            };

            foreach (var value in stringMap) {
                this.Redis.SetEntryInHash("testhash", value.Key, value.Value);
            }

            var hash = this.Redis.Hashes["testhash"];
            foreach (var value1 in stringIntMap) {
                hash.Add(value1.Key, value1.Value.ToString());
            }

        }

        private void AddSortedSets() {
            var i = 0;
            var storeMembers = new List<string> { "one", "two", "three", "four" };
            storeMembers.ForEach(x => this.Redis.AddItemToSortedSet("testzset", x, i++));

            var redisArticles = this.Redis.As<Article>();

            var articles = new[] {
                new Article { Id = 1, Title = "Article 1", ModifiedDate = new DateTime(2015, 01, 02) },
                new Article { Id = 2, Title = "Article 2", ModifiedDate = new DateTime(2015, 01, 01) },
                new Article { Id = 3, Title = "Article 3", ModifiedDate = new DateTime(2015, 01, 03) }
            };

            redisArticles.StoreAll(articles);

            const string latestArticlesSet = "urn:Article:modified";

            foreach (var article in articles) {
                this.Redis.AddItemToSortedSet(latestArticlesSet, article.Id.ToString(), article.ModifiedDate.Ticks);
            }
        }

        [Test]
        public void Create_test_data_for_all_types() {
            this.AddLists();
            this.AddSets();
            this.AddSortedSets();
            this.AddHashes();
        }

    }

}
