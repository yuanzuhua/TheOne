using System.Collections.Generic;
using NUnit.Framework;

namespace TheOne.Redis.Tests.Basic {

    /// <summary>
    ///     Simple class to populate redis with some test data
    /// </summary>
    [TestFixture]
    internal sealed class PopulateTestData : RedisClientTestsBase {

        private const string _stringId = "urn:populatetest:string";
        private const string _listId = "urn:populatetest:list";
        private const string _setId = "urn:populatetest:set";
        private const string _sortedSetId = "urn:populatetest:zset";
        private const string _hashId = "urn:populatetest:hash";

        private readonly List<string> _items = new List<string> { "one", "two", "three", "four" };

        private readonly Dictionary<string, string> _map = new Dictionary<string, string> {
            { "A", "one" },
            { "B", "two" },
            { "C", "three" },
            { "D", "four" }
        };

        [Test]
        public void Populate_Hash() {
            this.Redis.SetRangeInHash(_hashId, this._map);
        }

        [Test]
        public void Populate_List() {
            this._items.ForEach(x => this.Redis.AddItemToList(_listId, x));
        }

        [Test]
        public void Populate_Set() {
            this._items.ForEach(x => this.Redis.AddItemToSet(_setId, x));
        }

        [Test]
        public void Populate_SortedSet() {
            var i = 0;
            this._items.ForEach(x => this.Redis.AddItemToSortedSet(_sortedSetId, x, i++));
        }

        [Test]
        public void Populate_Strings() {
            this._items.ForEach(x => this.Redis.Set(_stringId + ":" + x, x));
        }

    }

}
