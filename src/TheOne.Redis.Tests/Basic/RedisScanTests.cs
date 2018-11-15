using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using TheOne.Redis.Client;

namespace TheOne.Redis.Tests.Basic {

    [TestFixture]
    internal sealed class RedisScanTests : RedisClientTestsBase {

        [Test]
        public void Can_HScan_10_hashes() {
            List<string> values = Enumerable.Range(0, 10).Select(x => "VALUE" + x).ToList();
            this.Redis.SetRangeInHash("scanhash", values.ToDictionary(x => x.Replace("VALUE", "KEY"), x1 => x1));

            ScanResult ret = this.Redis.HScan("scanhash", 0);

            Dictionary<string, string> keyValues = ret.AsKeyValues();

            Assert.That(ret.Cursor, Is.GreaterThanOrEqualTo(0));
            Assert.That(keyValues.Keys, Is.EquivalentTo(values.ConvertAll(x => x.Replace("VALUE", "KEY"))));
            Assert.That(keyValues.Values, Is.EquivalentTo(values));
        }

        [Test]
        public void Can_scan_10_collection() {
            List<string> keys = Enumerable.Range(0, 10).Select(x => "KEY" + x).ToList();
            this.Redis.SetAll(keys.ToDictionary(x => x, x1 => x1));

            ScanResult ret = this.Redis.Scan(0);

            Assert.That(ret.Cursor, Is.GreaterThanOrEqualTo(0));
            Assert.That(ret.AsStrings(), Is.EquivalentTo(keys));
        }

        [Test]
        public void Can_scan_100_collection_over_cursor() {
            var allKeys = new HashSet<string>();
            List<string> keys = Enumerable.Range(0, 100).Select(x => "KEY" + x).ToList();
            this.Redis.SetAll(keys.ToDictionary(x => x, x1 => x1));

            var i = 0;
            var ret = new ScanResult();
            while (true) {
                ret = this.Redis.Scan(ret.Cursor, 10);
                i++;
                ret.AsStrings().ForEach(x => allKeys.Add(x));
                if (ret.Cursor == 0) {
                    break;
                }
            }

            Assert.That(i, Is.GreaterThanOrEqualTo(2));
            Assert.That(allKeys.Count, Is.EqualTo(keys.Count));
            Assert.That(allKeys, Is.EquivalentTo(keys));
        }

        [Test]
        public void Can_scan_and_search_10_collection() {
            List<string> keys = Enumerable.Range(0, 11).Select(x => "KEY" + x).ToList();
            this.Redis.SetAll(keys.ToDictionary(x => x, x1 => x1));

            ScanResult ret = this.Redis.Scan(0, 11, "KEY1*");

            Assert.That(ret.Cursor, Is.GreaterThanOrEqualTo(0));
            Assert.That(ret.AsStrings(), Is.EquivalentTo(new[] { "KEY1", "KEY10" }));
        }

        [Test]
        public void Can_SScan_10_sets() {
            List<string> items = Enumerable.Range(0, 10).Select(x => "item" + x).ToList();
            items.ForEach(x => this.Redis.AddItemToSet("scanset", x));

            ScanResult ret = this.Redis.SScan("scanset", 0);

            Assert.That(ret.Cursor, Is.GreaterThanOrEqualTo(0));
            Assert.That(ret.AsStrings(), Is.EquivalentTo(items));
        }

        [Test]
        public void Can_ZScan_10_SortedSets() {
            List<string> items = Enumerable.Range(0, 10).Select(x => "item" + x).ToList();
            var i = 0;
            items.ForEach(x => this.Redis.AddItemToSortedSet("scanzset", x, i++));

            ScanResult ret = this.Redis.ZScan("scanzset", 0);
            Dictionary<string, double> itemsWithScore = ret.AsItemsWithScores();

            Assert.That(itemsWithScore.Keys, Is.EqualTo(items));
            Assert.That(itemsWithScore.Values, Is.EqualTo(Enumerable.Range(0, 10).Select(x => (double)x).ToList()));
        }

        [Test]
        public void Does_lazy_scan_all_hash_items() {
            List<string> values = Enumerable.Range(0, 100).Select(x => "VALUE" + x).ToList();
            this.Redis.SetRangeInHash("scanhash", values.ToDictionary(x => x.Replace("VALUE", "KEY"), x1 => x1));

            IEnumerable<KeyValuePair<string, string>> scanAllItems = this.Redis.ScanAllHashEntries("scanhash", pageSize: 10);
            List<KeyValuePair<string, string>> tenKeys = scanAllItems.Take(10).ToList();

            Assert.That(tenKeys.Count, Is.EqualTo(10));

            Assert.That(scanAllItems.Count(), Is.EqualTo(100));

            Dictionary<string, string> map = scanAllItems.ToDictionary(x => x.Key, x => x.Value);
            Assert.That(map.Values, Is.EquivalentTo(values));
        }

        [Test]
        public void Does_lazy_scan_all_keys() {
            List<string> keys = Enumerable.Range(0, 100).Select(x => "KEY" + x).ToList();
            this.Redis.SetAll(keys.ToDictionary(x => x, x1 => x1));

            IEnumerable<string> scanAllKeys = this.Redis.ScanAllKeys(pageSize: 10);
            List<string> tenKeys = scanAllKeys.Take(10).ToList();

            Assert.That(tenKeys.Count, Is.EqualTo(10));

            Assert.That(scanAllKeys.Count(), Is.EqualTo(100));
        }

        [Test]
        public void Does_lazy_scan_all_set_items() {
            List<string> items = Enumerable.Range(0, 100).Select(x => "item" + x).ToList();
            items.ForEach(x => this.Redis.AddItemToSet("scanset", x));

            IEnumerable<string> scanAllItems = this.Redis.ScanAllSetItems("scanset", pageSize: 10);
            List<string> tenKeys = scanAllItems.Take(10).ToList();

            Assert.That(tenKeys.Count, Is.EqualTo(10));

            Assert.That(scanAllItems.Count(), Is.EqualTo(100));
        }

        [Test]
        public void Does_lazy_scan_all_SortedSet_items() {
            List<string> items = Enumerable.Range(0, 100).Select(x => "item" + x).ToList();
            var i = 0;
            items.ForEach(x => this.Redis.AddItemToSortedSet("scanzset", x, i++));

            IEnumerable<KeyValuePair<string, double>> scanAllItems = this.Redis.ScanAllSortedSetItems("scanzset", pageSize: 10);
            List<KeyValuePair<string, double>> tenKeys = scanAllItems.Take(10).ToList();

            Assert.That(tenKeys.Count, Is.EqualTo(10));

            Assert.That(scanAllItems.Count(), Is.EqualTo(100));

            Dictionary<string, double> map = scanAllItems.ToDictionary(x => x.Key, x => x.Value);
            Assert.That(map.Keys, Is.EquivalentTo(items));
        }

    }

}
