using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Common;
using TheOne.Redis.Pipeline;

namespace TheOne.Redis.Tests.Client {

    [TestFixture]
    internal sealed class RedisTransactionTests : RedisClientTestsBase {

        private const string _key = "rdtmultitest";
        private const string _listKey = "rdtmultitest-list";
        private const string _setKey = "rdtmultitest-set";
        private const string _sortedSetKey = "rdtmultitest-sortedset";
        private const string _hashKey = "rdthashtest";
        private const string _prefix = "tran";

        [Test]
        public void Can_call_GetAllEntriesFromHash_in_transaction() {
            var stringMap = new Dictionary<string, string> {
                { "one", "a" },
                { "two", "b" },
                { "three", "c" },
                { "four", "d" }
            };
            foreach (KeyValuePair<string, string> value in stringMap) {
                this.Redis.SetEntryInHash(_hashKey, value.Key, value.Value);
            }

            Dictionary<string, string> results = null;
            using (IRedisTransaction trans = this.Redis.CreateTransaction()) {
                trans.QueueCommand(r => r.GetAllEntriesFromHash(_hashKey), x => results = x);

                trans.Commit();
            }

            Assert.That(results, Is.EquivalentTo(stringMap));
        }

        [Test]
        public void Can_call_HashSet_commands_in_transaction() {
            this.Redis.AddItemToSet("set", "ITEM 1");
            this.Redis.AddItemToSet("set", "ITEM 2");
            HashSet<string> result = null;

            using (IRedisTransaction trans = this.Redis.CreateTransaction()) {
                trans.QueueCommand(r => r.GetAllItemsFromSet("set"), values => result = values);

                trans.Commit();
            }

            Assert.That(result, Is.EquivalentTo(new[] { "ITEM 1", "ITEM 2" }));
        }

        [Test]
        public void Can_call_LUA_Script_in_transaction() {
            using (IRedisTransaction trans = this.Redis.CreateTransaction()) {
                trans.QueueCommand(r => r.ExecLua("return {'myval', 'myotherval'}"));

                trans.Commit();
            }

            RedisText result = null;
            using (IRedisTransaction trans = this.Redis.CreateTransaction()) {
                trans.QueueCommand(r => r.ExecLua("return {'myval', 'myotherval'}"), s => result = s);

                trans.Commit();
            }

            Assert.That(result.Children[0].Text, Is.EqualTo("myval"));
            Assert.That(result.Children[1].Text, Is.EqualTo("myotherval"));
        }

        [Test]
        public void Can_call_multi_string_operations_in_transaction() {
            string item1 = null;
            string item4 = null;

            var results = new List<string>();
            Assert.That(this.Redis.GetListCount(_listKey), Is.EqualTo(0));
            using (IRedisTransaction trans = this.Redis.CreateTransaction()) {
                trans.QueueCommand(r => r.AddItemToList(_listKey, "listitem1"));
                trans.QueueCommand(r => r.AddItemToList(_listKey, "listitem2"));
                trans.QueueCommand(r => r.AddItemToList(_listKey, "listitem3"));
                trans.QueueCommand(r => r.GetAllItemsFromList(_listKey), x => results = x);
                trans.QueueCommand(r => r.GetItemFromList(_listKey, 0), x => item1 = x);
                trans.QueueCommand(r => r.GetItemFromList(_listKey, 4), x => item4 = x);

                trans.Commit();
            }

            Assert.That(this.Redis.GetListCount(_listKey), Is.EqualTo(3));
            Assert.That(results, Is.EquivalentTo(new List<string> { "listitem1", "listitem2", "listitem3" }));
            Assert.That(item1, Is.EqualTo("listitem1"));
            Assert.That(item4, Is.Null);
        }

        [Test]
        public void Can_call_multiple_setexs_in_transaction() {
            Assert.That(this.Redis.GetValue(_key), Is.Null);
            var keys = new[] { "key1", "key2", "key3" };
            var values = new[] { "1", "2", "3" };
            IRedisTransaction trans = this.Redis.CreateTransaction();

            for (var i = 0; i < 3; ++i) {
                var index0 = i;
                trans.QueueCommand(r => ((RedisNativeClient)r).SetEx(keys[index0], 100, Encoding.UTF8.GetBytes(values[index0])));
            }

            trans.Commit();
            trans.Replay();


            for (var i = 0; i < 3; ++i) {
                Assert.AreEqual(this.Redis.GetValue(keys[i]), values[i]);
            }

            trans.Dispose();
        }

        [Test]
        // Operations that are not supported in older versions will look at server info to determine what to do.
        // If server info is fetched each time, then it will interfere with transaction
        public void Can_call_operation_not_supported_on_older_servers_in_transaction() {
            var temp = new byte[1];
            using (IRedisTransaction trans = this.Redis.CreateTransaction()) {
                trans.QueueCommand(r => ((RedisNativeClient)r).SetEx(_key, 5, temp));
                trans.Commit();
            }
        }

        [Test]
        public void Can_call_SetValueIfNotExists_in_transaction() {
            var f = false;
            var s = false;

            using (IRedisTransaction trans = this.Redis.CreateTransaction()) {
                trans.QueueCommand(c => c.SetValueIfNotExists("foo", "blah"), r => f = r);
                trans.QueueCommand(c => c.SetValueIfNotExists("bar", "blah"), r => s = r);
                trans.Commit();
            }

            Assert.That(f);
            Assert.That(s);
        }

        [Test]
        public void Can_call_single_operation_3_Times_in_transaction() {
            Assert.That(this.Redis.GetValue(_key), Is.Null);
            using (IRedisTransaction trans = this.Redis.CreateTransaction()) {
                trans.QueueCommand(r => r.IncrementValue(_key));
                trans.QueueCommand(r => r.IncrementValue(_key));
                trans.QueueCommand(r => r.IncrementValue(_key));

                trans.Commit();
            }

            Assert.That(this.Redis.GetValue(_key), Is.EqualTo("3"));
        }

        [Test]
        public void Can_call_single_operation_in_transaction() {
            Assert.That(this.Redis.GetValue(_key), Is.Null);
            using (IRedisTransaction trans = this.Redis.CreateTransaction()) {
                trans.QueueCommand(r => r.IncrementValue(_key));
                var map = new Dictionary<string, int>();
                trans.QueueCommand(r => r.Get<int>(_key), y => map[_key] = y);

                trans.Commit();

                Console.WriteLine(map.ToJson());
            }

            Assert.That(this.Redis.GetValue(_key), Is.EqualTo("1"));
        }

        [Test]
        public void Can_call_single_operation_with_callback_3_Times_in_transaction() {
            var results = new List<long>();
            Assert.That(this.Redis.GetValue(_key), Is.Null);
            using (IRedisTransaction trans = this.Redis.CreateTransaction()) {
                trans.QueueCommand(r => r.IncrementValue(_key), results.Add);
                trans.QueueCommand(r => r.IncrementValue(_key), results.Add);
                trans.QueueCommand(r => r.IncrementValue(_key), results.Add);

                trans.Commit();
            }

            Assert.That(this.Redis.GetValue(_key), Is.EqualTo("3"));
            Assert.That(results, Is.EquivalentTo(new List<long> { 1, 2, 3 }));
        }

        [Test]
        public void Can_call_Type_in_transaction() {
            this.Redis.SetValue("string", "STRING");
            this.Redis.AddItemToList("list", "LIST");
            this.Redis.AddItemToSet("set", "SET");
            this.Redis.AddItemToSortedSet("zset", "ZSET", 1);

            var keys = new[] { "string", "list", "set", "zset" };

            var results = new Dictionary<string, string>();
            using (IRedisTransaction trans = this.Redis.CreateTransaction()) {
                foreach (var key in keys) {
                    trans.QueueCommand(r => r.Type(key), x => results[key] = x);
                }

                trans.Commit();
            }

            Console.WriteLine(results.ToJson());

            Assert.That(results,
                Is.EquivalentTo(new Dictionary<string, string> {
                    { "string", "string" },
                    { "list", "list" },
                    { "set", "set" },
                    { "zset", "zset" }
                }));
        }

        [Test]
        public void Can_Pop_priority_message_from_SortedSet_and_Add_to_workq_in_atomic_transaction() {
            var messages = new List<string> { "message4", "message3", "message2" };

            this.Redis.AddItemToList(_prefix + "workq", "message1");

            var priority = 1;
            messages.ForEach(x => this.Redis.AddItemToSortedSet(_prefix + "prioritymsgs", x, priority++));

            var highestPriorityMessage = this.Redis.PopItemWithHighestScoreFromSortedSet(_prefix + "prioritymsgs");

            using (IRedisTransaction trans = this.Redis.CreateTransaction()) {
                trans.QueueCommand(r => r.RemoveItemFromSortedSet(_prefix + "prioritymsgs", highestPriorityMessage));
                trans.QueueCommand(r => r.AddItemToList(_prefix + "workq", highestPriorityMessage));

                trans.Commit();
            }

            Assert.That(this.Redis.GetAllItemsFromList(_prefix + "workq"),
                Is.EquivalentTo(new List<string> { "message1", "message2" }));
            Assert.That(this.Redis.GetAllItemsFromSortedSet(_prefix + "prioritymsgs"),
                Is.EquivalentTo(new List<string> { "message3", "message4" }));
        }

        [Test]
        public void Can_Set_and_Expire_key_in_atomic_transaction() {
            TimeSpan oneSec = TimeSpan.FromSeconds(1);

            Assert.That(this.Redis.GetValue(_prefix + "key"), Is.Null);
            // Calls 'MULTI'
            using (IRedisTransaction trans = this.Redis.CreateTransaction()) {
                trans.QueueCommand(r => r.SetValue(_prefix + "key", "a"));         // Queues 'SET key a'
                trans.QueueCommand(r => r.ExpireEntryIn(_prefix + "key", oneSec)); // Queues 'EXPIRE key 1'

                trans.Commit(); // Calls 'EXEC'

            } // Calls 'DISCARD' if 'EXEC' wasn't called

            Assert.That(this.Redis.GetValue(_prefix + "key"), Is.EqualTo("a"));
            Thread.Sleep(TimeSpan.FromSeconds(2));
            Assert.That(this.Redis.GetValue(_prefix + "key"), Is.Null);
        }

        [Test]
        public void Can_set_Expiry_on_key_in_transaction() {
            TimeSpan expiresIn = TimeSpan.FromMinutes(15);

            const string key = "No TTL-Transaction";
            var keyWithTtl = string.Format("{0}s TTL-Transaction", expiresIn.TotalSeconds);

            using (IRedisTransaction trans = this.Redis.CreateTransaction()) {
                trans.QueueCommand(r => r.Add(key, "Foo"));
                trans.QueueCommand(r => r.Add(keyWithTtl, "Bar", expiresIn));

                if (!trans.Commit()) {
                    throw new Exception("Transaction Failed");
                }
            }

            Assert.That(this.Redis.Get<string>(key), Is.EqualTo("Foo"));
            Assert.That(this.Redis.Get<string>(keyWithTtl), Is.EqualTo("Bar"));

            Assert.That(this.Redis.GetTimeToLive(key), Is.EqualTo(TimeSpan.MaxValue));
            TimeSpan? timeSpan = this.Redis.GetTimeToLive(keyWithTtl);
            Assert.NotNull(timeSpan);
            Assert.That(timeSpan.Value.TotalSeconds, Is.GreaterThan(1));
        }

        [Test]
        public void Does_not_set_Expiry_on_existing_key_in_transaction() {
            TimeSpan expiresIn = TimeSpan.FromMinutes(15);

            var key = "Exting TTL-Transaction";
            this.Redis.Add(key, "Foo");

            using (IRedisTransaction trans = this.Redis.CreateTransaction()) {
                trans.QueueCommand(r => r.Add(key, "Bar", expiresIn));

                if (!trans.Commit()) {
                    throw new Exception("Transaction Failed");
                }
            }

            Assert.That(this.Redis.Get<string>(key), Is.EqualTo("Foo"));
            Assert.That(this.Redis.GetTimeToLive(key), Is.EqualTo(TimeSpan.MaxValue));
        }

        [Test]
        public void Exception_in_atomic_transactions_discards_all_commands() {
            Assert.That(this.Redis.GetValue(_key), Is.Null);
            try {
                using (IRedisTransaction trans = this.Redis.CreateTransaction()) {
                    trans.QueueCommand(r => r.IncrementValue(_key));
                    throw new NotSupportedException();
                }
            } catch (NotSupportedException) {
                Assert.That(this.Redis.GetValue(_key), Is.Null);
            }
        }

        [Test]
        public void No_commit_of_atomic_transactions_discards_all_commands() {
            Assert.That(this.Redis.GetValue(_key), Is.Null);
            using (IRedisTransaction trans = this.Redis.CreateTransaction()) {
                trans.QueueCommand(r => r.IncrementValue(_key));
            }

            Assert.That(this.Redis.GetValue(_key), Is.Null);
        }

        [Test]
        public void Supports_different_operation_types_in_same_transaction() {
            var incrementResults = new List<long>();
            var collectionCounts = new List<long>();
            var containsItem = false;

            Assert.That(this.Redis.GetValue(_key), Is.Null);
            using (IRedisTransaction trans = this.Redis.CreateTransaction()) {
                trans.QueueCommand(r => r.IncrementValue(_key), intResult => incrementResults.Add(intResult));
                trans.QueueCommand(r => r.AddItemToList(_listKey, "listitem1"));
                trans.QueueCommand(r => r.AddItemToList(_listKey, "listitem2"));
                trans.QueueCommand(r => r.AddItemToSet(_setKey, "setitem"));
                trans.QueueCommand(r => r.SetContainsItem(_setKey, "setitem"), b => containsItem = b);
                trans.QueueCommand(r => r.AddItemToSortedSet(_sortedSetKey, "sortedsetitem1"));
                trans.QueueCommand(r => r.AddItemToSortedSet(_sortedSetKey, "sortedsetitem2"));
                trans.QueueCommand(r => r.AddItemToSortedSet(_sortedSetKey, "sortedsetitem3"));
                trans.QueueCommand(r => r.GetListCount(_listKey), intResult => collectionCounts.Add(intResult));
                trans.QueueCommand(r => r.GetSetCount(_setKey), intResult => collectionCounts.Add(intResult));
                trans.QueueCommand(r => r.GetSortedSetCount(_sortedSetKey), intResult => collectionCounts.Add(intResult));
                trans.QueueCommand(r => r.IncrementValue(_key), intResult => incrementResults.Add(intResult));

                trans.Commit();
            }

            Assert.That(containsItem, Is.True);
            Assert.That(this.Redis.GetValue(_key), Is.EqualTo("2"));
            Assert.That(incrementResults, Is.EquivalentTo(new List<long> { 1, 2 }));
            Assert.That(collectionCounts, Is.EquivalentTo(new List<int> { 2, 1, 3 }));
            Assert.That(this.Redis.GetAllItemsFromList(_listKey), Is.EquivalentTo(new List<string> { "listitem1", "listitem2" }));
            Assert.That(this.Redis.GetAllItemsFromSet(_setKey), Is.EquivalentTo(new List<string> { "setitem" }));
            Assert.That(this.Redis.GetAllItemsFromSortedSet(_sortedSetKey),
                Is.EquivalentTo(new List<string> { "sortedsetitem1", "sortedsetitem2", "sortedsetitem3" }));
        }


        [Test]
        public void Transaction_can_be_replayed() {
            var keySquared = _key + _key;
            Assert.That(this.Redis.GetValue(_key), Is.Null);
            Assert.That(this.Redis.GetValue(keySquared), Is.Null);
            using (IRedisTransaction trans = this.Redis.CreateTransaction()) {
                trans.QueueCommand(r => r.IncrementValue(_key));
                trans.QueueCommand(r => r.IncrementValue(keySquared));
                trans.Commit();

                Assert.That(this.Redis.GetValue(_key), Is.EqualTo("1"));
                Assert.That(this.Redis.GetValue(keySquared), Is.EqualTo("1"));
                this.Redis.Del(_key);
                this.Redis.Del(keySquared);
                Assert.That(this.Redis.GetValue(_key), Is.Null);
                Assert.That(this.Redis.GetValue(keySquared), Is.Null);

                trans.Replay();
                trans.Dispose();
                Assert.That(this.Redis.GetValue(_key), Is.EqualTo("1"));
                Assert.That(this.Redis.GetValue(keySquared), Is.EqualTo("1"));
            }
        }

        [Test]
        public void Transaction_can_issue_watch() {
            this.Redis.Del(_key);
            Assert.That(this.Redis.GetValue(_key), Is.Null);

            var keySquared = _key + _key;
            this.Redis.Del(keySquared);

            this.Redis.Watch(_key, keySquared);
            this.Redis.Set(_key, 7);

            using (IRedisTransaction trans = this.Redis.CreateTransaction()) {
                trans.QueueCommand(r => r.Set(_key, 1));
                trans.QueueCommand(r => r.Set(keySquared, 2));
                trans.Commit();
            }

            Assert.That(this.Redis.GetValue(_key), Is.EqualTo("7"));
            Assert.That(this.Redis.GetValue(keySquared), Is.Null);
        }

        [Test]
        public void Watch_aborts_transaction() {
            Assert.That(this.Redis.GetValue(_key), Is.Null);
            const string value1 = "value1";
            try {
                this.Redis.Watch(_key);
                this.Redis.Set(_key, value1);
                using (IRedisTransaction trans = this.Redis.CreateTransaction()) {
                    trans.QueueCommand(r => r.Set(_key, value1));
                    var success = trans.Commit();
                    Assert.False(success);
                    Assert.AreEqual(value1, this.Redis.Get<string>(_key));
                }
            } catch (NotSupportedException) {
                Assert.That(this.Redis.GetValue(_key), Is.Null);
            }
        }

    }

}
