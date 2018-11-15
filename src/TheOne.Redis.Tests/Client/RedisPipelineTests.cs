using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.ClientManager;
using TheOne.Redis.Common;
using TheOne.Redis.Pipeline;

namespace TheOne.Redis.Tests.Client {

    [TestFixture]
    internal sealed class RedisPipelineTests : RedisClientTestsBase {

        private const string _key = "pipemultitest";
        private const string _listKey = "pipemultitest-list";
        private const string _setKey = "pipemultitest-set";
        private const string _sortedSetKey = "pipemultitest-sortedset";

        [Test]
        public void Can_call_AddRangeToSet_in_pipeline() {
            using (IRedisPipeline pipeline = this.Redis.CreatePipeline()) {
                var key = "pipeline-test";

                pipeline.QueueCommand(r => r.Remove(key));
                pipeline.QueueCommand(r => r.AddRangeToSet(key, new[] { "A", "B", "C" }.ToList()));

                pipeline.Flush();
            }
        }

        [Test]
        public void Can_call_hash_operations_in_pipeline() {
            Assert.That(this.Redis.GetValue(_key), Is.Null);
            var fields = new[] { "field1", "field2", "field3" };
            var values = new[] { "1", "2", "3" };
            var fieldBytes = new byte[fields.Length][];
            for (var i = 0; i < fields.Length; ++i) {
                fieldBytes[i] = Encoding.UTF8.GetBytes(fields[i]);

            }

            var valueBytes = new byte[values.Length][];
            for (var i = 0; i < values.Length; ++i) {
                valueBytes[i] = Encoding.UTF8.GetBytes(values[i]);

            }

            byte[][] members = null;
            IRedisPipeline pipeline = this.Redis.CreatePipeline();


            pipeline.QueueCommand(r => ((RedisNativeClient)r).HMSet(_key, fieldBytes, valueBytes));
            pipeline.QueueCommand(r => ((RedisNativeClient)r).HGetAll(_key), x => members = x);
            pipeline.Flush();

            for (var i = 0; i < members.Length; i += 2) {
                Assert.AreEqual(members[i], fieldBytes[i / 2]);
                Assert.AreEqual(members[i + 1], valueBytes[i / 2]);

            }

            pipeline.Dispose();
        }

        [Test]
        public void Can_call_multi_string_operations_in_pipeline() {
            string item1 = null;
            string item4 = null;

            var results = new List<string>();
            Assert.That(this.Redis.GetListCount(_listKey), Is.EqualTo(0));
            using (IRedisPipeline pipeline = this.Redis.CreatePipeline()) {
                pipeline.QueueCommand(r => r.AddItemToList(_listKey, "listitem1"));
                pipeline.QueueCommand(r => r.AddItemToList(_listKey, "listitem2"));
                pipeline.QueueCommand(r => r.AddItemToList(_listKey, "listitem3"));
                pipeline.QueueCommand(r => r.GetAllItemsFromList(_listKey), x => results = x);
                pipeline.QueueCommand(r => r.GetItemFromList(_listKey, 0), x => item1 = x);
                pipeline.QueueCommand(r => r.GetItemFromList(_listKey, 4), x => item4 = x);

                pipeline.Flush();
            }

            Assert.That(this.Redis.GetListCount(_listKey), Is.EqualTo(3));
            Assert.That(results, Is.EquivalentTo(new List<string> { "listitem1", "listitem2", "listitem3" }));
            Assert.That(item1, Is.EqualTo("listitem1"));
            Assert.That(item4, Is.Null);
        }

        [Test]
        public void Can_call_multiple_setexs_in_pipeline() {
            Assert.That(this.Redis.GetValue(_key), Is.Null);
            string[] keys = { _key + "key1", _key + "key2", _key + "key3" };
            var values = new[] { "1", "2", "3" };
            IRedisPipeline pipeline = this.Redis.CreatePipeline();

            for (var i = 0; i < 3; ++i) {
                var index0 = i;
                pipeline.QueueCommand(r => ((RedisNativeClient)r).SetEx(keys[index0], 100, Encoding.UTF8.GetBytes(values[index0])));
            }

            pipeline.Flush();
            pipeline.Replay();


            for (var i = 0; i < 3; ++i) {
                Assert.AreEqual(this.Redis.GetValue(keys[i]), values[i]);
            }

            pipeline.Dispose();
        }

        // Operations that are not supported in older versions will look at server info to determine what to do.
        // If server info is fetched each time, then it will interfere with pipeline
        [Test]
        public void Can_call_operation_not_supported_on_older_servers_in_pipeline() {
            var temp = new byte[1];
            using (IRedisPipeline pipeline = this.Redis.CreatePipeline()) {
                pipeline.QueueCommand(r => ((RedisNativeClient)r).SetEx(_key + "key", 5, temp));
                pipeline.Flush();
            }
        }

        [Test]
        public void Can_call_single_operation_3_Times_in_pipeline() {
            Assert.That(this.Redis.GetValue(_key), Is.Null);
            using (IRedisPipeline pipeline = this.Redis.CreatePipeline()) {
                pipeline.QueueCommand(r => r.IncrementValue(_key));
                pipeline.QueueCommand(r => r.IncrementValue(_key));
                pipeline.QueueCommand(r => r.IncrementValue(_key));

                pipeline.Flush();
            }

            Assert.That(this.Redis.GetValue(_key), Is.EqualTo("3"));
        }

        [Test]
        public void Can_call_single_operation_in_pipeline() {
            Assert.That(this.Redis.GetValue(_key), Is.Null);
            using (IRedisPipeline pipeline = this.Redis.CreatePipeline()) {
                pipeline.QueueCommand(r => r.IncrementValue(_key));
                var map = new Dictionary<string, int>();
                pipeline.QueueCommand(r => r.Get<int>(_key), y => map[_key] = y);

                pipeline.Flush();
                Console.WriteLine(map.ToJson());
            }

            Assert.That(this.Redis.GetValue(_key), Is.EqualTo("1"));
        }

        [Test]
        public void Can_call_single_operation_with_callback_3_Times_in_pipeline() {
            var results = new List<long>();
            Assert.That(this.Redis.GetValue(_key), Is.Null);
            using (IRedisPipeline pipeline = this.Redis.CreatePipeline()) {
                pipeline.QueueCommand(r => r.IncrementValue(_key), results.Add);
                pipeline.QueueCommand(r => r.IncrementValue(_key), results.Add);
                pipeline.QueueCommand(r => r.IncrementValue(_key), results.Add);

                pipeline.Flush();
            }

            Assert.That(this.Redis.GetValue(_key), Is.EqualTo("3"));
            Assert.That(results, Is.EquivalentTo(new List<long> { 1, 2, 3 }));
        }

        [Test]
        public void Can_Pop_priority_message_from_SortedSet_and_Add_to_workq_in_atomic_transaction() {
            var messages = new List<string> { "message4", "message3", "message2" };

            this.Redis.AddItemToList("workq", "message1");

            var priority = 1;
            messages.ForEach(x => this.Redis.AddItemToSortedSet("prioritymsgs", x, priority++));

            var highestPriorityMessage = this.Redis.PopItemWithHighestScoreFromSortedSet("prioritymsgs");

            using (IRedisPipeline trans = this.Redis.CreatePipeline()) {
                trans.QueueCommand(r => r.RemoveItemFromSortedSet("prioritymsgs", highestPriorityMessage));
                trans.QueueCommand(r => r.AddItemToList("workq", highestPriorityMessage));

                trans.Flush();
            }

            Assert.That(this.Redis.GetAllItemsFromList("workq"),
                Is.EquivalentTo(new List<string> { "message1", "message2" }));
            Assert.That(this.Redis.GetAllItemsFromSortedSet("prioritymsgs"),
                Is.EquivalentTo(new List<string> { "message3", "message4" }));
        }

        [Test]
        public void Can_Set_and_Expire_key_in_atomic_transaction() {
            TimeSpan oneSec = TimeSpan.FromSeconds(1);

            Assert.That(this.Redis.GetValue("key"), Is.Null);
            // Calls 'MULTI'
            using (IRedisPipeline trans = this.Redis.CreatePipeline()) {
                trans.QueueCommand(r => r.SetValue("key", "a"));         // Queues 'SET key a'
                trans.QueueCommand(r => r.ExpireEntryIn("key", oneSec)); // Queues 'EXPIRE key 1'
                trans.Flush();                                           // Calls 'EXEC'
            }
            // Calls 'DISCARD' if 'EXEC' wasn't called

            Assert.That(this.Redis.GetValue("key"), Is.EqualTo("a"));
            Thread.Sleep(TimeSpan.FromSeconds(2));
            Assert.That(this.Redis.GetValue("key"), Is.Null);
        }

        [Test]
        public void Can_SetAll_and_Publish_in_atomic_transaction() {
            var messages = new Dictionary<string, string> { { "a", "a" }, { "b", "b" } };
            using (IRedisPipeline pipeline = this.Redis.CreatePipeline()) {
                pipeline.QueueCommand(c => c.SetAll(messages.ToDictionary(t => t.Key, t => t.Value)));
                pipeline.QueueCommand(c => c.PublishMessage("uc", "b"));

                pipeline.Flush();
            }
        }

        [Test]
        public void Disposing_Client_Clears_Pipeline() {
            var clientMgr = new PooledRedisClientManager(Config.MasterHost);

            using (IRedisClient client = clientMgr.GetClient()) {
                client.Set("k1", "v1");
                client.Set("k2", "v2");
                client.Set("k3", "v3");

                using (IRedisPipeline pipe = client.CreatePipeline()) {
                    pipe.QueueCommand(c => c.Get<string>("k1"), p => throw new Exception());
                    pipe.QueueCommand(c => c.Get<string>("k2"));

                    try {
                        pipe.Flush();
                    } catch {
                        // The exception is expected. Swallow it.
                    }
                }
            }

            using (IRedisClient client = clientMgr.GetClient()) {
                Assert.AreEqual("v3", client.Get<string>("k3"));
            }
        }

        [Test]
        public void Exception_in_atomic_pipelines_discards_all_commands() {
            Assert.That(this.Redis.GetValue(_key), Is.Null);
            try {
                using (IRedisPipeline pipeline = this.Redis.CreatePipeline()) {
                    pipeline.QueueCommand(r => r.IncrementValue(_key));
                    throw new NotSupportedException();
                }
            } catch (NotSupportedException) {
                Assert.That(this.Redis.GetValue(_key), Is.Null);
            }
        }

        [Test]
        public void No_commit_of_atomic_pipelines_discards_all_commands() {
            Assert.That(this.Redis.GetValue(_key), Is.Null);
            using (IRedisPipeline pipeline = this.Redis.CreatePipeline()) {
                pipeline.QueueCommand(r => r.IncrementValue(_key));
            }

            Assert.That(this.Redis.GetValue(_key), Is.Null);
        }

        [Test]
        public void Pipeline_can_be_contain_watch() {
            var KeySquared = _key + _key;
            Assert.That(this.Redis.GetValue(_key), Is.Null);
            Assert.That(this.Redis.GetValue(KeySquared), Is.Null);
            using (IRedisPipeline pipeline = this.Redis.CreatePipeline()) {
                pipeline.QueueCommand(r => r.IncrementValue(_key));
                pipeline.QueueCommand(r => r.IncrementValue(KeySquared));
                pipeline.QueueCommand(r => ((RedisNativeClient)r).Watch(_key + "FOO"));
                pipeline.Flush();

                Assert.That(this.Redis.GetValue(_key), Is.EqualTo("1"));
                Assert.That(this.Redis.GetValue(KeySquared), Is.EqualTo("1"));
            }
        }

        [Test]
        public void Pipeline_can_be_replayed() {
            var KeySquared = _key + _key;
            Assert.That(this.Redis.GetValue(_key), Is.Null);
            Assert.That(this.Redis.GetValue(KeySquared), Is.Null);
            using (IRedisPipeline pipeline = this.Redis.CreatePipeline()) {
                pipeline.QueueCommand(r => r.IncrementValue(_key));
                pipeline.QueueCommand(r => r.IncrementValue(KeySquared));
                pipeline.Flush();

                Assert.That(this.Redis.GetValue(_key), Is.EqualTo("1"));
                Assert.That(this.Redis.GetValue(KeySquared), Is.EqualTo("1"));
                this.Redis.Del(_key);
                this.Redis.Del(KeySquared);
                Assert.That(this.Redis.GetValue(_key), Is.Null);
                Assert.That(this.Redis.GetValue(KeySquared), Is.Null);

                pipeline.Replay();
                pipeline.Dispose();
                Assert.That(this.Redis.GetValue(_key), Is.EqualTo("1"));
                Assert.That(this.Redis.GetValue(KeySquared), Is.EqualTo("1"));
            }
        }

        [Test]
        public void Supports_different_operation_types_in_same_pipeline() {
            var incrementResults = new List<long>();
            var collectionCounts = new List<long>();
            var containsItem = false;

            Assert.That(this.Redis.GetValue(_key), Is.Null);
            using (IRedisPipeline pipeline = this.Redis.CreatePipeline()) {
                pipeline.QueueCommand(r => r.IncrementValue(_key), intResult => incrementResults.Add(intResult));
                pipeline.QueueCommand(r => r.AddItemToList(_listKey, "listitem1"));
                pipeline.QueueCommand(r => r.AddItemToList(_listKey, "listitem2"));
                pipeline.QueueCommand(r => r.AddItemToSet(_setKey, "setitem"));
                pipeline.QueueCommand(r => r.SetContainsItem(_setKey, "setitem"), b => containsItem = b);
                pipeline.QueueCommand(r => r.AddItemToSortedSet(_sortedSetKey, "sortedsetitem1"));
                pipeline.QueueCommand(r => r.AddItemToSortedSet(_sortedSetKey, "sortedsetitem2"));
                pipeline.QueueCommand(r => r.AddItemToSortedSet(_sortedSetKey, "sortedsetitem3"));
                pipeline.QueueCommand(r => r.GetListCount(_listKey), intResult => collectionCounts.Add(intResult));
                pipeline.QueueCommand(r => r.GetSetCount(_setKey), intResult => collectionCounts.Add(intResult));
                pipeline.QueueCommand(r => r.GetSortedSetCount(_sortedSetKey), intResult => collectionCounts.Add(intResult));
                pipeline.QueueCommand(r => r.IncrementValue(_key), intResult => incrementResults.Add(intResult));

                pipeline.Flush();
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

    }

}
