using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Pipeline;

namespace TheOne.Redis.Tests.Issues {

    [TestFixture]
    internal sealed class TransactionIssueTests : RedisClientTestsBase {

        private void CheckThisConnection() {
            Console.WriteLine("CheckingThisConnection()...");

            using (var redis = new RedisClient(Config.MasterHost)) {
                using (IRedisTransaction trans = redis.CreateTransaction()) {
                    trans.QueueCommand(
                        r => r.SetEntryInHash("Test", "Price", "123"));
                    trans.QueueCommand(
                        r => r.SetEntryInHash("Test2", "Price", "123"));
                    trans.QueueCommand(
                        r => r.SetEntryInHash("Test3", "Price", "123"));
                    trans.QueueCommand(
                        r => r.SetEntryInHash("Test4", "Price", "123"));
                    trans.QueueCommand(
                        r => r.SetEntryInHash("Test5", "Price", "123"));
                    trans.QueueCommand(
                        r => r.SetEntryInHash("Test6", "Price", "123"));
                    trans.QueueCommand(
                        r => r.SetEntryInHash("Test7", "Price", "123"));
                    trans.QueueCommand(
                        r => r.SetEntryInHash("Test8", "Price", "123"));
                    trans.QueueCommand(
                        r => r.SetEntryInHash("Test9", "Price", "123"));
                    trans.QueueCommand(
                        r => r.SetEntryInHash("Test10", "Price", "123"));
                    trans.QueueCommand(
                        r => r.SetEntryInHash("Test11", "Price", "123"));
                    trans.QueueCommand(
                        r => r.SetEntryInHash("Test12", "Price", "123"));
                    trans.QueueCommand(
                        r => r.SetEntryInHash("Test13", "Price", "123"));
                    trans.QueueCommand(
                        r => r.SetEntryInHash("Test14", "Price", "123"));
                    trans.QueueCommand(
                        r => r.SetEntryInHash("Test15", "Price", "123"));
                    trans.QueueCommand(
                        r => r.SetEntryInHash("Test16", "Price", "123"));
                    trans.QueueCommand(
                        r => r.SetEntryInHash("Test17", "Price", "123"));
                    trans.QueueCommand(
                        r => r.SetEntryInHash("Test18", "Price", "123"));
                    trans.QueueCommand(
                        r => r.SetEntryInHash("Test19", "Price", "123"));
                    trans.Commit();
                }
            }
        }

        private void CheckConnection(object state) {
            Task.Factory.StartNew(this.CheckThisConnection);
        }

        [Test]
        public void Can_Get_and_Remove_multiple_keys_in_same_transaction() {
            for (var i = 0; i < 5; i++) {
                this.Redis.Set("foo" + i, i);
            }

            List<string> keys = this.Redis.SearchKeys("foo*");
            Assert.That(keys, Has.Count.EqualTo(5));

            var dict = new Dictionary<string, int>();
            using (IRedisTransaction transaction = this.Redis.CreateTransaction()) {
                foreach (var key in keys) {
                    var y = key;
                    transaction.QueueCommand(x => x.Get<int>(y), val => dict.Add(y, val));
                }

                transaction.QueueCommand(x => x.RemoveAll(keys));
                transaction.Commit();
            }

            Assert.That(dict, Has.Count.EqualTo(5));
            keys = this.Redis.SearchKeys("foo*");
            Assert.That(keys, Has.Count.EqualTo(0));
        }

        [Test]
        public void Can_GetValues_and_Remove_multiple_keys_in_same_transaction() {
            for (var i = 0; i < 5; i++) {
                this.Redis.Set("foo" + i, i);
            }

            List<string> keys = this.Redis.SearchKeys("foo*");
            Assert.That(keys, Has.Count.EqualTo(5));

            var values = new List<string>();
            using (IRedisTransaction transaction = this.Redis.CreateTransaction()) {
                transaction.QueueCommand(x => x.GetValues(keys), val => values = val);
                transaction.QueueCommand(x => x.RemoveAll(keys));
                transaction.Commit();
            }

            Assert.That(values, Has.Count.EqualTo(5));
            keys = this.Redis.SearchKeys("foo*");
            Assert.That(keys, Has.Count.EqualTo(0));
        }

        [Test]
        public void Can_queue_large_transaction() {
            var q = new Timer(this.CheckConnection, null, 30000, 2);
            Thread.Sleep(30000);
        }

    }

}
