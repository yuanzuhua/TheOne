using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.ClientManager;
using TheOne.Redis.Common;
using TheOne.Redis.Tests.Shared;

namespace TheOne.Redis.Tests.Basic {

    [TestFixture]
    internal sealed class RedisRegressionTestRun : RedisTestBase {

        private static string _testData;

        [OneTimeSetUp]
        public void OneTimeSetUp() {
            var
                results = Enumerable.Range(0, 100).Select(ModelWithFieldsOfDifferentTypes.Create).ToList();
            _testData = results.ToJson();
        }

        private static void UseClientAsync(IRedisClientManager manager, int clientNo) {
            using (var client = manager.GetClient()) {
                UseClient(client, clientNo);
            }
        }

        private static void UseClient(IRedisClient client, int clientNo) {
            var host = "";

            try {
                host = client.Host;

                Console.WriteLine("Client '{0}' is using '{1}'", clientNo, client.Host);
                var differentDbs = new[] { 1, 0, 2 };

                foreach (var db in differentDbs) {
                    client.Db = db;

                    var testClientKey = "test:" + host + ":" + clientNo;
                    client.SetValue(testClientKey, _testData);
                    var result = client.GetValue(testClientKey) ?? "";
                    LogResult(db, testClientKey, result);

                    var testClientSetKey = "test+set:" + host + ":" + clientNo;
                    client.AddItemToSet(testClientSetKey, _testData);
                    var resultSet = client.GetAllItemsFromSet(testClientSetKey);
                    LogResult(db, testClientKey, resultSet.ToList().FirstOrDefault());

                    var testClientListKey = "test+list:" + host + ":" + clientNo;
                    client.AddItemToList(testClientListKey, _testData);
                    var resultList = client.GetAllItemsFromList(testClientListKey);
                    LogResult(db, testClientKey, resultList.FirstOrDefault());
                }
            } catch (NullReferenceException ex) {
                Console.WriteLine("NullReferenceException StackTrace: \n" + ex.StackTrace);
                Assert.Fail("NullReferenceException");
            } catch (Exception ex) {
                Console.WriteLine("\t[ERROR@{0}]: {1} => {2}", host, ex.GetType().Name, ex);
                Assert.Fail("Exception");
            }
        }

        private static void LogResult(int db, string testClientKey, string resultData) {
            if (string.IsNullOrEmpty(resultData)) {
                Console.WriteLine("\tERROR@[{0}] NULL", db);
                return;
            }

            Console.WriteLine("\t[{0}] {1} => {2} len {3} {4} len",
                db,
                testClientKey,
                _testData.Length,
                _testData.Length == resultData.Length ? "==" : "!=",
                resultData.Length);
        }

        [Test]
        public void Can_run_series_of_operations_sequentially() {
            var before = Stopwatch.GetTimestamp();

            const int noOfConcurrentClients = 64;

            using (var redisClient = new RedisClient(Config.MasterHost)) {
                redisClient.FlushAll();

                for (var i = 0; i < noOfConcurrentClients; i++) {
                    var clientNo = i;
                    UseClient(redisClient, clientNo);
                }
            }

            Console.WriteLine("Completed in {0} ticks", Stopwatch.GetTimestamp() - before);
        }

        [Test]
        public void Can_support_64_threads_using_the_client_simultaneously() {
            var before = Stopwatch.GetTimestamp();

            const int noOfConcurrentClients = 64;

            using (var manager = new PooledRedisClientManager(Config.MasterHost)) {
                var clientAsyncResults = new List<Task>();
                using (var client = manager.GetClient()) { client.FlushAll(); }

                for (var i = 0; i < noOfConcurrentClients; i++) {
                    var clientNo = i;
                    var item = Task.Run(() => UseClientAsync(manager, clientNo));
                    clientAsyncResults.Add(item);
                }

                Task.WaitAll(clientAsyncResults.ToArray());
            }

            Console.WriteLine("Completed in {0} ticks", Stopwatch.GetTimestamp() - before);

            Console.WriteLine(RedisStats.ToDictionary().ToJson());
        }

    }

}
