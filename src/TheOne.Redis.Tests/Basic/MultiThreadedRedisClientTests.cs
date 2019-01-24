using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Common;
using TheOne.Redis.Tests.Shared;

namespace TheOne.Redis.Tests.Basic {

    [TestFixture]
    internal sealed class MultiThreadedRedisClientTests : RedisTestBase {

        private static string _testData;

        [OneTimeSetUp]
        public void OnBeforeTestFixture() {
            var
                results = Enumerable.Range(0, 100).Select(ModelWithFieldsOfDifferentTypes.Create).ToList();

            _testData = results.ToJson();
        }

        private void UseClientAsync(RedisClient client, int clientNo) {
            lock (this) {
                UseClient(client, clientNo);
            }
        }

        private static void UseClient(RedisClient client, int clientNo) {
            var host = "";

            try {
                host = client.Host;

                Console.WriteLine("Client '{0}' is using '{1}'", clientNo, client.Host);

                var testClientKey = "test:" + host + ":" + clientNo;
                client.SetValue(testClientKey, _testData);
                var result = client.GetValue(testClientKey) ?? "";

                Console.WriteLine("\t{0} => {1} len {2} {3} len",
                    testClientKey,
                    _testData.Length,
                    _testData.Length == result.Length ? "==" : "!=",
                    result.Length);

            } catch (NullReferenceException ex) {
                Console.WriteLine("NullReferenceException StackTrace: \n" + ex.StackTrace);
            } catch (Exception ex) {
                Console.WriteLine("\t[ERROR@{0}]: {1} => {2}", host, ex.GetType().Name, ex.Message);
            }
        }

        [Test]
        public void Can_support_64_threads_using_the_client_sequentially() {
            var before = Stopwatch.GetTimestamp();

            const int noOfConcurrentClients = 64;

            using (var redisClient = new RedisClient(Config.MasterHost)) {
                for (var i = 0; i < noOfConcurrentClients; i++) {
                    var clientNo = i;
                    UseClient(redisClient, clientNo);
                }
            }

            Console.WriteLine("Time Taken: {0}", (Stopwatch.GetTimestamp() - before) / 1000);
        }

        [Test]
        public void Can_support_64_threads_using_the_client_simultaneously() {
            var before = Stopwatch.GetTimestamp();

            const int noOfConcurrentClients = 64;
            using (var redisClient = new RedisClient(Config.MasterHost)) {
                var tasks = new List<Task>();

                for (var i = 0; i < noOfConcurrentClients; i++) {
                    var clientNo = i;
                    var item = Task.Run(() => this.UseClientAsync(redisClient, clientNo));
                    tasks.Add(item);

                }

                Task.WaitAll(tasks.ToArray());
            }

            Console.WriteLine("Time Taken: {0}", (Stopwatch.GetTimestamp() - before) / 1000);
        }

    }

}
