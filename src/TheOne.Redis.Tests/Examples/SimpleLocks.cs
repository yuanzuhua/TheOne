using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using TheOne.Redis.Client;

namespace TheOne.Redis.Tests.Examples {

    [TestFixture]
    internal sealed class SimpleLocks {

        [SetUp]
        public void OnTestFixtureSetUp() {
            using (var redisClient = new RedisClient(Config.MasterHost)) {
                redisClient.FlushAll();
            }
        }

        [Test]
        public void AcquireLock_using_Tasks() {
            const int noOfClients = 4;
            var tasks = new Task[noOfClients];
            for (var i = 0; i < noOfClients; i++) {
                Thread.Sleep(2000);
                tasks[i] = Task.Factory.StartNew(clientNo => {
                        try {
                            Console.WriteLine("About to process " + clientNo);
                            var redisClient = new RedisClient(Config.MasterHost, 6379);

                            using (redisClient.AcquireLock("testlock1", TimeSpan.FromMinutes(3))) {
                                Console.WriteLine("client {0} acquired lock", (int)clientNo);
                                var counter = redisClient.Get<int>("atomic-counter");

                                // Add an artificial delay to demonstrate locking behaviour
                                Thread.Sleep(100);

                                redisClient.Set("atomic-counter", counter + 1);
                                Console.WriteLine("client {0} released lock", (int)clientNo);
                            }
                        } catch (Exception ex) {
                            Console.WriteLine(ex.Message);
                        }

                    },
                    i + 1);
            }
        }

        [Test]
        public void Acquiring_lock_with_timeout() {
            var redisClient = new RedisClient(Config.MasterHost);

            // Initialize and set counter to '1'
            redisClient.IncrementValue("atomic-counter");

            // Acquire lock and never release it
            redisClient.AcquireLock("testlock");

            var waitFor = TimeSpan.FromSeconds(2);
            var now = DateTime.Now;

            try {
                using (var newClient = new RedisClient(Config.MasterHost)) {
                    // Attempt to acquire a lock with a 2 second timeout
                    using (newClient.AcquireLock("testlock", waitFor)) {
                        // If lock was acquired this would be incremented to '2'
                        redisClient.IncrementValue("atomic-counter");
                    }
                }
            } catch (TimeoutException tex) {
                var timeTaken = DateTime.Now - now;
                Console.WriteLine("After '{0}', Received TimeoutException: '{1}'", timeTaken, tex.Message);

                var counter = redisClient.Get<int>("atomic-counter");
                Console.WriteLine("atomic-counter remains at '{0}'", counter);
            }
        }

        [Test]
        public void SimulateLockTimeout() {
            var redisClient = new RedisClient(Config.MasterHost);
            var waitFor = TimeSpan.FromMilliseconds(20);

            var loc = redisClient.AcquireLock("testlock", waitFor);
            Thread.Sleep(100); // should have lock expire
            using (var newloc = redisClient.AcquireLock("testlock", waitFor)) { }
        }

        [Test]
        public void Use_multiple_redis_clients_to_safely_execute() {
            // The number of concurrent clients to run
            const int noOfClients = 5;
            var asyncResults = new List<Task>(noOfClients);
            for (var i = 1; i <= noOfClients; i++) {
                var clientNo = i;

                // Asynchronously
                var item = Task.Run(() => {
                    var redisClient = new RedisClient(Config.MasterHost);
                    using (redisClient.AcquireLock("testlock")) {
                        Console.WriteLine("client {0} acquired lock", clientNo);
                        var counter = redisClient.Get<int>("atomic-counter");

                        // Add an artificial delay to demonstrate locking behaviour
                        Thread.Sleep(100);

                        redisClient.Set("atomic-counter", counter + 1);
                        Console.WriteLine("client {0} released lock", clientNo);
                    }
                });

                asyncResults.Add(item);
            }

            // Wait at most 1 second for all the threads to complete
            Task.WaitAll(asyncResults.ToArray(), TimeSpan.FromSeconds(1));

            // Print out the 'atomic-counter' result
            using (var redisClient = new RedisClient(Config.MasterHost)) {
                var counter = redisClient.Get<int>("atomic-counter");
                Console.WriteLine("atomic-counter after 1sec: {0}", counter);
            }
        }

    }


}
