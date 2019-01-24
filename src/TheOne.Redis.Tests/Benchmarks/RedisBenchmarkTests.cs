using System;
using System.Diagnostics;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Common;

namespace TheOne.Redis.Tests.Benchmarks {

    [TestFixture]
    internal sealed class RedisBenchmarkTests : RedisClientTestsBase {

        [Test]
        public void Compare_sort_nosort_to_smembers_mget() {
            var setKey = "setKey";
            var total = 25;
            var count = 20;
            var temp = new byte[1];
            byte fixedValue = 124;
            temp[0] = fixedValue;

            // initialize set and individual keys
            this.Redis.Del(setKey);
            for (var i = 0; i < total; ++i) {
                var key = setKey + i;
                this.Redis.SAdd(setKey, key.ToUtf8Bytes());
                this.Redis.Set(key, temp);
            }

            var sw = Stopwatch.StartNew();

            var results = Array.Empty<byte[]>();
            for (var i = 0; i < count; ++i) {
                var keys = this.Redis.SMembers(setKey);
                results = this.Redis.MGet(keys);
            }

            sw.Stop();

            // make sure that results are valid
            foreach (var result in results) {
                Assert.AreEqual(result[0], fixedValue);
            }

            Console.WriteLine("Time to call {0} SMembers and MGet operations: {1} ms", count, sw.ElapsedMilliseconds);
            var opt = new SortOptions { SortPattern = "nosort", GetPattern = "*" };

            sw = Stopwatch.StartNew();
            for (var i = 0; i < count; ++i) {
                results = this.Redis.Sort(setKey, opt);
            }

            sw.Stop();

            // make sure that results are valid
            foreach (var result in results) {
                Assert.AreEqual(result[0], fixedValue);
            }

            Console.WriteLine("Time to call {0} sort operations: {1} ms", count, sw.ElapsedMilliseconds);
        }

        [Test]
        public void Measure_pipeline_speedup() {
            var key = "key";
            var total = 500;
            var temp = new byte[1];
            for (var i = 0; i < total; ++i) {
                this.Redis.Del(key + i);
            }

            var sw = Stopwatch.StartNew();
            for (var i = 0; i < total; ++i) {
                ((RedisNativeClient)this.Redis).Set(key + i, temp);
            }

            sw.Stop();
            Console.WriteLine("Time for {0} Set(key,value) operations: {1} ms", total, sw.ElapsedMilliseconds);

            for (var i = 0; i < total; ++i) {
                this.Redis.Del(key + i);
            }

            sw = Stopwatch.StartNew();
            using (var pipeline = this.Redis.CreatePipeline()) {
                for (var i = 0; i < total; ++i) {
                    pipeline.QueueCommand(r => ((RedisNativeClient)this.Redis).Set(key + i.ToString(), temp));
                }

                pipeline.Flush();

            }

            sw.Stop();
            Console.WriteLine("Time for pipelining {0} Set(key,value) operations: {1} ms", total, sw.ElapsedMilliseconds);
        }

    }

}
