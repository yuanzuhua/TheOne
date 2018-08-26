using System;
using System.Diagnostics;
using NUnit.Framework;
using TheOne.Redis.Client;

namespace TheOne.Redis.Tests.Benchmarks {

    [TestFixture]
    internal sealed class RawBytesSetBenchmark {

        public void Run(string name, int nBlockSizeBytes, Action<int, byte[]> fn) {
            var nBytesHandled = 0;
            var nMaxIterations = 5;
            var pBuffer = new byte[nBlockSizeBytes];

            // Create Redis Wrapper
            var redis = new RedisNativeClient(Config.MasterHost);

            // Clear DB
            redis.FlushAll();

            Stopwatch sw = Stopwatch.StartNew();
            var ms1 = sw.ElapsedMilliseconds;
            for (var i = 0; i < nMaxIterations; i++) {
                fn(i, pBuffer);
                nBytesHandled += nBlockSizeBytes;
            }

            var ms2 = sw.ElapsedMilliseconds;
            var interval = ms2 - ms1;

            // Calculate rate mb/s
            var rate = nBytesHandled / 1024.0 / 1024.0 / (interval / 1000.0);
            Console.WriteLine(name + ": Rate {0:N4}, Total: {1}ms", rate, ms2);
        }

        [Test]
        public void Benchmark_SET_raw_bytes_100k() {
            var redis = new RedisNativeClient(Config.MasterHost);

            this.Run("ServiceStack.Redis 100K",
                100000,
                (i, bytes) => redis.Set("eitan" + i.ToString(), bytes));
        }

        [Test]
        public void Benchmark_SET_raw_bytes_10k() {
            var redis = new RedisNativeClient(Config.MasterHost);

            this.Run("ServiceStack.Redis 10K",
                10000,
                (i, bytes) => redis.Set("eitan" + i.ToString(), bytes));
        }

        [Test]
        public void Benchmark_SET_raw_bytes_1k() {
            var redis = new RedisNativeClient(Config.MasterHost);

            this.Run("ServiceStack.Redis 1K",
                1000,
                (i, bytes) => redis.Set("eitan" + i.ToString(), bytes));
        }

        [Test]
        public void Benchmark_SET_raw_bytes_1MB() {
            var redis = new RedisNativeClient(Config.MasterHost);

            this.Run("ServiceStack.Redis 1MB",
                1000000,
                (i, bytes) => redis.Set("eitan" + i.ToString(), bytes));
        }

        [Test]
        public void Benchmark_SET_raw_bytes_8MB() {
            var redis = new RedisNativeClient(Config.MasterHost);

            this.Run("ServiceStack.Redis 8MB",
                8000000,
                (i, bytes) => redis.Set("eitan" + i.ToString(), bytes));
        }

    }

}
