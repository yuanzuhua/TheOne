using System;
using System.Diagnostics;
using NUnit.Framework;
using TheOne.Redis.Client;

namespace TheOne.Redis.Tests.Basic {

    [TestFixture]
    internal sealed class DiagnosticTests : RedisTestBase {

        private const int _messageSizeBytes = 1024 * 1024;
        private const int _count = 10;

        private byte[] RandomBytes(int length) {
            var rnd = new Random();
            var bytes = new byte[length];
            for (long i = 0; i < length; i++) {
                bytes[i] = (byte)rnd.Next(254);
            }

            return bytes;
        }

        [Test]
        public void Test_Throughput() {
            byte[] bytes = this.RandomBytes(_messageSizeBytes);
            Stopwatch swTotal = Stopwatch.StartNew();

            var key = "test:bandwidth:" + bytes.Length;

            var bytesSent = 0;
            var bytesRecv = 0;

            using (var redisClient = new RedisNativeClient(Config.MasterHost)) {
                for (var i = 0; i < _count; i++) {
                    Stopwatch sw = Stopwatch.StartNew();
                    redisClient.Set(key, bytes);
                    bytesSent += bytes.Length;
                    Console.WriteLine("SEND {0} bytes in {1}ms", bytes.Length, sw.ElapsedMilliseconds);

                    sw.Reset();
                    sw.Start();
                    byte[] receivedBytes = redisClient.Get(key);
                    bytesRecv += receivedBytes.Length;
                    Console.WriteLine("RECV {0} bytes in {1}ms", receivedBytes.Length, sw.ElapsedMilliseconds);
                    Console.WriteLine("TOTAL {0} bytes SENT {0} RECV {1} in {2}ms\n", bytesSent, bytesRecv, swTotal.ElapsedMilliseconds);
                }
            }
        }

    }

}
