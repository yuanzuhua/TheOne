using System;
using System.Text;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Common;
using TheOne.Redis.Tests.Extensions;

namespace TheOne.Redis.Tests.Client {

    [TestFixture]
    internal sealed class AdhocClientTests : RedisTestBase {

        private static byte[] GetCmdBytes1(char cmdPrefix, int noOfLines) {
            var cmd = cmdPrefix + noOfLines.ToString() + "\r\n";
            return cmd.ToUtf8Bytes();
        }

        private static byte[] GetCmdBytes2(char cmdPrefix, int noOfLines) {
            var strLines = noOfLines.ToString();
            var cmdBytes = new byte[1 + strLines.Length + 2];
            cmdBytes[0] = (byte)cmdPrefix;

            for (var i = 0; i < strLines.Length; i++) {
                cmdBytes[i + 1] = (byte)strLines[i];
            }

            cmdBytes[cmdBytes.Length - 2] = 0x0D; // \r
            cmdBytes[cmdBytes.Length - 1] = 0x0A; // \n

            return cmdBytes;
        }

        [Test]
        public void Can_infer_utf8_bytes() {
            var cmd = "GET" + 2 + "\r\n";
            byte[] cmdBytes = Encoding.UTF8.GetBytes(cmd);

            var hex = BitConverter.ToString(cmdBytes);

            Console.WriteLine(hex);

            Console.WriteLine(BitConverter.ToString("G".ToUtf8Bytes()));
            Console.WriteLine(BitConverter.ToString("E".ToUtf8Bytes()));
            Console.WriteLine(BitConverter.ToString("T".ToUtf8Bytes()));
            Console.WriteLine(BitConverter.ToString("2".ToUtf8Bytes()));
            Console.WriteLine(BitConverter.ToString("\r".ToUtf8Bytes()));
            Console.WriteLine(BitConverter.ToString("\n".ToUtf8Bytes()));

            var bytes = new[] { (byte)'\r', (byte)'\n', (byte)'0', (byte)'9' };
            Console.WriteLine(BitConverter.ToString(bytes));
        }

        [Test]
        public void Compare_GetCmdBytes() {
            byte[] res1 = GetCmdBytes1('$', 1234);
            byte[] res2 = GetCmdBytes2('$', 1234);

            Console.WriteLine(BitConverter.ToString(res1));
            Console.WriteLine(BitConverter.ToString(res2));

            var ticks1 = PerfUtils.Measure(() => GetCmdBytes1('$', 2));
            var ticks2 = PerfUtils.Measure(() => GetCmdBytes2('$', 2));

            Console.WriteLine("{0} : {1} = {2}", ticks1, ticks2, ticks1 / ticks2);
        }

        [Test]
        public void Convert_int() {
            Console.WriteLine(BitConverter.ToString(1234.ToString().ToUtf8Bytes()));
        }

        [Test]
        public void Search_Test() {
            using (var client = new RedisClient(Config.MasterHost)) {
                const string cacheKey = "urn+metadata:All:SearchProProfiles?SwanShinichi Osawa /0/8,0,0,0";
                const long value = 1L;
                client.Set(cacheKey, value);
                var result = client.Get<long>(cacheKey);

                Assert.That(result, Is.EqualTo(value));
            }
        }

    }

}
