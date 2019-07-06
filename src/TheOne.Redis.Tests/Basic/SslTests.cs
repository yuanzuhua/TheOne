using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.ClientManager;
using TheOne.Redis.Common;
using TheOne.Redis.Tests.Shared;

namespace TheOne.Redis.Tests.Basic {

    [Ignore("ssl")]
    [TestFixture]
    internal sealed class SslTests {

        private readonly byte[] _endData = { (byte)'\r', (byte)'\n' };
        private string _connectionString;

        private string _host;
        private string _password;
        private int _port;

        [OneTimeSetUp]
        public void OneTimeSetUp() {
            this._host = Config.Localhost;
            this._port = Config.LocalhostPort;
            this._password = Config.LocalhostPassword;
            this._connectionString = string.Format("{0}@{1}", this._password, this._host);
        }


        private void SendAuth(Stream stream) {
            this.WriteAllToStream(stream, "AUTH".ToUtf8Bytes(), this._password.ToUtf8Bytes());
            this.ExpectSuccess(stream);
        }

        public void WriteAllToStream(Stream stream, params byte[][] cmdWithBinaryArgs) {
            this.WriteToStream(stream, GetCmdBytes('*', cmdWithBinaryArgs.Length));

            foreach (var safeBinaryValue in cmdWithBinaryArgs) {
                this.WriteToStream(stream, GetCmdBytes('$', safeBinaryValue.Length));
                this.WriteToStream(stream, safeBinaryValue);
                this.WriteToStream(stream, this._endData);
            }

            stream.Flush();
        }

        public void WriteToStream(Stream stream, byte[] bytes) {
            stream.Write(bytes, 0, bytes.Length);
        }

        private static byte[] GetCmdBytes(char cmdPrefix, int noOfLines) {
            var strLines = noOfLines.ToString();
            var strLinesLength = strLines.Length;

            var cmdBytes = new byte[1 + strLinesLength + 2];
            cmdBytes[0] = (byte)cmdPrefix;

            for (var i = 0; i < strLinesLength; i++) {
                cmdBytes[i + 1] = (byte)strLines[i];
            }

            cmdBytes[1 + strLinesLength] = 0x0D; // \r
            cmdBytes[2 + strLinesLength] = 0x0A; // \n

            return cmdBytes;
        }

        private void ExpectSuccess(Stream stream) {
            var c = stream.ReadByte();
            if (c == -1) {
                throw new RedisRetryableException("No more data");
            }

            var s = this.ReadLine(stream);
            Console.WriteLine(s);

            if (c == '-') {
                throw new Exception(s.StartsWith("ERR") && s.Length >= 4 ? s.Substring(4) : s);
            }
        }

        private string ReadLine(Stream stream) {
            var sb = new StringBuilder();

            int c;
            while ((c = stream.ReadByte()) != -1) {
                if (c == '\r') {
                    continue;
                }

                if (c == '\n') {
                    break;
                }

                sb.Append((char)c);
            }

            return sb.ToString();
        }

        private static void Log(string fmt, params object[] args) {
            Console.WriteLine(fmt, args);
        }

        private static void UseClientAsync(IRedisClientManager manager, int clientNo, string testData) {
            using (var client = manager.GetReadOnlyClient()) {
                UseClient(client, clientNo, testData);
            }
        }

        private static void UseClient(IRedisClient client, int clientNo, string testData) {
            var host = "";

            try {
                host = client.Host;

                Log("Client '{0}' is using '{1}'", clientNo, client.Host);

                var testClientKey = "test:" + host + ":" + clientNo;
                client.SetValue(testClientKey, testData);
                var result = client.GetValue(testClientKey) ?? "";

                Log("\t{0} => {1} len {2} {3} len",
                    testClientKey,
                    testData.Length,
                    testData.Length == result.Length ? "==" : "!=",
                    result.Length);
            } catch (NullReferenceException ex) {
                Console.WriteLine("NullReferenceException StackTrace: \n" + ex.StackTrace);
                Assert.Fail(ex.Message);
            } catch (Exception ex) {
                Console.WriteLine("\t[ERROR@{0}]: {1} => {2}", host, ex.GetType().Name, ex.Message);
                Assert.Fail(ex.Message);
            }
        }

        [Test]
        public void Can_connect_to_azure_redis() {
            using (var client = new RedisClient(this._connectionString)) {
                client.Set("foo", "bar");
                var foo = client.GetValue("foo");
                Console.WriteLine(foo);
            }
        }

        [Test]
        public void Can_connect_to_Buffered_SslStream() {
            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp) {
                SendTimeout = -1,
                ReceiveTimeout = -1
            };

            socket.Connect(this._host, this._port);

            if (!socket.Connected) {
                socket.Close();
                throw new Exception("Could not connect");
            }

            Stream networkStream = new NetworkStream(socket);

            var sslStream = new SslStream(networkStream,
                false,
                RedisConfig.CertificateValidationCallback,
                RedisConfig.CertificateSelectionCallback,
                EncryptionPolicy.RequireEncryption);

            sslStream.AuthenticateAsClientAsync(this._host).Wait();

            if (!sslStream.IsEncrypted) {
                throw new Exception("Could not establish an encrypted connection to " + this._host);
            }

            var bStream = new BufferedStream(sslStream, 16 * 1024);

            this.SendAuth(bStream);
        }

        [Test]
        public void Can_connect_to_NetworkStream() {
            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp) {
                SendTimeout = -1,
                ReceiveTimeout = -1
            };

            socket.Connect(this._host, 6379);

            if (!socket.Connected) {
                socket.Close();
                throw new Exception("Could not connect");
            }

            Stream networkStream = new NetworkStream(socket);

            this.SendAuth(networkStream);
        }

        [Test]
        public void Can_connect_to_ssl_azure_redis() {
            using (var client = new RedisClient(this._connectionString)) {
                client.Set("foo", "bar");
                var foo = client.GetValue("foo");
                Console.WriteLine(foo);
            }
        }

        [Test]
        public void Can_connect_to_ssl_azure_redis_with_PooledClientManager() {
            using (var redisManager = new PooledRedisClientManager(this._connectionString)) {
                using (var client1 = redisManager.GetClient()) {
                    using (var client2 = redisManager.GetClient()) {
                        client1.Set("foo", "bar");
                        var foo = client2.GetValue("foo");
                        Console.WriteLine(foo);
                    }
                }
            }
        }

        [Test]
        public void Can_connect_to_ssl_azure_redis_with_UrlFormat() {
            var url = string.Format("redis://{0}?ssl=true&password={1}", this._host, WebUtility.UrlEncode(this._password));
            using (var client = new RedisClient(url)) {
                client.Set("foo", "bar");
                var foo = client.GetValue("foo");
                Console.WriteLine(foo);
            }
        }

        [Test]
        public void Can_connect_to_ssl_azure_redis_with_UrlFormat_Custom_SSL_Protocol() {
            var url = string.Format("redis://{0}?ssl=true&sslprotocols=Tls12&password={1}",
                this._host,
                WebUtility.UrlEncode(this._password));
            using (var client = new RedisClient(url)) {
                client.Set("foo", "bar");
                var foo = client.GetValue("foo");
                Console.WriteLine(foo);
            }
        }

        [Test]
        public void SSL_can_support_64_threads_using_the_client_sequentially() {
            var
                results = Enumerable.Range(0, 100).Select(ModelWithFieldsOfDifferentTypes.Create).ToList();
            var testData = results.ToJson();

            var before = Stopwatch.GetTimestamp();

            const int noOfConcurrentClients = 64;

            using (var redisClient = new RedisClient(this._connectionString)) {
                for (var i = 0; i < noOfConcurrentClients; i++) {
                    var clientNo = i;
                    UseClient(redisClient, clientNo, testData);
                }
            }

            Console.WriteLine("Time Taken: {0}", (Stopwatch.GetTimestamp() - before) / 1000);
        }

        [Test]
        public void SSL_can_support_64_threads_using_the_client_simultaneously() {
            var
                results = Enumerable.Range(0, 100).Select(ModelWithFieldsOfDifferentTypes.Create).ToList();
            var testData = results.ToJson();

            var before = Stopwatch.GetTimestamp();

            const int noOfConcurrentClients = 64;

            var clientAsyncResults = new List<Task>();
            using (var manager = new PooledRedisClientManager(Config.MasterHosts, Config.SlaveHosts)) {
                using (var client = manager.GetClient()) { client.FlushAll(); }

                for (var i = 0; i < noOfConcurrentClients; i++) {
                    var clientNo = i;
                    var item = Task.Run(() => UseClientAsync(manager, clientNo, testData));
                    clientAsyncResults.Add(item);
                }
            }

            Task.WaitAll(clientAsyncResults.ToArray());

            Console.WriteLine("Completed in {0} ticks", Stopwatch.GetTimestamp() - before);
        }

    }

}
