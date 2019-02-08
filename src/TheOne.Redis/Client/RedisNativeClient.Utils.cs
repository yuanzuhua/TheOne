using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using TheOne.Logging;
using TheOne.Redis.Client.Internal;
using TheOne.Redis.Common;
using TheOne.Redis.External;

namespace TheOne.Redis.Client {

    public partial class RedisNativeClient {

        private const string _ok = "OK";
        private const string _queued = "QUEUED";
        private const int _unknown = -1;
        public static string ErrorConnect = "Could not connect to redis Instance at {0}:{1}";

        private static long _idCounter;
        private readonly IList<ArraySegment<byte>> _cmdBuffer = new List<ArraySegment<byte>>();
        private byte[] _currentBuffer = BufferPool.GetBuffer();
        private int _currentBufferIndex;
        private string _logPrefix = string.Empty;
        internal TrackThread? TrackThread;

        public static int ServerVersionNumber { get; set; }
        public long ClientId { get; } = Interlocked.Increment(ref _idCounter);

        public bool HasConnected => this.Socket != null;
        public Action OnBeforeFlush { get; set; }

        /// <inheritdoc />
        public long EvalInt(string luaBody, int numberKeysInArgs, params byte[][] keys) {
            if (luaBody == null) {
                throw new ArgumentNullException(nameof(luaBody));
            }

            var cmdArgs = MergeCommandWithArgs(Commands.Eval, luaBody.ToUtf8Bytes(), keys.PrependInt(numberKeysInArgs));
            return this.SendExpectLong(cmdArgs);
        }

        /// <inheritdoc />
        public long EvalShaInt(string sha1, int numberKeysInArgs, params byte[][] keys) {
            if (sha1 == null) {
                throw new ArgumentNullException(nameof(sha1));
            }

            var cmdArgs = MergeCommandWithArgs(Commands.EvalSha, sha1.ToUtf8Bytes(), keys.PrependInt(numberKeysInArgs));
            return this.SendExpectLong(cmdArgs);
        }

        /// <inheritdoc />
        public string EvalStr(string luaBody, int numberKeysInArgs, params byte[][] keys) {
            if (luaBody == null) {
                throw new ArgumentNullException(nameof(luaBody));
            }

            var cmdArgs = MergeCommandWithArgs(Commands.Eval, luaBody.ToUtf8Bytes(), keys.PrependInt(numberKeysInArgs));
            return this.SendExpectData(cmdArgs).FromUtf8Bytes();
        }

        /// <inheritdoc />
        public string EvalShaStr(string sha1, int numberKeysInArgs, params byte[][] keys) {
            if (sha1 == null) {
                throw new ArgumentNullException(nameof(sha1));
            }

            var cmdArgs = MergeCommandWithArgs(Commands.EvalSha, sha1.ToUtf8Bytes(), keys.PrependInt(numberKeysInArgs));
            return this.SendExpectData(cmdArgs).FromUtf8Bytes();
        }

        /// <inheritdoc />
        public byte[][] Eval(string luaBody, int numberKeysInArgs, params byte[][] keys) {
            if (luaBody == null) {
                throw new ArgumentNullException(nameof(luaBody));
            }

            var cmdArgs = MergeCommandWithArgs(Commands.Eval, luaBody.ToUtf8Bytes(), keys.PrependInt(numberKeysInArgs));
            return this.SendExpectMultiData(cmdArgs);
        }

        /// <inheritdoc />
        public byte[][] EvalSha(string sha1, int numberKeysInArgs, params byte[][] keys) {
            if (sha1 == null) {
                throw new ArgumentNullException(nameof(sha1));
            }

            var cmdArgs = MergeCommandWithArgs(Commands.EvalSha, sha1.ToUtf8Bytes(), keys.PrependInt(numberKeysInArgs));
            return this.SendExpectMultiData(cmdArgs);
        }

        /// <inheritdoc />
        public RedisData EvalShaCommand(string sha1, int numberKeysInArgs, params byte[][] keys) {
            if (sha1 == null) {
                throw new ArgumentNullException(nameof(sha1));
            }

            var cmdArgs = MergeCommandWithArgs(Commands.EvalSha, sha1.ToUtf8Bytes(), keys.PrependInt(numberKeysInArgs));
            return this.RawCommand(cmdArgs);
        }

        /// <inheritdoc />
        public string CalculateSha1(string luaBody) {
            if (luaBody == null) {
                throw new ArgumentNullException(nameof(luaBody));
            }

            var buffer = luaBody.ToUtf8Bytes();
            byte[] sha1Hash;

            using (var sha1 = SHA1.Create()) {
                sha1Hash = sha1.ComputeHash(buffer);
            }

            return BitConverter.ToString(sha1Hash).Replace("-", "");
        }

        /// <inheritdoc />
        public byte[] ScriptLoad(string luaBody) {
            if (luaBody == null) {
                throw new ArgumentNullException(nameof(luaBody));
            }

            var cmdArgs = MergeCommandWithArgs(Commands.Script, Commands.Load, luaBody.ToUtf8Bytes());
            return this.SendExpectData(cmdArgs);
        }

        /// <inheritdoc />
        public byte[][] ScriptExists(params byte[][] sha1Refs) {
            var keysAndValues = MergeCommandWithArgs(Commands.Script, Commands.Exists, sha1Refs);
            return this.SendExpectMultiData(keysAndValues);
        }

        /// <inheritdoc />
        public void ScriptFlush() {
            this.SendExpectSuccess(Commands.Script, Commands.Flush);
        }

        /// <inheritdoc />
        public void ScriptKill() {
            this.SendExpectSuccess(Commands.Script, Commands.Kill);
        }

        public RedisData EvalCommand(string luaBody, int numberKeysInArgs, params byte[][] keys) {
            if (luaBody == null) {
                throw new ArgumentNullException(nameof(luaBody));
            }

            var cmdArgs = MergeCommandWithArgs(Commands.Eval, luaBody.ToUtf8Bytes(), keys.PrependInt(numberKeysInArgs));
            return this.RawCommand(cmdArgs);
        }

        public int AssertServerVersionNumber() {
            if (ServerVersionNumber == 0) {
                this.AssertConnectedSocket();
            }

            return ServerVersionNumber;
        }

        private void Connect() {
            this.Socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp) {
                SendTimeout = this.SendTimeout,
                ReceiveTimeout = this.ReceiveTimeout
            };
            try {
                if (IPAddress.TryParse(this.Host, out var ip)) {
                    this.Socket.Connect(ip, this.Port);
                } else {
                    var addresses = Dns.GetHostAddresses(this.Host);
                    this.Socket.Connect(addresses.First(a => a.AddressFamily == AddressFamily.InterNetwork), this.Port);
                }

                if (!this.Socket.Connected) {
                    _logger.Debug(this._logPrefix + "Socket Connect failed");
                    this.Socket.Close();
                    this.Socket = null;
                    this.DeactivatedAt = DateTime.UtcNow;
                    return;
                }

                _logger.Debug(this._logPrefix + "Socket Connected");

                Stream networkStream = new NetworkStream(this.Socket);

                if (this.Ssl) {
                    this.SslStream = new SslStream(networkStream,
                        false,
                        RedisConfig.CertificateValidationCallback,
                        RedisConfig.CertificateSelectionCallback,
                        EncryptionPolicy.RequireEncryption);

                    this.SslStream.AuthenticateAsClientAsync(this.Host).Wait();

                    if (!this.SslStream.IsEncrypted) {
                        throw new RedisException("Could not establish an encrypted connection to " + this.Host);
                    }

                    networkStream = this.SslStream;
                }

                this.BStream = new BufferedStream(networkStream, 16 * 1024);

                if (!string.IsNullOrEmpty(this.Password)) {
                    this.SendUnmanagedExpectSuccess(Commands.Auth, this.Password.ToUtf8Bytes());
                }

                if (this._db != 0) {
                    this.SendUnmanagedExpectSuccess(Commands.Select, this._db.ToUtf8Bytes());
                }

                if (this.Client != null) {
                    this.SendUnmanagedExpectSuccess(Commands.Client, Commands.SetName, this.Client.ToUtf8Bytes());
                }

                try {
                    if (ServerVersionNumber == 0) {
                        ServerVersionNumber = RedisConfig.AssumeServerVersion.GetValueOrDefault(0);
                        if (ServerVersionNumber <= 0) {
                            var parts = this.ServerVersion.Split('.');
                            var version = int.Parse(parts[0]) * 1000;
                            if (parts.Length > 1) {
                                version += int.Parse(parts[1]) * 100;
                            }

                            if (parts.Length > 2) {
                                version += int.Parse(parts[2]);
                            }

                            ServerVersionNumber = version;
                        }
                    }
                } catch {
                    // Twemproxy doesn't support the INFO command so automatically closes the socket
                    // Fallback to ServerVersionNumber=Unknown then try re-connecting
                    ServerVersionNumber = _unknown;
                    this.Connect();
                    return;
                }

                this._clientPort = this.Socket.LocalEndPoint is IPEndPoint ipEndpoint ? ipEndpoint.Port : -1;
                this._lastCommand = null;
                this._lastSocketException = null;
                this.LastConnectedAtTimestamp = Stopwatch.GetTimestamp();

                this.OnConnected();

                this.ConnectionFilter?.Invoke(this);
            } catch (SocketException ex) {
                _logger.Error(ex, this._logPrefix + ErrorConnect, this.Host, this.Port);
                throw;
            }
        }

        protected virtual void OnConnected() { }

        protected string ReadLine() {
            var sb = StringBuilderCache.Acquire();

            int c;
            while ((c = this.BStream.ReadByte()) != -1) {
                if (c == '\r') {
                    continue;
                }

                if (c == '\n') {
                    break;
                }

                sb.Append((char)c);
            }

            return StringBuilderCache.GetStringAndRelease(sb);
        }

        public bool IsSocketConnected() {
            if (this.Socket == null) {
                return false;
            }

            var part1 = this.Socket.Poll(1000, SelectMode.SelectRead);
            var part2 = this.Socket.Available == 0;
            return !(part1 & part2);
        }

        internal bool AssertConnectedSocket() {
            try {
                this.TryConnectIfNeeded();
                var isConnected = this.Socket != null;
                return isConnected;
            } catch (SocketException ex) {
                _logger.Error(this._logPrefix + ErrorConnect, this.Host, this.Port);

                this.Socket?.Close();
                this.Socket = null;

                this.DeactivatedAt = DateTime.UtcNow;
                var message = this.Host + ":" + this.Port;
                var throwEx = new RedisException(message, ex);

                _logger.Error(ex, this._logPrefix + throwEx.Message);

                throw throwEx;
            }
        }

        private bool TryConnectIfNeeded() {
            var didConnect = false;
            if (this.LastConnectedAtTimestamp > 0) {
                var now = Stopwatch.GetTimestamp();
                var elapsedSecs = (now - this.LastConnectedAtTimestamp) / Stopwatch.Frequency;

                if (this.Socket == null || elapsedSecs > this.IdleTimeoutSecs && !this.Socket.IsConnected()) {
                    this.Reconnect();
                    didConnect = true;
                }

                this.LastConnectedAtTimestamp = now;
            }

            if (this.Socket == null) {
                this.Connect();
                didConnect = true;
            }

            return didConnect;
        }

        private bool Reconnect() {
            this.SafeConnectionClose();
            this.Connect(); // sets db
            return this.Socket != null;
        }

        private RedisResponseException CreateResponseError(string error) {
            this.DeactivatedAt = DateTime.UtcNow;

            if (RedisConfig.EnableVerboseLogging) {
                var safeLastCommand = string.IsNullOrEmpty(this.Password)
                    ? this._lastCommand
                    : (this._lastCommand ?? "").Replace(this.Password, "");

                if (!string.IsNullOrEmpty(safeLastCommand)) {
                    error = $"{error}, LastCommand:'{safeLastCommand}', srcPort:{this._clientPort}";
                }
            }

            var throwEx = new RedisResponseException(error);

            _logger.Error(this._logPrefix + error);

            return throwEx;
        }

        private RedisRetryableException CreateNoMoreDataError() {
            this.Reconnect();
            return this.CreateRetryableResponseError("No more data");
        }

        private RedisRetryableException CreateRetryableResponseError(string error) {
            var safeLastCommand = string.IsNullOrEmpty(this.Password)
                ? this._lastCommand
                : (this._lastCommand ?? "").Replace(this.Password, "");

            var message = string.Format("[{0:HH:mm:ss.fff}] {1}, sPort: {2}, LastCommand: {3}",
                DateTime.UtcNow,
                error,
                this._clientPort,
                safeLastCommand);

            var throwEx = new RedisRetryableException(message);

            _logger.Error(this._logPrefix + message);

            throw throwEx;
        }

        private RedisException CreateConnectionError(Exception originalEx) {
            this.DeactivatedAt = DateTime.UtcNow;

            var message = string.Format("[{0:HH:mm:ss.fff}] Unable to Connect: sPort: {1}{2}",
                DateTime.UtcNow,
                this._clientPort,
                originalEx != null ? ", Error: " + originalEx.Message + "\n" + originalEx.StackTrace : "");

            var throwEx = new RedisException(message, originalEx ?? this._lastSocketException);

            _logger.Error(this._logPrefix + message);

            throw throwEx;
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

        /// <summary>
        ///     Command to set multiple binary safe arguments
        /// </summary>
        protected void WriteCommandToSendBuffer(params byte[][] cmdWithBinaryArgs) {
            if (this.Pipeline == null && this.Transaction == null) {
                // Interlocked.Increment(ref _requestsPerHour);
            }

            if (_logger.IsTraceEnabled() && RedisConfig.EnableVerboseLogging) {
                this.CmdLog(cmdWithBinaryArgs);
            }

            // Total command lines count
            this.WriteAllToSendBuffer(cmdWithBinaryArgs);
        }

        /// <summary>
        ///     Send command outside of managed Write Buffer
        /// </summary>
        protected void SendUnmanagedExpectSuccess(params byte[][] cmdWithBinaryArgs) {

            byte[] Combine(byte[] bytes1, params byte[][] withBytes1) {
                var combinedLength = bytes1.Length + withBytes1.Sum(b => b.Length);
                var to = new byte[combinedLength];

                Buffer.BlockCopy(bytes1, 0, to, 0, bytes1.Length);
                var pos = bytes1.Length;

                foreach (var b in withBytes1) {
                    Buffer.BlockCopy(b, 0, to, pos, b.Length);
                    pos += b.Length;
                }

                return to;
            }

            var bytes = GetCmdBytes('*', cmdWithBinaryArgs.Length);

            foreach (var safeBinaryValue in cmdWithBinaryArgs) {
                bytes = Combine(bytes, GetCmdBytes('$', safeBinaryValue.Length), safeBinaryValue, this._endData);
            }

            if (RedisConfig.EnableVerboseLogging) {
                var message = Encoding.UTF8.GetString(bytes, 0, Math.Min(bytes.Length, 50)).TrimNewLine();
                _logger.Debug(this._logPrefix + "stream.Write: " + message);
            }

            this.BStream.Write(bytes, 0, bytes.Length);

            this.ExpectSuccess();
        }

        public void WriteAllToSendBuffer(params byte[][] cmdWithBinaryArgs) {
            this.WriteToSendBuffer(GetCmdBytes('*', cmdWithBinaryArgs.Length));

            foreach (var safeBinaryValue in cmdWithBinaryArgs) {
                this.WriteToSendBuffer(GetCmdBytes('$', safeBinaryValue.Length));
                this.WriteToSendBuffer(safeBinaryValue);
                this.WriteToSendBuffer(this._endData);
            }
        }

        public void WriteToSendBuffer(byte[] cmdBytes) {
            if (this.CouldAddToCurrentBuffer(cmdBytes)) {
                return;
            }

            this.PushCurrentBuffer();

            if (this.CouldAddToCurrentBuffer(cmdBytes)) {
                return;
            }

            var bytesCopied = 0;
            while (bytesCopied < cmdBytes.Length) {
                var copyOfBytes = BufferPool.GetBuffer(cmdBytes.Length);
                var bytesToCopy = Math.Min(cmdBytes.Length - bytesCopied, copyOfBytes.Length);
                Buffer.BlockCopy(cmdBytes, bytesCopied, copyOfBytes, 0, bytesToCopy);
                this._cmdBuffer.Add(new ArraySegment<byte>(copyOfBytes, 0, bytesToCopy));
                bytesCopied += bytesToCopy;
            }
        }

        private bool CouldAddToCurrentBuffer(byte[] cmdBytes) {
            if (cmdBytes.Length + this._currentBufferIndex < RedisConfig.BufferLength) {
                Buffer.BlockCopy(cmdBytes, 0, this._currentBuffer, this._currentBufferIndex, cmdBytes.Length);
                this._currentBufferIndex += cmdBytes.Length;
                return true;
            }

            return false;
        }

        private void PushCurrentBuffer() {
            this._cmdBuffer.Add(new ArraySegment<byte>(this._currentBuffer, 0, this._currentBufferIndex));
            this._currentBuffer = BufferPool.GetBuffer();
            this._currentBufferIndex = 0;
        }

        internal void FlushAndResetSendBuffer() {
            this.FlushSendBuffer();
            this.ResetSendBuffer();
        }

        internal void FlushSendBuffer() {
            if (this._currentBufferIndex > 0) {
                this.PushCurrentBuffer();
            }

            if (this._cmdBuffer.Count > 0) {
                this.OnBeforeFlush?.Invoke();

                if ( /*!Env.IsMono &&*/
                    this.SslStream == null) {

                    if (_logger.IsDebugEnabled() && RedisConfig.EnableVerboseLogging) {
                        var sb = StringBuilderCache.Acquire();
                        foreach (var cmd in this._cmdBuffer) {
                            if (sb.Length > 50) {
                                break;
                            }

                            if (cmd.Array != null) {
                                sb.Append(Encoding.UTF8.GetString(cmd.Array, cmd.Offset, cmd.Count));
                            }
                        }

                        sb.TrimNewLine();
                        _logger.Debug(this._logPrefix + "socket.Send: " + StringBuilderCache.GetStringAndRelease(sb));
                    }


                    this.Socket.Send(this._cmdBuffer); // Optimized for Windows
                } else {
                    // Sending IList<ArraySegment> Throws 'Message to Large' SocketException in Mono
                    foreach (var segment in this._cmdBuffer) {
                        var buffer = segment.Array;

                        if (buffer == null) {
                            throw new InvalidOperationException("buffer is null");
                        }

                        if (this.SslStream == null) {
                            this.Socket.Send(buffer, segment.Offset, segment.Count, SocketFlags.None);
                        } else {
                            this.SslStream.Write(buffer, segment.Offset, segment.Count);
                        }
                    }
                }
            }
        }

        /// <summary>
        ///     reset buffer index in send buffer
        /// </summary>
        public void ResetSendBuffer() {
            this._currentBufferIndex = 0;
            for (var i = this._cmdBuffer.Count - 1; i >= 0; i--) {
                var buffer = this._cmdBuffer[i].Array;
                BufferPool.ReleaseBufferToPool(ref buffer);
                this._cmdBuffer.RemoveAt(i);
            }
        }

        private int SafeReadByte(string name) {
            if (RedisConfig.EnableVerboseLogging) {
                _logger.Debug(this._logPrefix + name + "()");
            }

            return this.BStream.ReadByte();
        }

        protected T SendReceive<T>(byte[][] cmdWithBinaryArgs, Func<T> fn, Action<Func<T>> completePipelineFn = null,
            bool sendWithoutRead = false) {
            if (this.TrackThread != null) {
                if (this.TrackThread.Value.ThreadId != Thread.CurrentThread.ManagedThreadId) {
                    throw new InvalidAccessException(this.TrackThread.Value.ThreadId, this.TrackThread.Value.StackTrace);
                }
            }

            var i = 0;
            var didWriteToBuffer = false;
            Exception originalEx = null;
            var firstAttempt = DateTime.UtcNow;

            while (true) {
                try {
                    if (this.TryConnectIfNeeded()) {
                        didWriteToBuffer = false;
                    }

                    if (this.Socket == null) {
                        throw new RedisRetryableException("Socket is not connected");
                    }

                    if (!didWriteToBuffer) {
                        // only write to buffer once
                        this.WriteCommandToSendBuffer(cmdWithBinaryArgs);
                        didWriteToBuffer = true;
                    }

                    if (this.Pipeline == null) {
                        // pipeline will handle flush if in pipeline
                        this.FlushSendBuffer();
                    } else if (!sendWithoutRead) {
                        if (completePipelineFn == null) {
                            throw new NotSupportedException("Pipeline is not supported.");
                        }

                        completePipelineFn(fn);
                        return default;
                    }

                    T result = default;
                    if (fn != null) {
                        result = fn();
                    }

                    if (this.Pipeline == null) {
                        this.ResetSendBuffer();
                    }

                    if (i > 0) {
                        Interlocked.Increment(ref RedisState.TotalRetrySuccess);
                    }

                    Interlocked.Increment(ref RedisState.TotalCommandsSent);

                    return result;
                } catch (Exception outerEx) {
                    _logger.Debug(this._logPrefix + "SendReceive Exception: " + outerEx.Message);

                    var retryableEx = outerEx as RedisRetryableException;
                    if (retryableEx == null && outerEx is RedisException) {
                        this.ResetSendBuffer();
                        throw;
                    }

                    var ex = retryableEx ?? this.GetRetryableException(outerEx);
                    if (ex == null) {
                        throw this.CreateConnectionError(originalEx ?? outerEx);
                    }

                    if (originalEx == null) {
                        originalEx = ex;
                    }

                    var retry = DateTime.UtcNow - firstAttempt < this._retryTimeout;
                    if (!retry) {
                        if (this.Pipeline == null) {
                            this.ResetSendBuffer();
                        }

                        Interlocked.Increment(ref RedisState.TotalRetryTimedout);
                        throw this.CreateRetryTimeoutException(this._retryTimeout, originalEx);
                    }

                    Interlocked.Increment(ref RedisState.TotalRetryCount);
                    Thread.Sleep(GetBackOffMultiplier(++i));
                }
            }
        }

        private RedisException CreateRetryTimeoutException(TimeSpan retryTimeout, Exception originalEx) {
            this.DeactivatedAt = DateTime.UtcNow;
            var message = $"Exceeded timeout of {retryTimeout}";
            _logger.Error(this._logPrefix + message);
            return new RedisException(message, originalEx);
        }

        private Exception GetRetryableException(Exception outerEx) {
            // several stream commands wrap SocketException in IOException
            var socketEx = outerEx.InnerException as SocketException
                           ?? outerEx as SocketException;

            if (socketEx == null) {
                return null;
            }

            _logger.Error(socketEx, this._logPrefix + "SocketException in SendReceive, retrying...");

            this._lastSocketException = socketEx;

            this.Socket?.Close();
            this.Socket = null;

            return socketEx;
        }

        private static int GetBackOffMultiplier(int i) {
            var nextTryMs = (2 ^ i) * RedisConfig.BackOffMultiplier;
            return nextTryMs;
        }

        protected void SendWithoutRead(params byte[][] cmdWithBinaryArgs) {
            this.SendReceive<long>(cmdWithBinaryArgs, null, null, true);
        }

        protected void SendExpectSuccess(params byte[][] cmdWithBinaryArgs) {
            // Turn Action into Func Hack
            var completePipelineFn = this.Pipeline != null
                ? f => this.Pipeline.CompleteVoidQueuedCommand(() => f())
                : (Action<Func<long>>)null;

            this.SendReceive(cmdWithBinaryArgs, this.ExpectSuccessFn, completePipelineFn);
        }

        protected long SendExpectLong(params byte[][] cmdWithBinaryArgs) {
            return this.SendReceive(cmdWithBinaryArgs,
                this.ReadLong,
                this.Pipeline != null ? this.Pipeline.CompleteLongQueuedCommand : (Action<Func<long>>)null);
        }

        protected byte[] SendExpectData(params byte[][] cmdWithBinaryArgs) {
            return this.SendReceive(cmdWithBinaryArgs,
                this.ReadData,
                this.Pipeline != null ? this.Pipeline.CompleteBytesQueuedCommand : (Action<Func<byte[]>>)null);
        }

        protected double SendExpectDouble(params byte[][] cmdWithBinaryArgs) {
            return this.SendReceive(cmdWithBinaryArgs,
                this.ReadDouble,
                this.Pipeline != null ? this.Pipeline.CompleteDoubleQueuedCommand : (Action<Func<double>>)null);
        }

        protected string SendExpectCode(params byte[][] cmdWithBinaryArgs) {
            return this.SendReceive(cmdWithBinaryArgs,
                this.ExpectCode,
                this.Pipeline != null ? this.Pipeline.CompleteStringQueuedCommand : (Action<Func<string>>)null);
        }

        protected byte[][] SendExpectMultiData(params byte[][] cmdWithBinaryArgs) {
            return this.SendReceive(cmdWithBinaryArgs,
                       this.ReadMultiData,
                       this.Pipeline != null ? this.Pipeline.CompleteMultiBytesQueuedCommand : (Action<Func<byte[][]>>)null)
                   ?? Array.Empty<byte[]>();
        }

        protected object[] SendExpectDeeplyNestedMultiData(params byte[][] cmdWithBinaryArgs) {
            return this.SendReceive(cmdWithBinaryArgs, this.ReadDeeplyNestedMultiData);
        }

        protected RedisData SendExpectComplexResponse(params byte[][] cmdWithBinaryArgs) {
            return this.SendReceive(cmdWithBinaryArgs,
                this.ReadComplexResponse,
                this.Pipeline != null ? this.Pipeline.CompleteRedisDataQueuedCommand : (Action<Func<RedisData>>)null);
        }

        protected List<Dictionary<string, string>> SendExpectStringDictionaryList(params byte[][] cmdWithBinaryArgs) {
            var results = this.SendExpectComplexResponse(cmdWithBinaryArgs);
            var to = new List<Dictionary<string, string>>();
            foreach (var data in results.Children) {
                if (data.Children != null) {
                    var map = ToDictionary(data);
                    to.Add(map);
                }
            }

            return to;
        }

        private static Dictionary<string, string> ToDictionary(RedisData data) {
            string key = null;
            var map = new Dictionary<string, string>();

            if (data.Children == null) {
                throw new ArgumentNullException("data.Children");
            }

            for (var i = 0; i < data.Children.Count; i++) {
                var bytes = data.Children[i].Data;
                if (i % 2 == 0) {
                    key = bytes.FromUtf8Bytes();
                } else {
                    if (key == null) {
                        throw new RedisResponseException(
                            string.Format("key == null, i={0}, data.Children[i] = {1}", i, data.Children[i].ToRedisText().ToJson()));
                    }

                    var val = bytes.FromUtf8Bytes();
                    map[key] = val;
                }
            }

            return map;
        }

        protected string SendExpectString(params byte[][] cmdWithBinaryArgs) {
            var bytes = this.SendExpectData(cmdWithBinaryArgs);
            return bytes.FromUtf8Bytes();
        }

        protected void Log(string fmt, params object[] args) {
            if (!RedisConfig.EnableVerboseLogging) {
                return;
            }

            _logger.Trace(this._logPrefix + string.Format(fmt, args).Trim());
        }

        protected void CmdLog(byte[][] args) {
            var sb = StringBuilderCache.Acquire();
            foreach (var arg in args) {
                var strArg = arg.FromUtf8Bytes();
                if (strArg == this.Password) {
                    continue;
                }

                if (sb.Length > 0) {
                    sb.Append(" ");
                }

                sb.Append(strArg);

                if (sb.Length > 100) {
                    break;
                }
            }

            this._lastCommand = StringBuilderCache.GetStringAndRelease(sb);
            if (this._lastCommand.Length > 100) {
                this._lastCommand = this._lastCommand.Substring(0, 100) + "...";
            }

            _logger.Trace(this._logPrefix + "S: " + this._lastCommand);
        }

        // Turn Action into Func Hack
        protected long ExpectSuccessFn() {
            this.ExpectSuccess();
            return 0;
        }

        protected void ExpectSuccess() {
            var c = this.SafeReadByte(nameof(this.ExpectSuccess));
            if (c == -1) {
                throw this.CreateNoMoreDataError();
            }

            var s = this.ReadLine();

            this.Log((char)c + s);

            if (c == '-') {
                throw this.CreateResponseError(s.StartsWith("ERR") && s.Length >= 4 ? s.Substring(4) : s);
            }
        }

        private void ExpectWord(string word) {
            var c = this.SafeReadByte(nameof(this.ExpectWord));
            if (c == -1) {
                throw this.CreateNoMoreDataError();
            }

            var s = this.ReadLine();
            this.Log((char)c + s);

            if (c == '-') {
                throw this.CreateResponseError(s.StartsWith("ERR") ? s.Substring(4) : s);
            }

            if (s != word) {
                throw this.CreateResponseError($"Expected '{word}' got '{s}'");
            }
        }

        private string ExpectCode() {
            var c = this.SafeReadByte(nameof(this.ExpectCode));
            if (c == -1) {
                throw this.CreateNoMoreDataError();
            }

            var s = this.ReadLine();
            this.Log((char)c + s);

            if (c == '-') {
                throw this.CreateResponseError(s.StartsWith("ERR") ? s.Substring(4) : s);
            }

            return s;
        }

        internal void ExpectOk() {
            this.ExpectWord(_ok);
        }

        internal void ExpectQueued() {
            this.ExpectWord(_queued);
        }

        public long ReadLong() {
            var c = this.SafeReadByte(nameof(this.ReadLong));
            if (c == -1) {
                throw this.CreateNoMoreDataError();
            }

            var s = this.ReadLine();
            this.Log("R: {0}", s);

            if (c == '-') {
                throw this.CreateResponseError(s.StartsWith("ERR") ? s.Substring(4) : s);
            }

            if (c == ':' || c == '$') {
                // really strange why ZRANK needs the '$' here
                if (long.TryParse(s, out var i)) {
                    return i;
                }
            }

            throw this.CreateResponseError("Unknown reply on integer response: " + c + s);
        }

        public double ReadDouble() {
            var bytes = this.ReadData();
            return bytes == null ? double.NaN : ParseDouble(bytes);
        }

        public static double ParseDouble(byte[] doubleBytes) {
            var doubleString = doubleBytes.FromUtf8Bytes();
            double.TryParse(doubleString, NumberStyles.Any, CultureInfo.InvariantCulture.NumberFormat, out var d);
            return d;
        }

        private byte[] ReadData() {
            var r = this.ReadLine();
            return this.ParseSingleLine(r);
        }

        private byte[] ParseSingleLine(string r) {
            this.Log("R: {0}", r);

            if (r.Length == 0) {
                throw this.CreateResponseError("Zero length response");
            }

            var c = r[0];
            if (c == '-') {
                throw this.CreateResponseError(r.StartsWith("-ERR") ? r.Substring(5) : r.Substring(1));
            }

            if (c == '$') {
                if (r == "$-1") {
                    return null;
                }

                if (int.TryParse(r.Substring(1), out var count)) {
                    var retbuf = new byte[count];

                    var offset = 0;
                    while (count > 0) {
                        var readCount = this.BStream.Read(retbuf, offset, count);
                        if (readCount <= 0) {
                            throw this.CreateResponseError("Unexpected end of Stream");
                        }

                        offset += readCount;
                        count -= readCount;
                    }

                    if (this.BStream.ReadByte() != '\r' || this.BStream.ReadByte() != '\n') {
                        throw this.CreateResponseError("Invalid termination");
                    }

                    return retbuf;
                }

                throw this.CreateResponseError("Invalid length");
            }

            if (c == ':' || c == '+') {
                // match the return value
                return r.Substring(1).ToUtf8Bytes();
            }

            throw this.CreateResponseError("Unexpected reply: " + r);
        }

        private byte[][] ReadMultiData() {
            var c = this.SafeReadByte(nameof(this.ReadMultiData));
            if (c == -1) {
                throw this.CreateNoMoreDataError();
            }

            var s = this.ReadLine();
            this.Log("R: {0}", s);

            switch (c) {
                // Some commands like BRPOPLPUSH may return Bulk Reply instead of Multi-bulk
                case '$':
                    var t = new byte[2][];
                    t[1] = this.ParseSingleLine(string.Concat(char.ToString((char)c), s));
                    return t;

                case '-':
                    throw this.CreateResponseError(s.StartsWith("ERR") ? s.Substring(4) : s);

                case '*':
                    if (int.TryParse(s, out var count)) {
                        if (count == -1) {
                            // redis is in an invalid state
                            return Array.Empty<byte[]>();
                        }

                        var result = new byte[count][];

                        for (var i = 0; i < count; i++) {
                            result[i] = this.ReadData();
                        }

                        return result;
                    }

                    break;
            }

            throw this.CreateResponseError("Unknown reply on multi-request: " + c + s);
        }

        private object[] ReadDeeplyNestedMultiData() {
            var result = this.ReadDeeplyNestedMultiDataItem();
            return (object[])result;
        }

        private object ReadDeeplyNestedMultiDataItem() {
            var c = this.SafeReadByte(nameof(this.ReadDeeplyNestedMultiDataItem));
            if (c == -1) {
                throw this.CreateNoMoreDataError();
            }

            var s = this.ReadLine();
            this.Log("R: {0}", s);

            switch (c) {
                case '$':
                    return this.ParseSingleLine(string.Concat(char.ToString((char)c), s));

                case '-':
                    throw this.CreateResponseError(s.StartsWith("ERR") ? s.Substring(4) : s);

                case '*':
                    if (int.TryParse(s, out var count)) {
                        var array = new object[count];
                        for (var i = 0; i < count; i++) {
                            array[i] = this.ReadDeeplyNestedMultiDataItem();
                        }

                        return array;
                    }

                    break;

                default:
                    return s;
            }

            throw this.CreateResponseError("Unknown reply on multi-request: " + c + s);
        }

        internal RedisData ReadComplexResponse() {
            var c = this.SafeReadByte(nameof(this.ReadComplexResponse));
            if (c == -1) {
                throw this.CreateNoMoreDataError();
            }

            var s = this.ReadLine();
            this.Log("R: {0}", s);

            switch (c) {
                case '$':
                    return new RedisData {
                        Data = this.ParseSingleLine(string.Concat(char.ToString((char)c), s))
                    };

                case '-':
                    throw this.CreateResponseError(s.StartsWith("ERR") ? s.Substring(4) : s);

                case '*':
                    if (int.TryParse(s, out var count)) {
                        var ret = new RedisData { Children = new List<RedisData>() };
                        for (var i = 0; i < count; i++) {
                            ret.Children.Add(this.ReadComplexResponse());
                        }

                        return ret;
                    }

                    break;

                default:
                    return new RedisData { Data = s.ToUtf8Bytes() };
            }

            throw this.CreateResponseError("Unknown reply on multi-request: " + c + s);
        }

        internal int ReadMultiDataResultCount() {
            var c = this.SafeReadByte(nameof(this.ReadMultiDataResultCount));
            if (c == -1) {
                throw this.CreateNoMoreDataError();
            }

            var s = this.ReadLine();
            this.Log("R: {0}", s);

            if (c == '-') {
                throw this.CreateResponseError(s.StartsWith("ERR") ? s.Substring(4) : s);
            }

            if (c == '*') {
                if (int.TryParse(s, out var count)) {
                    return count;
                }
            }

            throw this.CreateResponseError("Unknown reply on multi-request: " + c + s);
        }

        private static void AssertListIdAndValue(string listId, byte[] value) {
            if (listId == null) {
                throw new ArgumentNullException(nameof(listId));
            }

            if (value == null) {
                throw new ArgumentNullException(nameof(value));
            }
        }

        private static byte[][] MergeCommandWithKeysAndValues(byte[] cmd, byte[][] keys, byte[][] values) {
            byte[][] firstParams = { cmd };
            return MergeCommandWithKeysAndValues(firstParams, keys, values);
        }

        private static byte[][] MergeCommandWithKeysAndValues(byte[] cmd, byte[] firstArg, byte[][] keys, byte[][] values) {
            byte[][] firstParams = { cmd, firstArg };
            return MergeCommandWithKeysAndValues(firstParams, keys, values);
        }

        private static byte[][] MergeCommandWithKeysAndValues(byte[][] firstParams, byte[][] keys, byte[][] values) {
            if (keys == null || keys.Length == 0) {
                throw new ArgumentNullException(nameof(keys));
            }

            if (values == null || values.Length == 0) {
                throw new ArgumentNullException(nameof(values));
            }

            if (keys.Length != values.Length) {
                throw new ArgumentException("The number of values must be equal to the number of keys");
            }

            var keyValueStartIndex = firstParams.Length;

            var keysAndValuesLength = keys.Length * 2 + keyValueStartIndex;
            var keysAndValues = new byte[keysAndValuesLength][];

            for (var i = 0; i < keyValueStartIndex; i++) {
                keysAndValues[i] = firstParams[i];
            }

            var j = 0;
            for (var i = keyValueStartIndex; i < keysAndValuesLength; i += 2) {
                keysAndValues[i] = keys[j];
                keysAndValues[i + 1] = values[j];
                j++;
            }

            return keysAndValues;
        }

        private static byte[][] MergeCommandWithArgs(byte[] cmd, params string[] args) {
            var byteArgs = args.ToMultiByteArray();
            return MergeCommandWithArgs(cmd, byteArgs);
        }

        private static byte[][] MergeCommandWithArgs(byte[] cmd, params byte[][] args) {
            var mergedBytes = new byte[1 + args.Length][];
            mergedBytes[0] = cmd;
            for (var i = 0; i < args.Length; i++) {
                mergedBytes[i + 1] = args[i];
            }

            return mergedBytes;
        }

        private static byte[][] MergeCommandWithArgs(byte[] cmd, byte[] firstArg, params byte[][] args) {
            var mergedBytes = new byte[2 + args.Length][];
            mergedBytes[0] = cmd;
            mergedBytes[1] = firstArg;
            for (var i = 0; i < args.Length; i++) {
                mergedBytes[i + 2] = args[i];
            }

            return mergedBytes;
        }

        protected byte[][] ConvertToBytes(string[] keys) {
            var keyBytes = new byte[keys.Length][];
            for (var i = 0; i < keys.Length; i++) {
                var key = keys[i];
                keyBytes[i] = key != null ? key.ToUtf8Bytes() : Array.Empty<byte>();
            }

            return keyBytes;
        }

        protected byte[][] MergeAndConvertToBytes(string[] keys, string[] args) {
            if (keys == null) {
                keys = Array.Empty<string>();
            }

            if (args == null) {
                args = Array.Empty<string>();
            }

            var keysLength = keys.Length;
            var merged = new string[keysLength + args.Length];
            for (var i = 0; i < merged.Length; i++) {
                merged[i] = i < keysLength ? keys[i] : args[i - keysLength];
            }

            return this.ConvertToBytes(merged);
        }

    }

}
