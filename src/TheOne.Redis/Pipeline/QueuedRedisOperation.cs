using System;
using System.Collections.Generic;
using TheOne.Logging;
using TheOne.Redis.Client;
using TheOne.Redis.Client.Internal;
using TheOne.Redis.Common;

namespace TheOne.Redis.Pipeline {

    internal class QueuedRedisOperation {

        protected static readonly ILog Logger = LogProvider.GetLogger(typeof(QueuedRedisOperation));

        public Action VoidReadCommand { get; set; }
        public Func<int> IntReadCommand { get; set; }
        public Func<long> LongReadCommand { get; set; }
        public Func<bool> BoolReadCommand { get; set; }
        public Func<byte[]> BytesReadCommand { get; set; }
        public Func<byte[][]> MultiBytesReadCommand { get; set; }
        public Func<string> StringReadCommand { get; set; }
        public Func<List<string>> MultiStringReadCommand { get; set; }
        public Func<Dictionary<string, string>> DictionaryStringReadCommand { get; set; }
        public Func<double> DoubleReadCommand { get; set; }
        public Func<RedisData> RedisDataReadCommand { get; set; }

        public Action OnSuccessVoidCallback { get; set; }
        public Action<int> OnSuccessIntCallback { get; set; }
        public Action<long> OnSuccessLongCallback { get; set; }
        public Action<bool> OnSuccessBoolCallback { get; set; }
        public Action<byte[]> OnSuccessBytesCallback { get; set; }
        public Action<byte[][]> OnSuccessMultiBytesCallback { get; set; }
        public Action<string> OnSuccessStringCallback { get; set; }
        public Action<List<string>> OnSuccessMultiStringCallback { get; set; }
        public Action<Dictionary<string, string>> OnSuccessDictionaryStringCallback { get; set; }
        public Action<RedisData> OnSuccessRedisDataCallback { get; set; }
        public Action<RedisText> OnSuccessRedisTextCallback { get; set; }
        public Action<double> OnSuccessDoubleCallback { get; set; }

        public Action<string> OnSuccessTypeCallback { get; set; }
        public Action<List<string>> OnSuccessMultiTypeCallback { get; set; }

        public Action<Exception> OnErrorCallback { get; set; }

        public virtual void Execute(IRedisClient client) { }

        public void ProcessResult() {
            try {
                if (this.VoidReadCommand != null) {
                    this.VoidReadCommand();
                    this.OnSuccessVoidCallback?.Invoke();
                } else if (this.IntReadCommand != null) {
                    var result = this.IntReadCommand();
                    this.OnSuccessIntCallback?.Invoke(result);

                    this.OnSuccessLongCallback?.Invoke(result);

                    if (this.OnSuccessBoolCallback != null) {
                        var success = result == RedisNativeClient.Success;
                        this.OnSuccessBoolCallback(success);
                    }

                    this.OnSuccessVoidCallback?.Invoke();
                } else if (this.LongReadCommand != null) {
                    var result = this.LongReadCommand();
                    this.OnSuccessIntCallback?.Invoke((int)result);

                    this.OnSuccessLongCallback?.Invoke(result);

                    if (this.OnSuccessBoolCallback != null) {
                        var success = result == RedisNativeClient.Success;
                        this.OnSuccessBoolCallback(success);
                    }

                    this.OnSuccessVoidCallback?.Invoke();
                } else if (this.DoubleReadCommand != null) {
                    var result = this.DoubleReadCommand();
                    this.OnSuccessDoubleCallback?.Invoke(result);
                } else if (this.BytesReadCommand != null) {
                    var result = this.BytesReadCommand();
                    if (result != null && result.Length == 0) {
                        result = null;
                    }

                    this.OnSuccessBytesCallback?.Invoke(result);
                    this.OnSuccessStringCallback?.Invoke(result?.FromUtf8Bytes());
                    this.OnSuccessTypeCallback?.Invoke(result?.FromUtf8Bytes());
                    this.OnSuccessIntCallback?.Invoke(result != null ? int.Parse(result.FromUtf8Bytes()) : 0);
                    this.OnSuccessBoolCallback?.Invoke(result != null && result.FromUtf8Bytes() == "OK");
                } else if (this.StringReadCommand != null) {
                    var result = this.StringReadCommand();
                    this.OnSuccessStringCallback?.Invoke(result);
                    this.OnSuccessTypeCallback?.Invoke(result);
                } else if (this.MultiBytesReadCommand != null) {
                    var result = this.MultiBytesReadCommand();
                    this.OnSuccessMultiBytesCallback?.Invoke(result);
                    this.OnSuccessMultiStringCallback?.Invoke(result?.ToStringList());
                    this.OnSuccessMultiTypeCallback?.Invoke(result?.ToStringList());
                    this.OnSuccessDictionaryStringCallback?.Invoke(result?.ToStringDictionary());
                } else if (this.MultiStringReadCommand != null) {
                    var result = this.MultiStringReadCommand();
                    this.OnSuccessMultiStringCallback?.Invoke(result);
                } else if (this.RedisDataReadCommand != null) {
                    var data = this.RedisDataReadCommand();
                    this.OnSuccessRedisTextCallback?.Invoke(data.ToRedisText());
                    this.OnSuccessRedisDataCallback?.Invoke(data);
                }
            } catch (Exception ex) {
                Logger.Error(ex, "Error in QueuedRedisOperation.ProcessResult");

                if (this.OnErrorCallback != null) {
                    this.OnErrorCallback(ex);
                } else {
                    throw;
                }
            }
        }

    }

}
