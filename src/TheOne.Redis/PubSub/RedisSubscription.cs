using System;
using System.Collections.Generic;
using TheOne.Redis.Client;
using TheOne.Redis.Common;

namespace TheOne.Redis.PubSub {

    public class RedisSubscription : IRedisSubscription {

        private const int _msgIndex = 2;
        private static readonly byte[] _subscribeWord = "subscribe".ToUtf8Bytes();
        private static readonly byte[] _psubscribeWord = "psubscribe".ToUtf8Bytes();
        private static readonly byte[] _unsubscribeWord = "unsubscribe".ToUtf8Bytes();
        private static readonly byte[] _punsubscribeWord = "punsubscribe".ToUtf8Bytes();
        private static readonly byte[] _messageWord = "message".ToUtf8Bytes();
        private static readonly byte[] _pmessageWord = "pmessage".ToUtf8Bytes();
        private readonly IRedisNativeClient _redisClient;
        private List<string> _activeChannels;

        public RedisSubscription(IRedisNativeClient redisClient) {
            this._redisClient = redisClient;
            this.SubscriptionCount = 0;
            this._activeChannels = new List<string>();
        }

        public bool IsPSubscription { get; private set; }

        /// <inheritdoc />
        public long SubscriptionCount { get; private set; }

        /// <inheritdoc />
        public Action<string> OnSubscribe { get; set; }

        /// <inheritdoc />
        public Action<string, string> OnMessage { get; set; }

        /// <inheritdoc />
        public Action<string, byte[]> OnMessageBytes { get; set; }

        /// <inheritdoc />
        public Action<string> OnUnSubscribe { get; set; }

        /// <inheritdoc />
        public void SubscribeToChannels(params string[] channels) {
            var multiBytes = this._redisClient.Subscribe(channels);
            this.ParseSubscriptionResults(multiBytes);

            while (this.SubscriptionCount > 0) {
                multiBytes = this._redisClient.ReceiveMessages();
                this.ParseSubscriptionResults(multiBytes);
            }
        }

        /// <inheritdoc />
        public void SubscribeToChannelsMatching(params string[] patterns) {
            var multiBytes = this._redisClient.PSubscribe(patterns);
            this.ParseSubscriptionResults(multiBytes);

            while (this.SubscriptionCount > 0) {
                multiBytes = this._redisClient.ReceiveMessages();
                this.ParseSubscriptionResults(multiBytes);
            }
        }

        /// <inheritdoc />
        public void UnSubscribeFromAllChannels() {
            if (this._activeChannels.Count == 0) {
                return;
            }

            var multiBytes = this._redisClient.UnSubscribe();
            this.ParseSubscriptionResults(multiBytes);

            this._activeChannels = new List<string>();
        }

        /// <inheritdoc />
        public void UnSubscribeFromChannels(params string[] channels) {
            var multiBytes = this._redisClient.UnSubscribe(channels);
            this.ParseSubscriptionResults(multiBytes);
        }

        /// <inheritdoc />
        public void UnSubscribeFromChannelsMatching(params string[] patterns) {
            var multiBytes = this._redisClient.PUnSubscribe(patterns);
            this.ParseSubscriptionResults(multiBytes);
        }

        /// <inheritdoc />
        public void Dispose() {
            if (this.IsPSubscription) {
                this.UnSubscribeFromAllChannelsMatchingAnyPatterns();
            } else {
                this.UnSubscribeFromAllChannels();
            }
        }

        private void ParseSubscriptionResults(byte[][] multiBytes) {
            var componentsPerMsg = this.IsPSubscription ? 4 : 3;
            for (var i = 0; i < multiBytes.Length; i += componentsPerMsg) {
                var messageType = multiBytes[i];
                var channel = multiBytes[i + 1].FromUtf8Bytes();
                if (AreEqual(_subscribeWord, messageType)
                    || AreEqual(_psubscribeWord, messageType)) {
                    this.IsPSubscription = AreEqual(_psubscribeWord, messageType);
                    this.SubscriptionCount = int.Parse(multiBytes[i + _msgIndex].FromUtf8Bytes());
                    this._activeChannels.Add(channel);
                    this.OnSubscribe?.Invoke(channel);
                } else if (AreEqual(_unsubscribeWord, messageType)
                           || AreEqual(_punsubscribeWord, messageType)) {
                    this.SubscriptionCount = int.Parse(multiBytes[i + 2].FromUtf8Bytes());
                    this._activeChannels.Remove(channel);
                    this.OnUnSubscribe?.Invoke(channel);
                } else if (AreEqual(_messageWord, messageType)) {
                    var msgBytes = multiBytes[i + _msgIndex];
                    this.OnMessageBytes?.Invoke(channel, msgBytes);
                    this.OnMessage?.Invoke(channel, msgBytes.FromUtf8Bytes());
                } else if (AreEqual(_pmessageWord, messageType)) {
                    channel = multiBytes[i + 2].FromUtf8Bytes();
                    var msgBytes = multiBytes[i + _msgIndex + 1];
                    this.OnMessageBytes?.Invoke(channel, msgBytes);
                    this.OnMessage?.Invoke(channel, msgBytes.FromUtf8Bytes());
                } else {
                    throw new RedisException(
                        "Invalid state. Expected [[p]subscribe|[p]unsubscribe|message] got: " + messageType.FromUtf8Bytes());
                }
            }
        }

        public void UnSubscribeFromAllChannelsMatchingAnyPatterns() {
            if (this._activeChannels.Count == 0) {
                return;
            }

            var multiBytes = this._redisClient.PUnSubscribe();
            this.ParseSubscriptionResults(multiBytes);

            this._activeChannels = new List<string>();
        }

        private static bool AreEqual(byte[] b1, byte[] b2) {
            if (b1 == b2) {
                return true;
            }

            if (b1 == null || b2 == null) {
                return false;
            }

            if (b1.Length != b2.Length) {
                return false;
            }

            for (var i = 0; i < b1.Length; i++) {
                if (b1[i] != b2[i]) {
                    return false;
                }
            }

            return true;
        }

    }

}
