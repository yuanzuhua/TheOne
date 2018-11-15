using System;
using System.Collections.Generic;
using System.Text;
using TheOne.Redis.Common;
using TheOne.Redis.External;

namespace TheOne.Redis.Client {

    public partial class RedisClient : IRedisClient {

        /// <inheritdoc />
        public void SetConfig(string configItem, string value) {
            this.ConfigSet(configItem, value.ToUtf8Bytes());
        }

        /// <inheritdoc />
        public RedisText GetServerRoleInfo() {
            return this.Role();
        }

        /// <inheritdoc />
        public string GetConfig(string configItem) {
            StringBuilder sb = StringBuilderCache.Acquire();
            byte[][] byteArray = this.ConfigGet(configItem);
            const int startAt = 1; // skip repeating config name
            for (var i = startAt; i < byteArray.Length; i++) {
                byte[] bytes = byteArray[i];
                if (sb.Length > 0) {
                    sb.Append(" ");
                }

                sb.Append(bytes.FromUtf8Bytes());
            }

            return StringBuilderCache.GetStringAndRelease(sb);
        }

        /// <inheritdoc />
        public void SaveConfig() {
            this.ConfigRewrite();
        }

        /// <inheritdoc />
        public void ResetInfoStats() {
            this.ConfigResetStat();
        }

        /// <inheritdoc />
        public string GetClient() {
            return this.ClientGetName();
        }

        /// <inheritdoc />
        public void SetClient(string name) {
            this.ClientSetName(name);
        }

        /// <inheritdoc />
        public void KillClient(string address) {
            this.ClientKill(address);
        }

        /// <inheritdoc />
        public long KillClients(string fromAddress = null, string withId = null, RedisClientType? ofType = null, bool? skipMe = null) {
            var typeString = ofType != null ? ofType.ToString().ToLower() : null;
            var skipMeString = skipMe != null ? skipMe.Value ? "yes" : "no" : null;
            return this.ClientKill(fromAddress, withId, typeString, skipMeString);
        }

        /// <inheritdoc />
        public List<Dictionary<string, string>> GetClientsInfo() {
            var clientList = this.ClientList().FromUtf8Bytes();
            var results = new List<Dictionary<string, string>>();

            string[] lines = clientList.Split('\n');
            foreach (var line in lines) {
                if (string.IsNullOrEmpty(line)) {
                    continue;
                }

                var map = new Dictionary<string, string>();
                string[] parts = line.Split(' ');
                foreach (var part in parts) {
                    string[] keyValue = part.SplitOnFirst('=');
                    map[keyValue[0]] = keyValue[1];
                }

                results.Add(map);
            }

            return results;
        }

        /// <inheritdoc />
        public void PauseAllClients(TimeSpan duration) {
            this.ClientPause((int)duration.TotalMilliseconds);
        }

        /// <inheritdoc />
        public DateTime GetServerTime() {
            byte[][] parts = this.Time();
            var unixTime = long.Parse(parts[0].FromUtf8Bytes());
            var microSecs = long.Parse(parts[1].FromUtf8Bytes());
            var ticks = microSecs / 1000 * TimeSpan.TicksPerMillisecond;

            DateTime date = unixTime.FromUnixTime();
            TimeSpan timeSpan = TimeSpan.FromTicks(ticks);
            return date + timeSpan;
        }

    }

}
