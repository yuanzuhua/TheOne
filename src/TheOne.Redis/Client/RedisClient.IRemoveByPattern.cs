using System.Linq;

namespace TheOne.Redis.Client {

    public partial class RedisClient : IRemoveByPattern {

        /// <inheritdoc />
        public void RemoveByPattern(string pattern) {
            var keys = this.ScanAllKeys(pattern).ToArray();
            if (keys.Length > 0) {
                this.Del(keys);
            }
        }

        /// <inheritdoc />
        public void RemoveByRegex(string pattern) {
            this.RemoveByPattern(pattern.Replace(".*", "*").Replace(".+", "?"));
        }

    }

}
