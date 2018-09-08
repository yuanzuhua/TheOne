using TheOne.Redis.Client.Internal;

namespace TheOne.Redis.Client {

    public partial class RedisClient : IRemoveByPattern {

        /// <inheritdoc />
        public void RemoveByPattern(string pattern) {
            string[] keys = this.Keys(pattern).ToStringArray();
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
