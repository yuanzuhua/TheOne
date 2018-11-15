namespace TheOne.Redis.Client {

    /// <inheritdoc />
    public class RedisRetryableException : RedisException {

        /// <inheritdoc />
        public RedisRetryableException(string message) : base(message) { }

        /// <inheritdoc />
        public RedisRetryableException(string message, string code) : base(message) {
            this.Code = code;
        }

        public string Code { get; }

    }

}
