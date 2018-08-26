namespace TheOne.Redis.Client {

    public class RedisRetryableException : RedisException {

        public RedisRetryableException(string message)
            : base(message) { }

        public RedisRetryableException(string message, string code) : base(message) {
            this.Code = code;
        }

        public string Code { get; }

    }

}
