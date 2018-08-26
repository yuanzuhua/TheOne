namespace TheOne.Redis.Client {

    public class RedisResponseException : RedisException {

        public RedisResponseException(string message)
            : base(message) { }

        public RedisResponseException(string message, string code) : base(message) {
            this.Code = code;
        }

        public string Code { get; }

    }

}
