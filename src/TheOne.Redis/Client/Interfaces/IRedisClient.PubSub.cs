using TheOne.Redis.PubSub;

namespace TheOne.Redis.Client {

    public partial interface IRedisClient {

        void Watch(params string[] keys);

        void UnWatch();

        IRedisSubscription CreateSubscription();

        long PublishMessage(string toChannel, string message);

    }

}
