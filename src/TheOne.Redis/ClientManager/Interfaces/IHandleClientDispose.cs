using TheOne.Redis.Client;

namespace TheOne.Redis.ClientManager {

    public interface IHandleClientDispose {

        void DisposeClient(RedisNativeClient client);

    }

}
