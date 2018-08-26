namespace TheOne.Redis.Common {

    internal delegate object GetMemberDelegate<T>(T instance);

    internal class ModelConfig<T> {

        public static void Id(GetMemberDelegate<T> getIdFn) {
            IdUtils<T>.CanGetId = getIdFn;
        }

    }

}
