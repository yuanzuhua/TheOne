using System;

namespace TheOne.Redis {

    public interface IHasRedisId<T> {

        T Id { get; }

    }

    public interface IHasRedisIntId : IHasRedisId<int> { }

    public interface IHasRedisLongId : IHasRedisId<long> { }

    public interface IHasRedisGuidId : IHasRedisId<Guid> { }

    public interface IHasRedisStringId : IHasRedisId<string> { }

}
