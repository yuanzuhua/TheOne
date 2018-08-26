using System;

namespace TheOne.Redis.Pipeline {

    /// <summary>
    ///     Base transaction interface, shared by typed and non-typed transactions
    /// </summary>
    public interface IRedisTransactionBase : IRedisPipelineShared { }

    /// <summary>
    ///     Interface to redis transaction
    /// </summary>
    public interface IRedisTransaction : IRedisTransactionBase, IRedisQueueableOperation {

        bool Commit();
        void Rollback();

    }

    /// <summary>
    ///     Redis transaction for typed client
    /// </summary>
    public interface IRedisTypedTransaction<T> : IRedisTypedQueueableOperation<T>, IDisposable {

        bool Commit();
        void Rollback();

    }

}
