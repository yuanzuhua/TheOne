using System;

namespace TheOne.Redis.Pipeline {

    /// <summary>
    ///     Pipeline interface shared by typed and non-typed pipelines
    /// </summary>
    public interface IRedisPipelineShared : IDisposable, IRedisQueueCompletableOperation {

        void Flush();
        bool Replay();

    }

    /// <summary>
    ///     Interface to redis pipeline
    /// </summary>
    public interface IRedisPipeline : IRedisPipelineShared, IRedisQueueableOperation { }

    /// <summary>
    ///     Interface to redis typed pipeline
    /// </summary>
    public interface IRedisTypedPipeline<T> : IRedisPipelineShared, IRedisTypedQueueableOperation<T> { }

}
