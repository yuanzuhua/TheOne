using System.Threading;

namespace TheOne.Redis.Client {

    internal struct TrackThread {

        public readonly int ThreadId;
        public readonly string StackTrace;

        public TrackThread(int threadId, string stackTrace) {
            this.ThreadId = threadId;
            this.StackTrace = stackTrace;
        }

    }

    public class InvalidAccessException : RedisException {

        public InvalidAccessException(int threadId, string stackTrace)
            : base(
                $"The Current Thread #{Thread.CurrentThread.ManagedThreadId} is different to the original Thread #{threadId} that resolved this pooled client at: \n{stackTrace}") { }

    }

}
