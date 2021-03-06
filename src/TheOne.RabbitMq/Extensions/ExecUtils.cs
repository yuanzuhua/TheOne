using System;
using System.Collections.Generic;
using System.Threading;

namespace TheOne.RabbitMq.Extensions {

    internal static class ExecUtils {

        /// <summary>
        ///     Default base sleep time (milliseconds).
        /// </summary>
        public static int BaseDelayMs { get; set; } = 100;

        /// <summary>
        ///     Default maximum back-off time before retrying a request
        /// </summary>
        public static int MaxBackOffMs { get; set; } = 1000 * 20;

        /// <summary>
        ///     Maximum retry limit. Avoids integer overflow issues.
        /// </summary>
        public static int MaxRetries { get; set; } = 30;

        public static void RetryUntilTrue(Func<bool> action, TimeSpan? timeout) {
            var i = 0;
            var firstAttempt = DateTime.Now;

            while (timeout == null || DateTime.Now - firstAttempt < timeout.Value) {
                i++;
                if (action()) {
                    return;
                }

                SleepBackOffMultiplier(i);
            }

            throw new TimeoutException($"Exceeded timeout of {timeout.Value}");
        }

        public static void RetryOnException(Action action, TimeSpan? timeout) {
            var i = 0;
            Exception lastEx = null;
            var firstAttempt = DateTime.Now;

            while (timeout == null || DateTime.Now - firstAttempt < timeout.Value) {
                i++;
                try {
                    action();
                    return;
                } catch (Exception ex) {
                    lastEx = ex;

                    SleepBackOffMultiplier(i);
                }
            }

            throw new TimeoutException($"Exceeded timeout of {timeout.Value}", lastEx);
        }

        public static void RetryOnException(Action action, int maxRetries) {
            for (var i = 0; i < maxRetries; i++) {
                try {
                    action();
                    break;
                } catch {
                    if (i == maxRetries - 1) {
                        throw;
                    }

                    SleepBackOffMultiplier(i);
                }
            }
        }

        public static List<Exception> ExecMultiThreading(int threadCount, Action action, int extraSleep = 20) {
            var errors = new List<Exception>();
            var threads = 0;
            var mainLock = new object();

            for (var i = 0; i < threadCount; i++) {
                ThreadPool.QueueUserWorkItem(state => {
                    Interlocked.Increment(ref threads);
                    var threadId = Thread.CurrentThread.ManagedThreadId;
                    Console.WriteLine("Thread {0} started...", threadId);

                    try {
                        action();
                    } catch (Exception ex) {
                        errors.Add(ex);
                    }

                    Console.WriteLine("Thread {0} finished...", threadId);

                    if (Interlocked.Decrement(ref threads) == 0) {
                        Console.WriteLine("All Threads Finished...");
                        lock (mainLock) {
                            Monitor.Pulse(mainLock);
                        }
                    }
                });
            }

            lock (mainLock) {
                Monitor.Wait(mainLock);
            }

            if (extraSleep > 1) {
                Thread.Sleep(extraSleep);
            }

            return errors;
        }

        /// <summary>
        ///     How long to sleep before next retry using Exponential BackOff delay with Full Jitter.
        /// </summary>
        public static void SleepBackOffMultiplier(int retriesAttempted) {
            Thread.Sleep(CalculateFullJitterBackOffDelay(retriesAttempted));
        }

        /// <summary>
        ///     Exponential BackOff Delay with Full Jitter
        /// </summary>
        public static int CalculateFullJitterBackOffDelay(int retriesAttempted) {
            return CalculateFullJitterBackOffDelay(retriesAttempted, BaseDelayMs, MaxBackOffMs);
        }

        /// <summary>
        ///     Exponential BackOff Delay with Full Jitter from:
        ///     https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-core/src/main/java/com/amazonaws/retry/PredefinedBackoffStrategies.java
        /// </summary>
        public static int CalculateFullJitterBackOffDelay(int retriesAttempted, int baseDelay, int maxBackOffMs) {
            var random = new Random(Guid.NewGuid().GetHashCode());
            var ceil = CalculateExponentialDelay(retriesAttempted, baseDelay, maxBackOffMs);
            return random.Next(ceil);
        }

        /// <summary>
        ///     Calculate exponential retry back-off.
        /// </summary>
        public static int CalculateExponentialDelay(int retriesAttempted) {
            return CalculateExponentialDelay(retriesAttempted, BaseDelayMs, MaxBackOffMs);
        }

        /// <summary>
        ///     Calculate exponential retry back-off.
        /// </summary>
        public static int CalculateExponentialDelay(int retriesAttempted, int baseDelay, int maxBackOffMs) {
            if (retriesAttempted <= 0) {
                return baseDelay;
            }

            var retries = Math.Min(retriesAttempted, MaxRetries);
            return (int)Math.Min((1L << retries) * baseDelay, maxBackOffMs);
        }

    }

}
