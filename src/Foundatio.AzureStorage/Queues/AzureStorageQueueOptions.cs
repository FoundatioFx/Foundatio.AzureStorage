using System;
using Azure.Core;

namespace Foundatio.Queues {
    public class AzureStorageQueueOptions<T> : SharedQueueOptions<T> where T : class {
        public string ConnectionString { get; set; }
        public TimeSpan DequeueInterval { get; set; } = TimeSpan.FromSeconds(2);
        public RetryMode  RetryMode { get; set; }

        // The delay between retry attempts for a fixed approach or the delay on which to base calculations for a backoff-based approach.
        public TimeSpan Delay { get; set; }
    }

    public class AzureStorageQueueOptionsBuilder<T> : SharedQueueOptionsBuilder<T, AzureStorageQueueOptions<T>, AzureStorageQueueOptionsBuilder<T>> where T: class {
        public AzureStorageQueueOptionsBuilder<T> ConnectionString(string connectionString) {
            Target.ConnectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            return this;
        }

        public AzureStorageQueueOptionsBuilder<T> DequeueInterval(TimeSpan dequeueInterval) {
            Target.DequeueInterval = dequeueInterval;
            return this;
        }

        public AzureStorageQueueOptionsBuilder<T> RetryMode(RetryMode retryMode) {
            Target.RetryMode = retryMode;
            return this;
        }

        public AzureStorageQueueOptionsBuilder<T> RetryDelay(TimeSpan retryDelay) {
            Target.Delay = retryDelay;
            return this;
        }

    }
}