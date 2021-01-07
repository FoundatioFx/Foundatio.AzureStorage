﻿using System;

namespace Foundatio.Queues {
    public class AzureStorageQueueOptions<T> : SharedQueueOptions<T> where T : class {
        public string ConnectionString { get; set; }
        public TimeSpan DequeueInterval { get; set; } = TimeSpan.FromSeconds(2);
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
    }
}