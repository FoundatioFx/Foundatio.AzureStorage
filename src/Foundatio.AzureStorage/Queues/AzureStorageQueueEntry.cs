using System;
using Azure.Storage.Queues.Models;

namespace Foundatio.Queues {
    public class AzureStorageQueueEntry<T> : QueueEntry<T> where T : class {
        public QueueMessage UnderlyingMessage { get; }

        public AzureStorageQueueEntry(QueueMessage message, T value, IQueue<T> queue)
            : base(message.MessageId, null, value, queue, message.InsertedOn.GetValueOrDefault().UtcDateTime, (int)message.DequeueCount) {

            UnderlyingMessage = message;
        }
    }
}