using System;
using Azure.Storage.Queues.Models;

namespace Foundatio.Queues;

public class AzureStorageQueueEntry<T> : QueueEntry<T> where T : class
{
    public QueueMessage UnderlyingMessage { get; }

    /// <summary>
    /// The current pop receipt for this message. This gets updated after each
    /// UpdateMessageAsync call (renew lock, abandon with retry delay).
    /// </summary>
    public string PopReceipt { get; internal set; }

    public AzureStorageQueueEntry(QueueMessage message, AzureStorageQueueEnvelope<T> envelope, IQueue<T> queue)
        : base(message.MessageId, envelope?.CorrelationId, envelope?.Value, queue, message.InsertedOn?.UtcDateTime ?? DateTime.MinValue, (int)message.DequeueCount)
    {
        UnderlyingMessage = message;
        PopReceipt = message.PopReceipt;

        // Copy properties from envelope to the entry
        if (envelope?.Properties != null)
        {
            foreach (var property in envelope.Properties)
                Properties.Add(property.Key, property.Value);
        }
    }
}
