using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.AsyncEx;
using Foundatio.Extensions;
using Foundatio.Serializer;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Queue;
using Microsoft.Extensions.Logging;

namespace Foundatio.Queues;

public class AzureStorageQueue<T> : QueueBase<T, AzureStorageQueueOptions<T>> where T : class
{
    private readonly AsyncLock _lock = new();
    private readonly CloudQueue _queueReference;
    private readonly CloudQueue _deadletterQueueReference;
    private long _enqueuedCount;
    private long _dequeuedCount;
    private long _completedCount;
    private long _abandonedCount;
    private long _workerErrorCount;
    private bool _queueCreated;

    public AzureStorageQueue(AzureStorageQueueOptions<T> options) : base(options)
    {
        if (String.IsNullOrEmpty(options.ConnectionString))
            throw new ArgumentException("ConnectionString is required.");

        var account = CloudStorageAccount.Parse(options.ConnectionString);
        var client = account.CreateCloudQueueClient();
        if (options.RetryPolicy != null)
            client.DefaultRequestOptions.RetryPolicy = options.RetryPolicy;

        _queueReference = client.GetQueueReference(_options.Name);
        _deadletterQueueReference = client.GetQueueReference($"{_options.Name}-poison");
    }

    public AzureStorageQueue(Builder<AzureStorageQueueOptionsBuilder<T>, AzureStorageQueueOptions<T>> config)
        : this(config(new AzureStorageQueueOptionsBuilder<T>()).Build()) { }

    protected override async Task EnsureQueueCreatedAsync(CancellationToken cancellationToken = default)
    {
        if (_queueCreated)
            return;

        using (await _lock.LockAsync().AnyContext())
        {
            if (_queueCreated)
                return;

            var sw = Stopwatch.StartNew();
            await Task.WhenAll(
                _queueReference.CreateIfNotExistsAsync(),
                _deadletterQueueReference.CreateIfNotExistsAsync()
            ).AnyContext();
            _queueCreated = true;

            sw.Stop();
            _logger.LogTrace("Ensure queue exists took {Elapsed:g}", sw.Elapsed);
        }
    }

    protected override async Task<string> EnqueueImplAsync(T data, QueueEntryOptions options)
    {
        if (!await OnEnqueuingAsync(data, options).AnyContext())
            return null;

        Interlocked.Increment(ref _enqueuedCount);
        var message = new CloudQueueMessage(_serializer.SerializeToBytes(data));
        await _queueReference.AddMessageAsync(message, null, options.DeliveryDelay, null, null).AnyContext();

        var entry = new QueueEntry<T>(message.Id, options.CorrelationId, data, this, _timeProvider.GetLocalNow().UtcDateTime, 0);
        await OnEnqueuedAsync(entry).AnyContext();

        return message.Id;
    }

    protected override async Task<IQueueEntry<T>> DequeueImplAsync(CancellationToken linkedCancellationToken)
    {
        var message = await _queueReference.GetMessageAsync(_options.WorkItemTimeout, null, null, !linkedCancellationToken.IsCancellationRequested ? linkedCancellationToken : CancellationToken.None).AnyContext();
        if (message == null)
        {
            var sw = Stopwatch.StartNew();
            var lastReport = DateTime.Now;
            _logger.LogTrace("No message available to dequeue, waiting...");

            while (message == null && !linkedCancellationToken.IsCancellationRequested)
            {
                if (DateTime.Now.Subtract(lastReport) > TimeSpan.FromSeconds(10))
                    _logger.LogTrace("Still waiting for message to dequeue: {Elapsed:g}", sw.Elapsed);

                try
                {
                    if (!linkedCancellationToken.IsCancellationRequested)
                        await _timeProvider.Delay(_options.DequeueInterval, linkedCancellationToken).AnyContext();
                }
                catch (OperationCanceledException) { }

                message = await _queueReference.GetMessageAsync(_options.WorkItemTimeout, null, null, !linkedCancellationToken.IsCancellationRequested ? linkedCancellationToken : CancellationToken.None).AnyContext();
            }

            sw.Stop();
            _logger.LogTrace("Waited to dequeue message: {Elapsed:g}", sw.Elapsed);
        }

        if (message == null)
        {
            _logger.LogTrace("No message was dequeued");
            return null;
        }

        _logger.LogTrace("Dequeued message {QueueEntryId}", message.Id);
        Interlocked.Increment(ref _dequeuedCount);
        var data = _serializer.Deserialize<T>(message.AsBytes);
        var entry = new AzureStorageQueueEntry<T>(message, data, this);
        await OnDequeuedAsync(entry).AnyContext();
        return entry;
    }

    public override async Task RenewLockAsync(IQueueEntry<T> entry)
    {
        _logger.LogDebug("Queue {QueueName} renew lock item: {QueueEntryId}", _options.Name, entry.Id);
        var azureQueueEntry = ToAzureEntryWithCheck(entry);
        await _queueReference.UpdateMessageAsync(azureQueueEntry.UnderlyingMessage, _options.WorkItemTimeout, MessageUpdateFields.Visibility).AnyContext();
        await OnLockRenewedAsync(entry).AnyContext();
        _logger.LogTrace("Renew lock done: {QueueEntryId}", entry.Id);
    }

    public override async Task CompleteAsync(IQueueEntry<T> entry)
    {
        _logger.LogDebug("Queue {QueueName} complete item: {QueueEntryId}", _options.Name, entry.Id);
        if (entry.IsAbandoned || entry.IsCompleted)
            throw new InvalidOperationException("Queue entry has already been completed or abandoned.");

        var azureQueueEntry = ToAzureEntryWithCheck(entry);
        await _queueReference.DeleteMessageAsync(azureQueueEntry.UnderlyingMessage).AnyContext();

        Interlocked.Increment(ref _completedCount);
        entry.MarkCompleted();
        await OnCompletedAsync(entry).AnyContext();
        _logger.LogTrace("Complete done: {QueueEntryId}", entry.Id);
    }

    public override async Task AbandonAsync(IQueueEntry<T> entry)
    {
        _logger.LogDebug("Queue {QueueName} ({QueueId}) abandon item: {QueueEntryId}", _options.Name, QueueId, entry.Id);
        if (entry.IsAbandoned || entry.IsCompleted)
            throw new InvalidOperationException("Queue entry has already been completed or abandoned.");

        var azureQueueEntry = ToAzureEntryWithCheck(entry);
        if (azureQueueEntry.Attempts > _options.Retries)
        {
            await Task.WhenAll(
                _queueReference.DeleteMessageAsync(azureQueueEntry.UnderlyingMessage),
                _deadletterQueueReference.AddMessageAsync(azureQueueEntry.UnderlyingMessage)
            ).AnyContext();
        }
        else
        {
            // Make the item visible immediately
            await _queueReference.UpdateMessageAsync(azureQueueEntry.UnderlyingMessage, TimeSpan.Zero, MessageUpdateFields.Visibility).AnyContext();
        }

        Interlocked.Increment(ref _abandonedCount);
        entry.MarkAbandoned();
        await OnAbandonedAsync(entry).AnyContext();
        _logger.LogTrace("Abandon complete: {QueueEntryId}", entry.Id);
    }

    protected override Task<IEnumerable<T>> GetDeadletterItemsImplAsync(CancellationToken cancellationToken)
    {
        throw new NotImplementedException("Azure Storage Queues do not support retrieving the entire queue");
    }

    protected override async Task<QueueStats> GetQueueStatsImplAsync()
    {
        if (_queueReference == null || _deadletterQueueReference == null || !_queueCreated)
            return new QueueStats
            {
                Queued = 0,
                Working = 0,
                Deadletter = 0,
                Enqueued = _enqueuedCount,
                Dequeued = _dequeuedCount,
                Completed = _completedCount,
                Abandoned = _abandonedCount,
                Errors = _workerErrorCount,
                Timeouts = 0
            };

        var sw = Stopwatch.StartNew();
        await Task.WhenAll(
            _queueReference.FetchAttributesAsync(),
            _deadletterQueueReference.FetchAttributesAsync()
        ).AnyContext();
        sw.Stop();
        _logger.LogTrace("Fetching stats took {Elapsed:g}", sw.Elapsed);

        return new QueueStats
        {
            Queued = _queueReference.ApproximateMessageCount.GetValueOrDefault(),
            Working = 0,
            Deadletter = _deadletterQueueReference.ApproximateMessageCount.GetValueOrDefault(),
            Enqueued = _enqueuedCount,
            Dequeued = _dequeuedCount,
            Completed = _completedCount,
            Abandoned = _abandonedCount,
            Errors = _workerErrorCount,
            Timeouts = 0
        };
    }

    protected override QueueStats GetMetricsQueueStats()
    {
        if (_queueReference == null || _deadletterQueueReference == null || !_queueCreated)
            return new QueueStats
            {
                Queued = 0,
                Working = 0,
                Deadletter = 0,
                Enqueued = _enqueuedCount,
                Dequeued = _dequeuedCount,
                Completed = _completedCount,
                Abandoned = _abandonedCount,
                Errors = _workerErrorCount,
                Timeouts = 0
            };

        var sw = Stopwatch.StartNew();
        _queueReference.FetchAttributes();
        _deadletterQueueReference.FetchAttributes();
        sw.Stop();
        _logger.LogTrace("Fetching stats took {Elapsed:g}", sw.Elapsed);

        return new QueueStats
        {
            Queued = _queueReference.ApproximateMessageCount.GetValueOrDefault(),
            Working = 0,
            Deadletter = _deadletterQueueReference.ApproximateMessageCount.GetValueOrDefault(),
            Enqueued = _enqueuedCount,
            Dequeued = _dequeuedCount,
            Completed = _completedCount,
            Abandoned = _abandonedCount,
            Errors = _workerErrorCount,
            Timeouts = 0
        };
    }

    public override async Task DeleteQueueAsync()
    {
        var sw = Stopwatch.StartNew();
        await Task.WhenAll(
            _queueReference.DeleteIfExistsAsync(),
            _deadletterQueueReference.DeleteIfExistsAsync()
        ).AnyContext();
        _queueCreated = false;

        _enqueuedCount = 0;
        _dequeuedCount = 0;
        _completedCount = 0;
        _abandonedCount = 0;
        _workerErrorCount = 0;

        sw.Stop();
        _logger.LogTrace("Deleting queue took {Elapsed:g}", sw.Elapsed);
    }

    protected override void StartWorkingImpl(Func<IQueueEntry<T>, CancellationToken, Task> handler, bool autoComplete, CancellationToken cancellationToken)
    {
        if (handler == null)
            throw new ArgumentNullException(nameof(handler));

        var linkedCancellationToken = GetLinkedDisposableCancellationTokenSource(cancellationToken);

        Task.Run(async () =>
        {
            _logger.LogTrace("WorkerLoop Start {QueueName}", _options.Name);

            while (!linkedCancellationToken.IsCancellationRequested)
            {
                _logger.LogTrace("WorkerLoop Signaled {QueueName}", _options.Name);

                IQueueEntry<T> queueEntry = null;
                try
                {
                    queueEntry = await DequeueImplAsync(linkedCancellationToken.Token).AnyContext();
                }
                catch (OperationCanceledException) { }

                if (linkedCancellationToken.IsCancellationRequested || queueEntry == null)
                    continue;

                try
                {
                    await handler(queueEntry, linkedCancellationToken.Token).AnyContext();
                    if (autoComplete && !queueEntry.IsAbandoned && !queueEntry.IsCompleted)
                        await queueEntry.CompleteAsync().AnyContext();
                }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref _workerErrorCount);
                    _logger.LogError(ex, "Worker error: {Message}", ex.Message);

                    if (!queueEntry.IsAbandoned && !queueEntry.IsCompleted)
                        await queueEntry.AbandonAsync().AnyContext();
                }
            }

            _logger.LogTrace("Worker exiting: {QueueName} Cancel Requested: {IsCancellationRequested}", _queueReference.Name, linkedCancellationToken.IsCancellationRequested);
        }, linkedCancellationToken.Token).ContinueWith(t => linkedCancellationToken.Dispose());
    }

    private static AzureStorageQueueEntry<T> ToAzureEntryWithCheck(IQueueEntry<T> queueEntry)
    {
        if (!(queueEntry is AzureStorageQueueEntry<T> azureQueueEntry))
            throw new ArgumentException($"Unknown entry type. Can only process entries of type '{nameof(AzureStorageQueueEntry<T>)}'");

        return azureQueueEntry;
    }
}
