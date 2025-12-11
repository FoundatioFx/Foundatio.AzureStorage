using System;
using System.Threading.Tasks;
using Azure.Core;
using Foundatio.Queues;
using Foundatio.Tests.Queue;
using Foundatio.Tests.Utility;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.Azure.Tests.Queue;

public class AzureStorageQueueTests : QueueTestBase
{
    private readonly string _queueName = "foundatio-" + Guid.NewGuid().ToString("N").Substring(10);

    public AzureStorageQueueTests(ITestOutputHelper output) : base(output)
    {
    }

    protected override IQueue<SimpleWorkItem> GetQueue(int retries = 1, TimeSpan? workItemTimeout = null, TimeSpan? retryDelay = null, int[] retryMultipliers = null, int deadLetterMaxItems = 100, bool runQueueMaintenance = true, TimeProvider timeProvider = null)
    {
        string connectionString = Configuration.GetConnectionString("AzureStorageConnectionString");
        if (String.IsNullOrEmpty(connectionString))
            return null;

        // TODO: We could use ExponentialRetry here if we wanted to test that as well. Could it use the same as options (into a shared helper) public Func<int, TimeSpan> RetryDelay { get; set; } = attempt =>
        // TimeSpan.FromSeconds(Math.Pow(2, attempt)) + TimeSpan.FromMilliseconds(Random.Shared.Next(0, 100));

        _logger.LogDebug("Queue Id: {Name}", _queueName);
        return new AzureStorageQueue<SimpleWorkItem>(o => o
            .ConnectionString(connectionString)
            .Name(_queueName)
            .Retries(retries)
            .RetryDelay(_ => retries <= 0 ? TimeSpan.Zero : retryDelay.GetValueOrDefault(TimeSpan.FromMinutes(1)))
            .WorkItemTimeout(workItemTimeout.GetValueOrDefault(TimeSpan.FromMinutes(5)))
            .DequeueInterval(TimeSpan.FromSeconds(1))
            .MetricsPollingInterval(TimeSpan.Zero)
            .TimeProvider(timeProvider)
            .LoggerFactory(Log));
    }

    protected override Task CleanupQueueAsync(IQueue<SimpleWorkItem> queue)
    {
        // Don't delete the queue, it's super expensive and will be cleaned up later.
        queue?.Dispose();
        return Task.CompletedTask;
    }

    [Fact]
    public override Task CanQueueAndDequeueWorkItemAsync()
    {
        return base.CanQueueAndDequeueWorkItemAsync();
    }

    [Fact]
    public override Task CanQueueAndDequeueWorkItemWithDelayAsync()
    {
        return base.CanQueueAndDequeueWorkItemWithDelayAsync();
    }

    [Fact(Skip = "Azure Storage Queue does not support CorrelationId or Properties - only message body is persisted")]
    public override Task CanUseQueueOptionsAsync()
    {
        return base.CanUseQueueOptionsAsync();
    }

    [Fact]
    public override Task CanDiscardDuplicateQueueEntriesAsync()
    {
        return base.CanDiscardDuplicateQueueEntriesAsync();
    }

    [Fact]
    public override Task CanDequeueWithCancelledTokenAsync()
    {
        return base.CanDequeueWithCancelledTokenAsync();
    }

    [Fact(Skip = "Dequeue Time takes forever")]
    public override Task CanDequeueEfficientlyAsync()
    {
        return base.CanDequeueEfficientlyAsync();
    }

    [Fact(Skip = "Dequeue Time takes forever")]
    public override Task CanResumeDequeueEfficientlyAsync()
    {
        return base.CanResumeDequeueEfficientlyAsync();
    }

    [Fact]
    public override Task CanQueueAndDequeueMultipleWorkItemsAsync()
    {
        return base.CanQueueAndDequeueMultipleWorkItemsAsync();
    }

    [Fact]
    public override Task WillNotWaitForItemAsync()
    {
        return base.WillNotWaitForItemAsync();
    }

    [Fact]
    public override Task WillWaitForItemAsync()
    {
        return base.WillWaitForItemAsync();
    }

    [Fact]
    public override Task DequeueWaitWillGetSignaledAsync()
    {
        return base.DequeueWaitWillGetSignaledAsync();
    }

    [Fact]
    public override Task CanUseQueueWorkerAsync()
    {
        return base.CanUseQueueWorkerAsync();
    }

    [Fact]
    public override Task CanHandleErrorInWorkerAsync()
    {
        return base.CanHandleErrorInWorkerAsync();
    }

    [Fact(Skip = "Azure Storage Queue handles visibility timeout natively; no client-side auto-abandon")]
    public override Task WorkItemsWillTimeoutAsync()
    {
        return base.WorkItemsWillTimeoutAsync();
    }

    [Fact]
    public override Task WorkItemsWillGetMovedToDeadletterAsync()
    {
        return base.WorkItemsWillGetMovedToDeadletterAsync();
    }

    [Fact]
    public override Task CanAutoCompleteWorkerAsync()
    {
        return base.CanAutoCompleteWorkerAsync();
    }

    [Fact]
    public override Task CanHaveMultipleQueueInstancesAsync()
    {
        return base.CanHaveMultipleQueueInstancesAsync();
    }

    [Fact]
    public override Task CanDelayRetryAsync()
    {
        return base.CanDelayRetryAsync();
    }

    [Fact]
    public override Task CanRunWorkItemWithMetricsAsync()
    {
        return base.CanRunWorkItemWithMetricsAsync();
    }

    [Fact]
    public override Task CanRenewLockAsync()
    {
        return base.CanRenewLockAsync();
    }

    [Fact]
    public override Task CanAbandonQueueEntryOnceAsync()
    {
        return base.CanAbandonQueueEntryOnceAsync();
    }

    [Fact]
    public override Task CanCompleteQueueEntryOnceAsync()
    {
        return base.CanCompleteQueueEntryOnceAsync();
    }

    [Fact]
    public override Task CanDequeueWithLockingAsync()
    {
        return base.CanDequeueWithLockingAsync();
    }

    [Fact]
    public override Task CanHaveMultipleQueueInstancesWithLockingAsync()
    {
        return base.CanHaveMultipleQueueInstancesWithLockingAsync();
    }

    [Fact]
    public override Task MaintainJobNotAbandon_NotWorkTimeOutEntry()
    {
        return base.MaintainJobNotAbandon_NotWorkTimeOutEntry();
    }

    [Fact]
    public override Task VerifyRetryAttemptsAsync()
    {
        return base.VerifyRetryAttemptsAsync();
    }

    [Fact]
    public override Task VerifyDelayedRetryAttemptsAsync()
    {
        return base.VerifyDelayedRetryAttemptsAsync();
    }

    [Fact(Skip = "Azure Storage Queue handles visibility timeout natively; no client-side auto-abandon")]
    public override Task CanHandleAutoAbandonInWorker()
    {
        return base.CanHandleAutoAbandonInWorker();
    }
}
