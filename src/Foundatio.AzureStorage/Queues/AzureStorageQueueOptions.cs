using System;
using Azure.Core;

namespace Foundatio.Queues;

public class AzureStorageQueueOptions<T> : SharedQueueOptions<T> where T : class
{
    public string ConnectionString { get; set; }

    /// <summary>
    /// The interval to wait between polling for new messages when the queue is empty.
    /// </summary>
    public TimeSpan DequeueInterval { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// A function that returns the delay before retrying a failed message based on the attempt number.
    /// Default is exponential backoff with jitter: 2^attempt seconds + random 0-100ms.
    /// </summary>
    public Func<int, TimeSpan> RetryDelay { get; set; } = attempt =>
        TimeSpan.FromSeconds(Math.Pow(2, attempt)) + TimeSpan.FromMilliseconds(Random.Shared.Next(0, 100));

    /// <summary>
    /// When true, messages are Base64-encoded for backward compatibility with the legacy
    /// Microsoft.Azure.Storage.Queue SDK (v11). The v11 SDK encoded all messages as Base64
    /// by default, while the v12 Azure.Storage.Queues SDK does not.
    ///
    /// <para><b>Default:</b> false (v12 behavior - no encoding)</para>
    ///
    /// <para><b>When to enable:</b> Only enable this if you have existing messages in your
    /// queue that were written using the v11 SDK and need to be read during migration.</para>
    ///
    /// <para><b>Migration path:</b></para>
    /// <list type="number">
    ///   <item><description>Enable UseBase64Encoding=true to read existing v11 messages</description></item>
    ///   <item><description>Process all existing messages from the queue</description></item>
    ///   <item><description>Once queue is empty, disable UseBase64Encoding (set to false)</description></item>
    ///   <item><description>All new messages will use raw encoding (v12 default)</description></item>
    /// </list>
    ///
    /// <para><b>Deprecation notice:</b> This option exists solely for migration purposes
    /// and may be removed in a future major version. Plan to migrate away from Base64
    /// encoding as soon as practical.</para>
    /// </summary>
    public bool UseBase64Encoding { get; set; }

    /// <summary>
    /// Optional action to configure Azure SDK retry options.
    /// Default Azure SDK retry settings: MaxRetries=3, Delay=0.8s, MaxDelay=1min, Mode=Exponential.
    /// </summary>
    /// <example>
    /// <code>
    /// options.ConfigureRetry = retry =>
    /// {
    ///     retry.MaxRetries = 5;
    ///     retry.Delay = TimeSpan.FromSeconds(1);
    ///     retry.Mode = RetryMode.Exponential;
    /// };
    /// </code>
    /// </example>
    public Action<RetryOptions> ConfigureRetry { get; set; }
}

public class AzureStorageQueueOptionsBuilder<T> : SharedQueueOptionsBuilder<T, AzureStorageQueueOptions<T>, AzureStorageQueueOptionsBuilder<T>> where T : class
{
    public AzureStorageQueueOptionsBuilder<T> ConnectionString(string connectionString)
    {
        ArgumentException.ThrowIfNullOrEmpty(connectionString);

        Target.ConnectionString = connectionString;
        return this;
    }

    public AzureStorageQueueOptionsBuilder<T> DequeueInterval(TimeSpan dequeueInterval)
    {
        Target.DequeueInterval = dequeueInterval;
        return this;
    }

    /// <summary>
    /// Sets a custom retry delay function for failed messages.
    /// </summary>
    /// <param name="retryDelay">A function that takes the attempt number and returns the delay before the next retry.</param>
    public AzureStorageQueueOptionsBuilder<T> RetryDelay(Func<int, TimeSpan> retryDelay)
    {
        ArgumentNullException.ThrowIfNull(retryDelay);

        Target.RetryDelay = retryDelay;
        return this;
    }

    /// <summary>
    /// Enables Base64 message encoding for backward compatibility with the legacy v11 SDK.
    /// See <see cref="AzureStorageQueueOptions{T}.UseBase64Encoding"/> for migration guidance.
    /// </summary>
    public AzureStorageQueueOptionsBuilder<T> UseBase64Encoding(bool useBase64Encoding = true)
    {
        Target.UseBase64Encoding = useBase64Encoding;
        return this;
    }

    /// <summary>
    /// Configures Azure SDK retry options for transient failure handling.
    /// </summary>
    /// <param name="configure">Action to configure retry options.</param>
    /// <example>
    /// <code>
    /// .ConfigureRetry(retry =>
    /// {
    ///     retry.MaxRetries = 5;
    ///     retry.Delay = TimeSpan.FromSeconds(1);
    /// })
    /// </code>
    /// </example>
    public AzureStorageQueueOptionsBuilder<T> ConfigureRetry(Action<RetryOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        Target.ConfigureRetry = configure;
        return this;
    }
}
