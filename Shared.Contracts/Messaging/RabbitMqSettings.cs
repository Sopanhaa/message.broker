using RabbitMQ.Client;

namespace Shared.Contracts.Messaging;

// Shared by both applications so queue arguments always match.
public sealed class RabbitMqSettings
{
    public string HostName { get; set; } = "localhost";
    public int Port { get; set; } = 5672;
    public string UserName { get; set; } = "";
    public string Password { get; set; } = "";
    public string VirtualHost { get; set; } = "/";
    public string QueueName { get; set; } = "orders";
    public int PublishTimeoutSeconds { get; set; } = 10;
    public string DeadLetterExchange => $"{QueueName}.dlx";
    public string DeadLetterQueue => $"{QueueName}.dead-letter";

    // Retry queues expire into the existing dead-letter exchange. A dedicated
    // routing key binds the main queue to that exchange so an expiring retry
    // returns to the original processing queue instead of the DLQ.
    public string RetryReturnRoutingKey => $"{QueueName}.retry.return";
    public IReadOnlyList<RetryQueue> RetryQueues => RetrySchedule.ForQueue(QueueName);

    // Status updates travel on their own queue so a slow order process cannot delay them.
    public string StatusQueueName => $"{QueueName}.status";
    public string StatusDeadLetterExchange => $"{StatusQueueName}.dlx";
    public string StatusDeadLetterQueue => $"{StatusQueueName}.dead-letter";
    public string StatusRetryReturnRoutingKey => $"{StatusQueueName}.retry.return";
    public IReadOnlyList<RetryQueue> StatusRetryQueues => RetrySchedule.ForQueue(StatusQueueName);

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(HostName) || string.IsNullOrWhiteSpace(UserName) ||
            string.IsNullOrWhiteSpace(Password) || string.IsNullOrWhiteSpace(VirtualHost) ||
            string.IsNullOrWhiteSpace(QueueName) || Port is < 1 or > 65535 ||
            PublishTimeoutSeconds is < 1 or > 300)
            throw new InvalidOperationException("Check the RabbitMq configuration: host, port, credentials, virtual host, queue and timeout are required.");
    }

    public ConnectionFactory CreateConnectionFactory() => new()
    {
        HostName = HostName,
        Port = Port,
        UserName = UserName,
        Password = Password,
        VirtualHost = VirtualHost,
        // Each app explicitly reconnects, including after the first connection fails.
        AutomaticRecoveryEnabled = false,
        RequestedConnectionTimeout = TimeSpan.FromSeconds(5),
        ContinuationTimeout = TimeSpan.FromSeconds(10),
        RequestedHeartbeat = TimeSpan.FromSeconds(15)
    };
}
