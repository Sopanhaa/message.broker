using RabbitMQ.Client;
using Shared.Contracts.Messaging;

namespace OrderConsumer.Worker;

// A retry publish must be confirmed before the original delivery is acknowledged.
// The channel is created with publisher confirmations enabled by the Worker.
public sealed class RabbitMqRetryPublisher(IChannel channel, ILogger<RabbitMqRetryPublisher> logger)
    : IRetryPublisher
{
    // One channel must not be used by two concurrent publish operations.
    // Order and status consumers share this publisher, so serialize access.
    private readonly SemaphoreSlim _gate = new(1, 1);

    public async Task PublishAsync(RetryMessage message, string queueName, CancellationToken cancellationToken)
    {
        var source = message.SourceProperties;
        var headers = source?.Headers is null
            ? new Dictionary<string, object?>()
            : new Dictionary<string, object?>(source.Headers);
        headers[RetryHeaderInspector.RetryAttemptHeader] = (long)message.Attempt;

        var properties = new BasicProperties
        {
            Persistent = true,
            ContentType = source?.ContentType ?? "application/json",
            ContentEncoding = source?.ContentEncoding,
            Type = source?.Type ?? message.MessageType,
            MessageId = message.MessageId ?? source?.MessageId,
            CorrelationId = message.CorrelationId ?? source?.CorrelationId,
            ReplyTo = source?.ReplyTo,
            AppId = source?.AppId,
            Headers = headers
        };
        if (source is not null && source.Timestamp.UnixTime > 0)
            properties.Timestamp = source.Timestamp;

        var entered = false;
        try
        {
            await _gate.WaitAsync(cancellationToken);
            entered = true;

            // Confirmation tracking makes this await complete only after the
            // broker confirms the retry publish. mandatory=true also reports an
            // unroutable message (for example, a missing retry queue).
            await channel.BasicPublishAsync(exchange: "", routingKey: queueName, mandatory: true,
                basicProperties: properties, body: message.Body, cancellationToken: cancellationToken);

            logger.LogInformation("Broker confirmed retry attempt {Attempt} for message {MessageId} in {Queue}",
                message.Attempt, properties.MessageId, queueName);
        }
        finally
        {
            if (entered) _gate.Release();
        }
    }
}
