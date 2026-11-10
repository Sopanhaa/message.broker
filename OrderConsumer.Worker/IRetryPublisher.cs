using RabbitMQ.Client;

namespace OrderConsumer.Worker;

// Everything needed to republish a failed delivery to a retry queue without
// changing its identity. SourceProperties carries the original headers,
// MessageId, correlation ID and message type.
public sealed record RetryMessage(
    ReadOnlyMemory<byte> Body,
    string? MessageId,
    string? CorrelationId,
    string? MessageType,
    int Attempt,
    IReadOnlyBasicProperties? SourceProperties);

public interface IRetryPublisher
{
    Task PublishAsync(RetryMessage message, string queueName, CancellationToken cancellationToken);
}
