using Shared.Contracts.Events;

namespace OrderProducer.Api.Services;

public interface IMessagePublisher
{
    Task PublishAsync(OrderCreated order, CancellationToken cancellationToken);
}

public sealed class MessagePublishException(string message, Exception innerException)
    : Exception(message, innerException);
