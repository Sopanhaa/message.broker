using Shared.Contracts.Events;

namespace OrderProducer.Api.Services;

public interface IMessagePublisher
{
    Task PublishAsync(OrderCreated order, CancellationToken cancellationToken);

    Task PublishStatusAsync(OrderStatusChanged status, CancellationToken cancellationToken);

    Task PublishFanoutAsync(OrderRoutingEvent routingEvent, CancellationToken cancellationToken);

    Task PublishTopicAsync(OrderRoutingEvent routingEvent, string routingKey,
        CancellationToken cancellationToken);
}

public sealed class MessagePublishException(string message, Exception innerException)
    : Exception(message, innerException);
