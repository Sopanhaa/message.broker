using System.Text.Json;
using RabbitMQ.Client;
using Shared.Contracts.Events;

namespace OrderConsumer.Worker;

// Handles one delivery from a fanout or topic demo queue.
// Routing demo messages have no retry schedule; failures go to the routing DLQ.
public sealed class RoutingMessageHandler(ILogger<RoutingMessageHandler> logger)
{
    public async Task HandleAsync(IChannel channel, ReadOnlyMemory<byte> body, ulong deliveryTag,
        string queueName, CancellationToken cancellationToken)
    {
        OrderRoutingEvent routingEvent;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            // Deserialize before returning from the callback: RabbitMQ owns the body memory.
            routingEvent = JsonSerializer.Deserialize<OrderRoutingEvent>(body.Span, JsonSerializerOptions.Web)
                ?? throw new JsonException("OrderRoutingEvent payload was null.");
            if (!OrderRoutingEvent.IsValid(routingEvent))
                throw new JsonException("OrderRoutingEvent payload contains invalid or missing values.");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Channel disposal requeues unfinished work without acknowledging it.
            return;
        }
        catch (Exception ex)
        {
            if (cancellationToken.IsCancellationRequested) return;

            logger.LogError(ex,
                "Routing delivery {DeliveryTag} from {Queue} has an invalid payload; rejecting to the routing dead-letter queue",
                deliveryTag, queueName);
            await channel.BasicNackAsync(deliveryTag, multiple: false, requeue: false,
                cancellationToken: cancellationToken);
            return;
        }

        logger.LogInformation("[{Queue}] received {EventType} event {EventId} at {OccurredAtUtc}",
            queueName, routingEvent.EventType, routingEvent.EventId, routingEvent.OccurredAtUtc);

        // A failed acknowledgment has an unknown outcome. Let the worker reconnect
        // rather than attempting a second settlement.
        await channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken: cancellationToken);
        logger.LogInformation("Processed and acknowledged routing message {EventId} from {Queue}",
            routingEvent.EventId, queueName);
    }
}
