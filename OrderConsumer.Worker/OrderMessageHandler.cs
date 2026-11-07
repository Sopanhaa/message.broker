using System.Text.Json;
using RabbitMQ.Client;
using Shared.Contracts.Events;
using Shared.Contracts.Messaging;

namespace OrderConsumer.Worker;

public sealed class OrderMessageHandler(
    IOrderProcessor processor, RabbitMqSettings settings, ILogger<OrderMessageHandler> logger)
{
    public async Task HandleAsync(IChannel channel, ReadOnlyMemory<byte> body, ulong deliveryTag,
        CancellationToken cancellationToken)
    {
        OrderCreated order;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            // Deserialize before returning from the callback: RabbitMQ owns the body memory.
            order = JsonSerializer.Deserialize<OrderCreated>(body.Span, JsonSerializerOptions.Web)
                ?? throw new JsonException("OrderCreated payload was null.");
            if (order.OrderId == Guid.Empty || string.IsNullOrWhiteSpace(order.CustomerName) ||
                order.CustomerName.Trim().Length > 100 || order.TotalAmount <= 0 || order.CreatedAtUtc == default ||
                !OrderProduct.IsValid(order.Products, order.TotalAmount))
                throw new JsonException("OrderCreated payload contains invalid or missing values.");

            await processor.ProcessAsync(order, cancellationToken);
            cancellationToken.ThrowIfCancellationRequested();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Channel disposal requeues unfinished work without acknowledging it.
            return;
        }
        catch (Exception ex)
        {
            // Shutdown takes precedence over rejection, including when a processor throws
            // a different exception while cancellation is in progress.
            if (cancellationToken.IsCancellationRequested) return;

            logger.LogError(ex, "Delivery {DeliveryTag} failed; rejecting to {DeadLetterQueue}",
                deliveryTag, settings.DeadLetterQueue);
            await channel.BasicNackAsync(deliveryTag, multiple: false, requeue: false,
                cancellationToken: cancellationToken);
            return;
        }

        // A failed acknowledgment has an unknown outcome. Let the worker reconnect;
        // attempting to reject the same delivery could settle it twice.
        await channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken: cancellationToken);
        logger.LogInformation("Processed and acknowledged order {OrderId}", order.OrderId);
    }
}
