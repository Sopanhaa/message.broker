using System.Text.Json;
using RabbitMQ.Client;
using Shared.Contracts.Events;
using Shared.Contracts.Messaging;

namespace OrderConsumer.Worker;

public sealed class OrderStatusMessageHandler(
    IOrderStatusProcessor processor, RabbitMqSettings settings, ILogger<OrderStatusMessageHandler> logger)
{
    public async Task HandleAsync(IChannel channel, ReadOnlyMemory<byte> body, ulong deliveryTag,
        CancellationToken cancellationToken)
    {
        OrderStatusChanged update;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            // Deserialize before returning from the callback: RabbitMQ owns the body memory.
            update = JsonSerializer.Deserialize<OrderStatusChanged>(body.Span, JsonSerializerOptions.Web)
                ?? throw new JsonException("OrderStatusChanged payload was null.");
            if (!OrderStatusChanged.IsValid(update))
                throw new JsonException("OrderStatusChanged payload contains invalid or missing values.");

            update = update with
            {
                Detail = string.IsNullOrWhiteSpace(update.Detail) ? null : update.Detail.Trim()
            };
            await processor.ProcessAsync(update, cancellationToken);
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

            logger.LogError(ex, "Status delivery {DeliveryTag} failed; rejecting to {DeadLetterQueue}",
                deliveryTag, settings.StatusDeadLetterQueue);
            await channel.BasicNackAsync(deliveryTag, multiple: false, requeue: false,
                cancellationToken: cancellationToken);
            return;
        }

        // A failed acknowledgment has an unknown outcome. Let the worker reconnect;
        // attempting to reject the same delivery could settle it twice.
        await channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken: cancellationToken);
        logger.LogInformation("Processed and acknowledged status update for order {OrderId}", update.OrderId);
    }
}
