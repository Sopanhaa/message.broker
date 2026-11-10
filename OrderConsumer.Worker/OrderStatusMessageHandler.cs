using System.Text.Json;
using OrderPersistence;
using RabbitMQ.Client;
using Shared.Contracts.Events;
using Shared.Contracts.Messaging;

namespace OrderConsumer.Worker;

public sealed class OrderStatusMessageHandler(
    IOrderStatusProcessor processor, IOrderStore store, RabbitMqSettings settings,
    ILogger<OrderStatusMessageHandler> logger)
{
    public async Task HandleAsync(IChannel channel, ReadOnlyMemory<byte> body, ulong deliveryTag,
        CancellationToken cancellationToken, string? messageId = null)
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
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Channel disposal requeues unfinished work without acknowledging it.
            return;
        }
        catch (Exception ex)
        {
            if (cancellationToken.IsCancellationRequested) return;

            logger.LogError(ex, "Status delivery {DeliveryTag} failed; rejecting to {DeadLetterQueue}",
                deliveryTag, settings.StatusDeadLetterQueue);
            await channel.BasicNackAsync(deliveryTag, multiple: false, requeue: false,
                cancellationToken: cancellationToken);
            return;
        }

        // A redelivery after a lost ACK is expected. Skip work already completed for this message.
        // This check is outside the processing try so a failed ACK reconnects instead of NACKing twice.
        if (messageId is not null && await store.IsProcessedAsync(messageId, cancellationToken))
        {
            await channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken: cancellationToken);
            logger.LogInformation("Skipped duplicate OrderStatusChanged {MessageId} for order {OrderId}",
                messageId, update.OrderId);
            return;
        }

        try
        {
            await processor.ProcessAsync(update, cancellationToken);

            // Recorded after the work succeeds. A crash before this line causes redelivery and
            // reprocessing; status writes are timestamp-guarded, so that is safe.
            if (messageId is not null)
                await store.MarkProcessedAsync(messageId, update.OrderId, nameof(OrderStatusChanged),
                    cancellationToken);

            cancellationToken.ThrowIfCancellationRequested();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
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
