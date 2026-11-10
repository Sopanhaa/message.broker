using System.Text.Json;
using OrderPersistence;
using RabbitMQ.Client;
using Shared.Contracts.Events;
using Shared.Contracts.Messaging;

namespace OrderConsumer.Worker;

public sealed class OrderMessageHandler(
    IOrderProcessor processor, IOrderStore store, RabbitMqSettings settings,
    ILogger<OrderMessageHandler> logger, RetryScheduler retryScheduler)
{
    public async Task HandleAsync(IChannel channel, ReadOnlyMemory<byte> body, ulong deliveryTag,
        CancellationToken cancellationToken, string? messageId = null,
        IReadOnlyBasicProperties? properties = null, IRetryPublisher? retryPublisher = null)
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
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Channel disposal requeues unfinished work without acknowledging it.
            return;
        }
        catch (Exception ex)
        {
            if (cancellationToken.IsCancellationRequested) return;

            // Invalid JSON or schema is permanent: retrying cannot fix the payload.
            logger.LogError(ex, "Delivery {DeliveryTag} has an invalid payload; rejecting to {DeadLetterQueue}",
                deliveryTag, settings.DeadLetterQueue);
            await channel.BasicNackAsync(deliveryTag, multiple: false, requeue: false,
                cancellationToken: cancellationToken);
            return;
        }

        // A redelivery after a lost ACK is expected. Skip work already completed for this message.
        // Settlement is outside the try so a failed ACK reconnects instead of NACKing twice.
        bool alreadyProcessed;
        try
        {
            alreadyProcessed = messageId is not null &&
                await store.IsProcessedAsync(messageId, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex) when (ProcessingFailureClassifier.IsTransient(ex))
        {
            if (cancellationToken.IsCancellationRequested) return;

            await ScheduleRetryAsync(channel, body, deliveryTag, messageId, properties, retryPublisher,
                ex, cancellationToken);
            return;
        }
        catch (Exception ex)
        {
            if (cancellationToken.IsCancellationRequested) return;

            // The deduplication check could not be completed. Treat an unexpected
            // failure like any other non-recoverable failure and keep it in the DLQ
            // rather than silently discarding it.
            logger.LogError(ex,
                "Deduplication check for delivery {DeliveryTag} failed; rejecting to {DeadLetterQueue}",
                deliveryTag, settings.DeadLetterQueue);
            await channel.BasicNackAsync(deliveryTag, multiple: false, requeue: false,
                cancellationToken: cancellationToken);
            return;
        }

        if (alreadyProcessed)
        {
            await channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken: cancellationToken);
            logger.LogInformation("Skipped duplicate OrderCreated {MessageId} for order {OrderId}",
                messageId, order.OrderId);
            return;
        }

        try
        {
            await processor.ProcessAsync(order, cancellationToken);

            // Recorded after the work succeeds. A crash before this line causes redelivery and
            // reprocessing; order status writes are timestamp-guarded, so that is safe.
            if (messageId is not null)
                await store.MarkProcessedAsync(messageId, order.OrderId, nameof(OrderCreated), cancellationToken);

            cancellationToken.ThrowIfCancellationRequested();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex) when (ProcessingFailureClassifier.IsTransient(ex))
        {
            if (cancellationToken.IsCancellationRequested) return;

            // Do not mark the message processed: the retry must do the work again.
            await ScheduleRetryAsync(channel, body, deliveryTag, messageId, properties, retryPublisher,
                ex, cancellationToken);
            return;
        }
        catch (Exception ex)
        {
            if (cancellationToken.IsCancellationRequested) return;

            // Unexpected failures use the bounded DLQ policy: log once and retain
            // the message for inspection instead of retrying forever.
            logger.LogError(ex, "Delivery {DeliveryTag} failed permanently; rejecting to {DeadLetterQueue}",
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

    private Task ScheduleRetryAsync(IChannel channel, ReadOnlyMemory<byte> body, ulong deliveryTag,
        string? messageId, IReadOnlyBasicProperties? properties, IRetryPublisher? retryPublisher,
        Exception exception, CancellationToken cancellationToken) =>
        retryScheduler.HandleTransientFailureAsync(channel, deliveryTag, body, messageId,
            nameof(OrderCreated), properties, settings.RetryQueues, settings.DeadLetterQueue,
            retryPublisher, exception, cancellationToken);
}
