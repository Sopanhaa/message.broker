using System.Text.Json;
using OrderPersistence;
using RabbitMQ.Client;
using Shared.Contracts.Events;
using Shared.Contracts.Messaging;

namespace OrderConsumer.Worker;

public sealed class OrderStatusMessageHandler(
    IOrderStatusProcessor processor, IOrderStore store, RabbitMqSettings settings,
    ILogger<OrderStatusMessageHandler> logger, RetryScheduler retryScheduler)
{
    public async Task HandleAsync(IChannel channel, ReadOnlyMemory<byte> body, ulong deliveryTag,
        CancellationToken cancellationToken, string? messageId = null,
        IReadOnlyBasicProperties? properties = null, IRetryPublisher? retryPublisher = null,
        string? consumerId = null)
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

            // Invalid JSON or schema is permanent: retrying cannot fix the payload.
            logger.LogError(ex,
                "Consumer {ConsumerId} rejected status delivery {DeliveryTag} with an invalid payload (status: dead-lettered to {DeadLetterQueue})",
                consumerId, deliveryTag, settings.StatusDeadLetterQueue);
            await channel.BasicNackAsync(deliveryTag, multiple: false, requeue: false,
                cancellationToken: cancellationToken);
            return;
        }

        logger.LogInformation("Consumer {ConsumerId} received status update for order {OrderId} (status: processing)",
            consumerId, update.OrderId);

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
            logger.LogWarning("Consumer {ConsumerId} status update for order {OrderId} (status: retry scheduled)",
                consumerId, update.OrderId);
            return;
        }
        catch (Exception ex)
        {
            if (cancellationToken.IsCancellationRequested) return;

            logger.LogError(ex,
                "Consumer {ConsumerId} could not check duplication for status update of order {OrderId} (status: dead-lettered to {DeadLetterQueue})",
                consumerId, update.OrderId, settings.StatusDeadLetterQueue);
            await channel.BasicNackAsync(deliveryTag, multiple: false, requeue: false,
                cancellationToken: cancellationToken);
            return;
        }

        if (alreadyProcessed)
        {
            await channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken: cancellationToken);
            logger.LogInformation(
                "Consumer {ConsumerId} skipped duplicate OrderStatusChanged {MessageId} for order {OrderId} (status: duplicate skipped)",
                consumerId, messageId, update.OrderId);
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
        catch (Exception ex) when (ProcessingFailureClassifier.IsTransient(ex))
        {
            if (cancellationToken.IsCancellationRequested) return;

            // Do not mark the message processed: the retry must do the work again.
            await ScheduleRetryAsync(channel, body, deliveryTag, messageId, properties, retryPublisher,
                ex, cancellationToken);
            logger.LogWarning("Consumer {ConsumerId} status update for order {OrderId} (status: retry scheduled)",
                consumerId, update.OrderId);
            return;
        }
        catch (Exception ex)
        {
            if (cancellationToken.IsCancellationRequested) return;

            // Unexpected failures use the bounded DLQ policy: log once and retain
            // the message for inspection instead of retrying forever.
            logger.LogError(ex,
                "Consumer {ConsumerId} failed status update for order {OrderId} permanently (status: dead-lettered to {DeadLetterQueue})",
                consumerId, update.OrderId, settings.StatusDeadLetterQueue);
            await channel.BasicNackAsync(deliveryTag, multiple: false, requeue: false,
                cancellationToken: cancellationToken);
            return;
        }

        // A failed acknowledgment has an unknown outcome. Let the worker reconnect;
        // attempting to reject the same delivery could settle it twice.
        await channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken: cancellationToken);
        logger.LogInformation(
            "Consumer {ConsumerId} processed and acknowledged status update for order {OrderId} (status: acknowledged)",
            consumerId, update.OrderId);
    }

    private Task ScheduleRetryAsync(IChannel channel, ReadOnlyMemory<byte> body, ulong deliveryTag,
        string? messageId, IReadOnlyBasicProperties? properties, IRetryPublisher? retryPublisher,
        Exception exception, CancellationToken cancellationToken) =>
        retryScheduler.HandleTransientFailureAsync(channel, deliveryTag, body, messageId,
            nameof(OrderStatusChanged), properties, settings.StatusRetryQueues, settings.StatusDeadLetterQueue,
            retryPublisher, exception, cancellationToken);
}
