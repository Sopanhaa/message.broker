using RabbitMQ.Client;
using Shared.Contracts.Messaging;

namespace OrderConsumer.Worker;

// Handles a transient processing failure by publishing to the next retry queue
// and only then acknowledging the original delivery. If the retry publish cannot
// be confirmed, no settlement occurs: the Worker reconnects and RabbitMQ
// requeues the unacknowledged original message.
public sealed class RetryScheduler(ILogger<RetryScheduler> logger)
{
    public async Task HandleTransientFailureAsync(
        IChannel channel,
        ulong deliveryTag,
        ReadOnlyMemory<byte> body,
        string? messageId,
        string messageType,
        IReadOnlyBasicProperties? properties,
        IReadOnlyList<RetryQueue> retryQueues,
        string deadLetterQueue,
        IRetryPublisher? retryPublisher,
        Exception exception,
        CancellationToken cancellationToken)
    {
        var nextIndex = RetryHeaderInspector.GetNextRetryIndex(properties?.Headers, retryQueues);

        if (nextIndex >= retryQueues.Count)
        {
            // NACK with requeue=false uses the main queue's configured dead-letter
            // exchange. RabbitMQ does not let a consumer NACK directly to a queue.
            logger.LogError(exception,
                "Delivery {DeliveryTag} exhausted {RetryLimit} retry attempts; rejecting to {DeadLetterQueue}",
                deliveryTag, retryQueues.Count, deadLetterQueue);
            await channel.BasicNackAsync(deliveryTag, multiple: false, requeue: false,
                cancellationToken: cancellationToken);
            return;
        }

        if (retryPublisher is null)
        {
            // Never settle the delivery if we cannot schedule a retry. Throwing
            // makes the Worker reconnect, and the unacknowledged message is requeued.
            logger.LogError(exception,
                "Delivery {DeliveryTag} failed transiently but no retry publisher is configured; leaving it unacknowledged",
                deliveryTag);
            throw new InvalidOperationException("A retry publisher is required for transient failures.", exception);
        }

        var retryQueue = retryQueues[nextIndex];
        var attempt = nextIndex + 1;
        var retryMessage = new RetryMessage(body, messageId, properties?.CorrelationId,
            messageType, attempt, properties);

        try
        {
            await retryPublisher.PublishAsync(retryMessage, retryQueue.QueueName, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Shutdown: leave the delivery unacknowledged so RabbitMQ requeues it.
            throw;
        }
        catch (Exception publishException)
        {
            // Do not ACK and do not NACK to the DLQ. Reconnecting keeps the
            // original message alive; a duplicate retry copy is possible after an
            // ambiguous confirmation failure, which the handlers tolerate.
            logger.LogError(publishException,
                "Could not publish delivery {DeliveryTag} to retry queue {RetryQueue}; the original delivery remains unacknowledged for redelivery",
                deliveryTag, retryQueue.QueueName);
            throw;
        }

        // The broker confirmed the retry publish, so it is now safe to remove the original.
        await channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken: cancellationToken);
        logger.LogWarning(exception,
            "Delivery {DeliveryTag} failed transiently; scheduled retry {Attempt}/{RetryLimit} in {RetryQueue} after {DelaySeconds} seconds",
            deliveryTag, attempt, retryQueues.Count, retryQueue.QueueName, retryQueue.Delay.TotalSeconds);
    }
}
