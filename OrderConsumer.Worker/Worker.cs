using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shared.Contracts.Events;
using Shared.Contracts.Messaging;

namespace OrderConsumer.Worker;

public sealed class Worker(RabbitMqSettings settings, ILogger<Worker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ConsumeUntilDisconnectedAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                logger.LogError(ex, "Consumer connection failed; reconnecting in 5 seconds");
            }
            try { await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken); }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
        }
        logger.LogInformation("Consumer stopped. Unacknowledged messages remain available for redelivery.");
    }

    private async Task ConsumeUntilDisconnectedAsync(CancellationToken stoppingToken)
    {
        await using var connection = await settings.CreateConnectionFactory().CreateConnectionAsync(stoppingToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);
        var disconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        channel.ChannelShutdownAsync += (_, reason) =>
        {
            logger.LogWarning("Consumer channel closed: {Reason}", reason.ReplyText);
            disconnected.TrySetResult();
            return Task.CompletedTask;
        };
        await RabbitMqTopology.DeclareAsync(channel, settings, stoppingToken);
        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false,
            cancellationToken: stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, message) =>
        {
            try
            {
                // Deserialize inside the callback: RabbitMQ owns the body memory after it returns.
                var order = JsonSerializer.Deserialize<OrderCreated>(message.Body.Span, JsonSerializerOptions.Web)
                    ?? throw new JsonException("OrderCreated payload was null.");
                if (order.OrderId == Guid.Empty || string.IsNullOrWhiteSpace(order.CustomerName) ||
                    order.CustomerName.Trim().Length > 100 || order.TotalAmount <= 0 || order.CreatedAtUtc == default)
                    throw new JsonException("OrderCreated payload contains invalid or missing values.");

                logger.LogInformation("Processing order {OrderId}: customer={CustomerName}, total={TotalAmount}",
                    order.OrderId, order.CustomerName, order.TotalAmount);
                await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
                await channel.BasicAckAsync(message.DeliveryTag, multiple: false, cancellationToken: stoppingToken);
                logger.LogInformation("Processed and acknowledged order {OrderId}", order.OrderId);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // Closing the channel requeues the delivery; don't acknowledge unfinished work.
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Delivery {DeliveryTag} failed; rejecting to {DeadLetterQueue}",
                    message.DeliveryTag, settings.DeadLetterQueue);
                try
                {
                    await channel.BasicNackAsync(message.DeliveryTag, multiple: false, requeue: false,
                        cancellationToken: stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
                catch (Exception nackError)
                {
                    logger.LogWarning(nackError, "Reject failed; reconnecting. The unacknowledged message may be redelivered.");
                    disconnected.TrySetResult();
                }
            }
        };
        await channel.BasicConsumeAsync(settings.QueueName, autoAck: false, consumer: consumer,
            cancellationToken: stoppingToken);
        logger.LogInformation("Waiting for orders in {Queue}; prefetch=1, manual acknowledgments enabled", settings.QueueName);
        // Disposing the channel/connection on cancellation cancels consumption and requeues unfinished work.
        await disconnected.Task.WaitAsync(stoppingToken);
    }
}
