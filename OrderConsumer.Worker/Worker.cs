using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shared.Contracts.Messaging;

namespace OrderConsumer.Worker;

public sealed class Worker(
    RabbitMqSettings settings, OrderMessageHandler orderHandler, OrderStatusMessageHandler statusHandler,
    ILogger<Worker> logger) : BackgroundService
{
    private static readonly TimeSpan ReconnectDelay = TimeSpan.FromSeconds(5);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await ConsumeUntilDisconnectedAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Consumer connection failed");
                }

                if (stoppingToken.IsCancellationRequested) break;
                logger.LogInformation("Reconnecting to RabbitMQ in {DelaySeconds} seconds", ReconnectDelay.TotalSeconds);
                await Task.Delay(ReconnectDelay, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        finally
        {
            logger.LogInformation("Consumer stopped. Unacknowledged messages remain available for redelivery.");
        }
    }

    private async Task ConsumeUntilDisconnectedAsync(CancellationToken stoppingToken)
    {
        using var session = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        await using var connection = await settings.CreateConnectionFactory().CreateConnectionAsync(stoppingToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);
        var disconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        Task OnShutdownAsync(object sender, ShutdownEventArgs reason)
        {
            if (!stoppingToken.IsCancellationRequested)
                logger.LogWarning("Consumer connection or channel closed: {Reason}", reason.ReplyText);
            disconnected.TrySetResult();
            return Task.CompletedTask;
        }

        connection.ConnectionShutdownAsync += OnShutdownAsync;
        channel.ChannelShutdownAsync += OnShutdownAsync;

        try
        {
            await RabbitMqTopology.DeclareAsync(channel, settings, stoppingToken);
            await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false,
                cancellationToken: stoppingToken);

            AsyncEventingBasicConsumer CreateConsumer(string queueName,
                Func<IChannel, ReadOnlyMemory<byte>, ulong, CancellationToken, Task> handle)
            {
                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.UnregisteredAsync += (_, _) =>
                {
                    if (!stoppingToken.IsCancellationRequested)
                        logger.LogWarning("Consumer was cancelled; reconnecting to queue {Queue}", queueName);
                    disconnected.TrySetResult();
                    return Task.CompletedTask;
                };
                consumer.ReceivedAsync += async (_, message) =>
                {
                    try
                    {
                        await handle(channel, message.Body, message.DeliveryTag, session.Token);
                    }
                    catch (OperationCanceledException) when (session.IsCancellationRequested) { }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex,
                            "Could not settle delivery {DeliveryTag} from {Queue}; reconnecting. The message may be redelivered.",
                            message.DeliveryTag, queueName);
                        disconnected.TrySetResult();
                    }
                };
                return consumer;
            }

            var orderConsumer = CreateConsumer(settings.QueueName, orderHandler.HandleAsync);
            var statusConsumer = CreateConsumer(settings.StatusQueueName, statusHandler.HandleAsync);

            await channel.BasicConsumeAsync(settings.QueueName, autoAck: false, consumer: orderConsumer,
                cancellationToken: stoppingToken);
            await channel.BasicConsumeAsync(settings.StatusQueueName, autoAck: false, consumer: statusConsumer,
                cancellationToken: stoppingToken);
            logger.LogInformation(
                "Waiting for orders in {Queue} and status updates in {StatusQueue}; prefetch=1 per queue, manual acknowledgments enabled",
                settings.QueueName, settings.StatusQueueName);
            await disconnected.Task.WaitAsync(stoppingToken);
        }
        finally
        {
            // Stop in-flight work before disposing the channel and requeuing unfinished deliveries.
            await session.CancelAsync();
            connection.ConnectionShutdownAsync -= OnShutdownAsync;
            channel.ChannelShutdownAsync -= OnShutdownAsync;
        }
    }
}
