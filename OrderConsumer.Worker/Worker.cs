using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shared.Contracts.Messaging;

namespace OrderConsumer.Worker;

public sealed class Worker(
    RabbitMqSettings settings, OrderMessageHandler orderHandler, OrderStatusMessageHandler statusHandler,
    ILogger<Worker> logger, ILogger<RabbitMqRetryPublisher> retryLogger) : BackgroundService
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

        // Each consumer owns its channel, and the retry publisher owns a separate
        // confirm-enabled channel. No channel is shared by concurrent operations.
        await using var orderChannel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);
        await using var statusChannel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);
        await using var retryChannel = await connection.CreateChannelAsync(new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true), stoppingToken);
        var retryPublisher = new RabbitMqRetryPublisher(retryChannel, retryLogger);
        var disconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        Task OnShutdownAsync(object sender, ShutdownEventArgs reason)
        {
            if (!stoppingToken.IsCancellationRequested)
                logger.LogWarning("Consumer connection or channel closed: {Reason}", reason.ReplyText);
            disconnected.TrySetResult();
            return Task.CompletedTask;
        }

        connection.ConnectionShutdownAsync += OnShutdownAsync;
        orderChannel.ChannelShutdownAsync += OnShutdownAsync;
        statusChannel.ChannelShutdownAsync += OnShutdownAsync;
        retryChannel.ChannelShutdownAsync += OnShutdownAsync;

        try
        {
            await RabbitMqTopology.DeclareAsync(orderChannel, settings, stoppingToken);
            await orderChannel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false,
                cancellationToken: stoppingToken);
            await statusChannel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false,
                cancellationToken: stoppingToken);

            AsyncEventingBasicConsumer CreateConsumer(IChannel consumerChannel, string queueName,
                Func<IChannel, ReadOnlyMemory<byte>, IReadOnlyBasicProperties?, string?, ulong, CancellationToken,
                    Task> handle)
            {
                var consumer = new AsyncEventingBasicConsumer(consumerChannel);
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
                        await handle(consumerChannel, message.Body, message.BasicProperties,
                            message.BasicProperties.MessageId, message.DeliveryTag, session.Token);
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

            var orderConsumer = CreateConsumer(orderChannel, settings.QueueName,
                (channel, body, properties, messageId, deliveryTag, token) =>
                    orderHandler.HandleAsync(channel, body, deliveryTag, token, messageId, properties,
                        retryPublisher));
            var statusConsumer = CreateConsumer(statusChannel, settings.StatusQueueName,
                (channel, body, properties, messageId, deliveryTag, token) =>
                    statusHandler.HandleAsync(channel, body, deliveryTag, token, messageId, properties,
                        retryPublisher));

            await orderChannel.BasicConsumeAsync(settings.QueueName, autoAck: false, consumer: orderConsumer,
                cancellationToken: stoppingToken);
            await statusChannel.BasicConsumeAsync(settings.StatusQueueName, autoAck: false, consumer: statusConsumer,
                cancellationToken: stoppingToken);
            logger.LogInformation(
                "Waiting for orders in {Queue} and status updates in {StatusQueue}; prefetch=1 per queue, manual acknowledgments enabled",
                settings.QueueName, settings.StatusQueueName);
            await disconnected.Task.WaitAsync(stoppingToken);
        }
        finally
        {
            // Stop in-flight work before disposing the channels and requeuing unfinished deliveries.
            await session.CancelAsync();
            connection.ConnectionShutdownAsync -= OnShutdownAsync;
            orderChannel.ChannelShutdownAsync -= OnShutdownAsync;
            statusChannel.ChannelShutdownAsync -= OnShutdownAsync;
            retryChannel.ChannelShutdownAsync -= OnShutdownAsync;
        }
    }
}
