using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shared.Contracts.Messaging;

namespace OrderConsumer.Worker;

public sealed class Worker(
    RabbitMqSettings settings, OrderMessageHandler orderHandler, OrderStatusMessageHandler statusHandler,
    ILogger<Worker> logger, ILogger<RabbitMqRetryPublisher> retryLogger) : BackgroundService
{
    private static readonly TimeSpan ReconnectDelay = TimeSpan.FromSeconds(5);

    // Each consumer may hold at most one unacknowledged message. RabbitMQ will
    // not send the next message on that channel until this one is ACKed or NACKed.
    private const int PrefetchCount = 1;

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

        // Every consumer owns its channel, and the retry publisher owns a separate
        // confirm-enabled channel. No channel is shared by concurrent operations.
        await using var retryChannel = await connection.CreateChannelAsync(new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true), stoppingToken);
        var retryPublisher = new RabbitMqRetryPublisher(retryChannel, retryLogger);
        var consumerChannels = new List<IChannel>();
        var disconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        Task OnShutdownAsync(object sender, ShutdownEventArgs reason)
        {
            if (!stoppingToken.IsCancellationRequested)
                logger.LogWarning("Consumer connection or channel closed: {Reason}", reason.ReplyText);
            disconnected.TrySetResult();
            return Task.CompletedTask;
        }

        connection.ConnectionShutdownAsync += OnShutdownAsync;
        retryChannel.ChannelShutdownAsync += OnShutdownAsync;

        try
        {
            // Declare the topology once on a short-lived channel. Doing this before
            // starting consumers guarantees every consumer sees the same queues.
            await using (var topologyChannel =
                         await connection.CreateChannelAsync(cancellationToken: stoppingToken))
            {
                await RabbitMqTopology.DeclareAsync(topologyChannel, settings, stoppingToken);
            }

            AsyncEventingBasicConsumer CreateConsumer(IChannel consumerChannel, string queueName,
                string consumerId,
                Func<IChannel, ReadOnlyMemory<byte>, IReadOnlyBasicProperties?, string?, ulong, string,
                    CancellationToken, Task> handle)
            {
                var consumer = new AsyncEventingBasicConsumer(consumerChannel);
                consumer.UnregisteredAsync += (_, _) =>
                {
                    if (!stoppingToken.IsCancellationRequested)
                        logger.LogWarning("Consumer {ConsumerId} was cancelled; reconnecting to queue {Queue}",
                            consumerId, queueName);
                    disconnected.TrySetResult();
                    return Task.CompletedTask;
                };
                consumer.ReceivedAsync += async (_, message) =>
                {
                    try
                    {
                        await handle(consumerChannel, message.Body, message.BasicProperties,
                            message.BasicProperties.MessageId, message.DeliveryTag, consumerId, session.Token);
                    }
                    catch (OperationCanceledException) when (session.IsCancellationRequested) { }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex,
                            "Consumer {ConsumerId} could not settle delivery {DeliveryTag} from {Queue}; reconnecting. The message may be redelivered.",
                            consumerId, message.DeliveryTag, queueName);
                        disconnected.TrySetResult();
                    }
                };
                return consumer;
            }

            async Task StartConsumersAsync(string queueName, string consumerIdPrefix,
                Func<IChannel, ReadOnlyMemory<byte>, IReadOnlyBasicProperties?, string?, ulong, string,
                    CancellationToken, Task> handle)
            {
                // Several consumers can share one queue. RabbitMQ round-robins
                // messages between them, and each consumer has its own channel.
                for (var index = 1; index <= settings.ConsumerCount; index++)
                {
                    var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);
                    consumerChannels.Add(channel);
                    channel.ChannelShutdownAsync += OnShutdownAsync;

                    // Prefetch applies per channel: at most one unacknowledged message here.
                    await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: PrefetchCount,
                        global: false, cancellationToken: stoppingToken);

                    var consumerId = $"{consumerIdPrefix}-{index}";
                    var consumer = CreateConsumer(channel, queueName, consumerId, handle);

                    // autoAck=false: the handler decides when to ACK or NACK.
                    await channel.BasicConsumeAsync(queueName, autoAck: false, consumer: consumer,
                        cancellationToken: stoppingToken);

                    logger.LogInformation(
                        "Started consumer {ConsumerId} on {Queue}; prefetch={PrefetchCount}, manual acknowledgments enabled",
                        consumerId, queueName, PrefetchCount);
                }
            }

            await StartConsumersAsync(settings.QueueName, "order-consumer",
                (channel, body, properties, messageId, deliveryTag, consumerId, token) =>
                    orderHandler.HandleAsync(channel, body, deliveryTag, token, messageId, properties,
                        retryPublisher, consumerId));
            await StartConsumersAsync(settings.StatusQueueName, "status-consumer",
                (channel, body, properties, messageId, deliveryTag, consumerId, token) =>
                    statusHandler.HandleAsync(channel, body, deliveryTag, token, messageId, properties,
                        retryPublisher, consumerId));

            logger.LogInformation(
                "Waiting for orders in {Queue} and status updates in {StatusQueue}; consumers per queue={ConsumerCount}, prefetch={PrefetchCount}, manual acknowledgments enabled",
                settings.QueueName, settings.StatusQueueName, settings.ConsumerCount, PrefetchCount);
            await disconnected.Task.WaitAsync(stoppingToken);
        }
        finally
        {
            // Stop in-flight work before disposing the channels and requeuing unfinished deliveries.
            await session.CancelAsync();
            connection.ConnectionShutdownAsync -= OnShutdownAsync;
            retryChannel.ChannelShutdownAsync -= OnShutdownAsync;

            foreach (var channel in consumerChannels)
            {
                channel.ChannelShutdownAsync -= OnShutdownAsync;
                await channel.DisposeAsync();
            }
        }
    }
}
