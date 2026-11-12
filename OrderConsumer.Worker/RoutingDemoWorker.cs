using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shared.Contracts.Messaging;

namespace OrderConsumer.Worker;

// Owns the fanout and topic demo consumers. A separate connection keeps this
// demo traffic isolated from the critical order and status consumers.
public sealed class RoutingDemoWorker(
    RabbitMqSettings settings,
    RoutingMessageHandler handler,
    ILogger<RoutingDemoWorker> logger) : BackgroundService
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
                    logger.LogError(ex, "Routing consumer connection failed");
                }

                if (stoppingToken.IsCancellationRequested) break;
                logger.LogInformation("Reconnecting routing consumers to RabbitMQ in {DelaySeconds} seconds",
                    ReconnectDelay.TotalSeconds);
                await Task.Delay(ReconnectDelay, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        finally
        {
            logger.LogInformation(
                "Routing consumers stopped. Unacknowledged messages remain available for redelivery.");
        }
    }

    private async Task ConsumeUntilDisconnectedAsync(CancellationToken stoppingToken)
    {
        using var session = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        await using var connection = await settings.CreateConnectionFactory().CreateConnectionAsync(stoppingToken);

        IChannel? topologyChannel = null;
        var consumerChannels = new List<IChannel>();
        var disconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        Task OnShutdownAsync(object sender, ShutdownEventArgs reason)
        {
            if (!stoppingToken.IsCancellationRequested)
                logger.LogWarning("Routing consumer connection or channel closed: {Reason}", reason.ReplyText);
            disconnected.TrySetResult();
            return Task.CompletedTask;
        }

        try
        {
            connection.ConnectionShutdownAsync += OnShutdownAsync;
            topologyChannel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);
            topologyChannel.ChannelShutdownAsync += OnShutdownAsync;
            await RoutingTopology.DeclareAsync(topologyChannel, settings, stoppingToken);

            var queueNames = RoutingTopology.AllQueueNames(settings);
            foreach (var queueName in queueNames)
            {
                var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);
                consumerChannels.Add(channel);
                channel.ChannelShutdownAsync += OnShutdownAsync;
                await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false,
                    cancellationToken: stoppingToken);

                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.UnregisteredAsync += (_, _) =>
                {
                    if (!stoppingToken.IsCancellationRequested)
                        logger.LogWarning("Routing consumer was cancelled; reconnecting to queue {Queue}", queueName);
                    disconnected.TrySetResult();
                    return Task.CompletedTask;
                };
                consumer.ReceivedAsync += async (_, message) =>
                {
                    try
                    {
                        await handler.HandleAsync(channel, message.Body, message.DeliveryTag, queueName,
                            session.Token);
                    }
                    catch (OperationCanceledException) when (session.IsCancellationRequested) { }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex,
                            "Could not settle routing delivery {DeliveryTag} from {Queue}; reconnecting. The message may be redelivered.",
                            message.DeliveryTag, queueName);
                        disconnected.TrySetResult();
                    }
                };
                await channel.BasicConsumeAsync(queueName, autoAck: false, consumer: consumer,
                    cancellationToken: stoppingToken);
            }

            logger.LogInformation(
                "Waiting for routing demo messages in {Queues}; prefetch=1 per queue, manual acknowledgments enabled",
                string.Join(", ", queueNames));
            await disconnected.Task.WaitAsync(stoppingToken);
        }
        finally
        {
            // Stop in-flight work before disposing the channels and requeuing unfinished deliveries.
            await session.CancelAsync();
            connection.ConnectionShutdownAsync -= OnShutdownAsync;

            if (topologyChannel is not null)
            {
                topologyChannel.ChannelShutdownAsync -= OnShutdownAsync;
                await topologyChannel.DisposeAsync();
            }

            foreach (var channel in consumerChannels)
            {
                channel.ChannelShutdownAsync -= OnShutdownAsync;
                await channel.DisposeAsync();
            }
        }
    }
}
