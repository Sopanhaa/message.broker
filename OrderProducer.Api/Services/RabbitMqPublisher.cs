using System.Text.Json;
using RabbitMQ.Client;
using Shared.Contracts.Events;
using Shared.Contracts.Messaging;

namespace OrderProducer.Api.Services;

public sealed class RabbitMqPublisher(RabbitMqSettings settings, ILogger<RabbitMqPublisher> logger)
    : IMessagePublisher, IAsyncDisposable
{
    // A single channel cannot safely publish concurrently. Serialize access for this small demo.
    private readonly SemaphoreSlim _gate = new(1, 1);
    private IConnection? _connection;
    private IChannel? _channel;

    public Task PublishAsync(OrderCreated order, CancellationToken cancellationToken) =>
        PublishToExchangeAsync(order, exchange: "", routingKey: settings.QueueName,
            nameof(OrderCreated), order.OrderId, cancellationToken);

    public Task PublishStatusAsync(OrderStatusChanged status, CancellationToken cancellationToken) =>
        // Status updates are not unique per order, so each publish needs its own message ID.
        // OrderCreated can reuse the order ID because an order is created once.
        PublishToExchangeAsync(status, exchange: "", routingKey: settings.StatusQueueName,
            nameof(OrderStatusChanged), Guid.NewGuid(), cancellationToken);

    public Task PublishFanoutAsync(OrderRoutingEvent routingEvent, CancellationToken cancellationToken) =>
        PublishToExchangeAsync(routingEvent, RoutingTopology.FanoutExchangeName(settings), routingKey: "",
            nameof(OrderRoutingEvent), routingEvent.EventId, cancellationToken);

    public Task PublishTopicAsync(OrderRoutingEvent routingEvent, string routingKey,
        CancellationToken cancellationToken) =>
        PublishToExchangeAsync(routingEvent, RoutingTopology.TopicExchangeName(settings), routingKey,
            nameof(OrderRoutingEvent), routingEvent.EventId, cancellationToken);

    private async Task PublishToExchangeAsync<T>(T message, string exchange, string routingKey,
        string messageType, Guid messageId, CancellationToken cancellationToken)
    {
        var body = JsonSerializer.SerializeToUtf8Bytes(message, JsonSerializerOptions.Web);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(settings.PublishTimeoutSeconds));
        var entered = false;
        try
        {
            await _gate.WaitAsync(timeout.Token);
            entered = true;
            if (_connection is not { IsOpen: true } || _channel is not { IsOpen: true })
            {
                await ResetAsync();
                _connection = await settings.CreateConnectionFactory().CreateConnectionAsync(timeout.Token);
                _channel = await _connection.CreateChannelAsync(new CreateChannelOptions(
                    publisherConfirmationsEnabled: true,
                    publisherConfirmationTrackingEnabled: true), timeout.Token);
                await RabbitMqTopology.DeclareAsync(_channel, settings, timeout.Token);
                await RoutingTopology.DeclareAsync(_channel, settings, timeout.Token);
                logger.LogInformation("Publisher connected to RabbitMQ at {Host}:{Port}", settings.HostName, settings.Port);
            }

            var properties = new BasicProperties
            {
                Persistent = true,
                ContentType = "application/json",
                Type = messageType,
                MessageId = messageId.ToString()
            };
            // With confirmation tracking enabled, this await includes the broker confirmation.
            // mandatory=true also reports an unroutable message as a publishing failure.
            await _channel!.BasicPublishAsync(exchange: exchange, routingKey: routingKey,
                mandatory: true, basicProperties: properties, body: body, cancellationToken: timeout.Token);
            logger.LogInformation("Broker confirmed {MessageType} {MessageId} on {Exchange}/{RoutingKey}",
                messageType, messageId, string.IsNullOrEmpty(exchange) ? "(default)" : exchange, routingKey);
        }
        catch (Exception ex)
        {
            if (entered)
                await ResetAsync(); // The next request makes a fresh connection after a failure.
            if (ex is OperationCanceledException && cancellationToken.IsCancellationRequested)
                throw;
            throw new MessagePublishException("RabbitMQ did not confirm the message.", ex);
        }
        finally
        {
            if (entered) _gate.Release();
        }
    }

    private async Task ResetAsync()
    {
        var channel = _channel;
        var connection = _connection;
        _channel = null;
        _connection = null;
        try { if (channel is not null) await channel.DisposeAsync(); }
        catch (Exception ex) { logger.LogWarning(ex, "Could not dispose publisher channel cleanly"); }
        try { if (connection is not null) await connection.DisposeAsync(); }
        catch (Exception ex) { logger.LogWarning(ex, "Could not dispose publisher connection cleanly"); }
    }

    public async ValueTask DisposeAsync()
    {
        await _gate.WaitAsync();
        try { await ResetAsync(); }
        finally { _gate.Release(); _gate.Dispose(); }
    }
}
