using System.Diagnostics;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shared.Contracts.Events;
using Shared.Contracts.Messaging;

namespace OrderMessagingSystem.Tests;

// Opt-in against the development RabbitMQ instance from docker-compose.
// The class name intentionally contains RabbitMqIntegrationTests so the
// existing integration-test filter also picks these tests up.
public sealed class RoutingRabbitMqIntegrationTests
{
    private const string Host = "localhost";
    private const int AmqpPort = 5673;
    private const string UserName = "orderdev";
    private const string Password = "orderdev_password";

    [RabbitMqIntegrationFact]
    public async Task Fanout_publishes_one_copy_to_each_bound_queue()
    {
        var settings = CreateSettings();
        await using var connection = await ConnectAsync(settings);
        await using var channel = await connection.CreateChannelAsync();

        try
        {
            await RoutingTopology.DeclareAsync(channel, settings, CancellationToken.None);
            var routingEvent = NewEvent(OrderRoutingEventTypes.Created);

            await PublishAsync(channel, RoutingTopology.FanoutExchangeName(settings), "", routingEvent);

            foreach (var queueName in RoutingTopology.FanoutQueueNames(settings))
                AssertReceived(await GetOneAsync(channel, queueName), routingEvent);
        }
        finally
        {
            await CleanupAsync(settings);
        }
    }

    [RabbitMqIntegrationFact]
    public async Task Topic_exact_binding_reaches_all_matching_queues()
    {
        var settings = CreateSettings();
        await using var connection = await ConnectAsync(settings);
        await using var channel = await connection.CreateChannelAsync();

        try
        {
            await RoutingTopology.DeclareAsync(channel, settings, CancellationToken.None);
            var routingEvent = NewEvent(OrderRoutingEventTypes.Created);

            await PublishAsync(channel, RoutingTopology.TopicExchangeName(settings),
                RoutingTopology.OrderCreatedRoutingKey, routingEvent);

            AssertReceived(await GetOneAsync(channel, TopicQueue(settings, "order")), routingEvent);
            AssertReceived(await GetOneAsync(channel, TopicQueue(settings, "notification")), routingEvent);
            AssertReceived(await GetOneAsync(channel, TopicQueue(settings, "audit")), routingEvent);
        }
        finally
        {
            await CleanupAsync(settings);
        }
    }

    [RabbitMqIntegrationFact]
    public async Task Topic_star_wildcard_matches_one_word()
    {
        var settings = CreateSettings();
        await using var connection = await ConnectAsync(settings);
        await using var channel = await connection.CreateChannelAsync();

        try
        {
            await RoutingTopology.DeclareAsync(channel, settings, CancellationToken.None);
            var routingEvent = NewEvent(OrderRoutingEventTypes.Updated);

            await PublishAsync(channel, RoutingTopology.TopicExchangeName(settings),
                RoutingTopology.OrderUpdatedRoutingKey, routingEvent);

            AssertReceived(await GetOneAsync(channel, TopicQueue(settings, "order")), routingEvent);
            AssertReceived(await GetOneAsync(channel, TopicQueue(settings, "audit")), routingEvent);
            await AssertQueueEmptyAsync(channel, TopicQueue(settings, "notification"));
        }
        finally
        {
            await CleanupAsync(settings);
        }
    }

    [RabbitMqIntegrationFact]
    public async Task Topic_star_wildcard_does_not_cross_dots()
    {
        var settings = CreateSettings();
        await using var connection = await ConnectAsync(settings);
        await using var channel = await connection.CreateChannelAsync();

        try
        {
            await RoutingTopology.DeclareAsync(channel, settings, CancellationToken.None);
            var routingEvent = NewEvent(OrderRoutingEventTypes.Created);

            await PublishAsync(channel, RoutingTopology.TopicExchangeName(settings),
                "order.created.extra", routingEvent);

            AssertReceived(await GetOneAsync(channel, TopicQueue(settings, "audit")), routingEvent);
            await AssertQueueEmptyAsync(channel, TopicQueue(settings, "order"));
            await AssertQueueEmptyAsync(channel, TopicQueue(settings, "notification"));
        }
        finally
        {
            await CleanupAsync(settings);
        }
    }

    [RabbitMqIntegrationFact]
    public async Task Topic_hash_wildcard_routes_every_key_to_audit()
    {
        var settings = CreateSettings();
        await using var connection = await ConnectAsync(settings);
        await using var channel = await connection.CreateChannelAsync();

        try
        {
            await RoutingTopology.DeclareAsync(channel, settings, CancellationToken.None);
            var routingEvent = NewEvent("PaymentCreated");

            // This key matches only the # binding on the audit queue.
            await PublishAsync(channel, RoutingTopology.TopicExchangeName(settings),
                "payment.created", routingEvent);

            AssertReceived(await GetOneAsync(channel, TopicQueue(settings, "audit")), routingEvent);
            await AssertQueueEmptyAsync(channel, TopicQueue(settings, "order"));
            await AssertQueueEmptyAsync(channel, TopicQueue(settings, "notification"));
        }
        finally
        {
            await CleanupAsync(settings);
        }
    }

    [RabbitMqIntegrationFact]
    public async Task Fanout_delivers_to_multiple_consumers()
    {
        var settings = CreateSettings();
        await using var connection = await ConnectAsync(settings);
        var topologyChannel = await connection.CreateChannelAsync();
        var emailChannel = await connection.CreateChannelAsync();
        var loggingChannel = await connection.CreateChannelAsync();

        try
        {
            await RoutingTopology.DeclareAsync(topologyChannel, settings, CancellationToken.None);
            var emailReceived = new TaskCompletionSource<OrderRoutingEvent>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            var loggingReceived = new TaskCompletionSource<OrderRoutingEvent>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            var fanoutQueues = RoutingTopology.FanoutQueueNames(settings);

            await StartConsumerAsync(emailChannel, fanoutQueues[0], emailReceived);
            await StartConsumerAsync(loggingChannel, fanoutQueues[1], loggingReceived);

            var routingEvent = NewEvent(OrderRoutingEventTypes.Created);
            await PublishAsync(topologyChannel, RoutingTopology.FanoutExchangeName(settings), "",
                routingEvent);

            var email = await emailReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));
            var logging = await loggingReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(routingEvent.EventId, email.EventId);
            Assert.Equal(routingEvent.EventId, logging.EventId);
        }
        finally
        {
            await DisposeSafelyAsync(emailChannel);
            await DisposeSafelyAsync(loggingChannel);
            await DisposeSafelyAsync(topologyChannel);
            await CleanupAsync(settings);
        }
    }

    private static RabbitMqSettings CreateSettings() => new()
    {
        HostName = Host,
        Port = AmqpPort,
        UserName = UserName,
        Password = Password,
        VirtualHost = "/",
        QueueName = $"orders.routing.it.{Guid.NewGuid():N}"
    };

    private static OrderRoutingEvent NewEvent(string eventType) =>
        new(Guid.NewGuid(), eventType, DateTimeOffset.UtcNow, "Routing integration test");

    private static string TopicQueue(RabbitMqSettings settings, string name) =>
        $"{settings.QueueName}.topic.{name}";

    private static async Task<IConnection> ConnectAsync(RabbitMqSettings settings)
    {
        try
        {
            return await settings.CreateConnectionFactory().CreateConnectionAsync();
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                "RabbitMQ is not available on localhost:5673. Start it with docker compose up -d --wait.", ex);
        }
    }

    private static async Task PublishAsync(IChannel channel, string exchange, string routingKey,
        OrderRoutingEvent routingEvent)
    {
        var body = JsonSerializer.SerializeToUtf8Bytes(routingEvent, JsonSerializerOptions.Web);
        var properties = new BasicProperties
        {
            Persistent = true,
            ContentType = "application/json",
            Type = nameof(OrderRoutingEvent),
            MessageId = routingEvent.EventId.ToString()
        };
        await channel.BasicPublishAsync(exchange, routingKey, mandatory: true,
            basicProperties: properties, body: body);
    }

    private static async Task StartConsumerAsync(IChannel channel, string queueName,
        TaskCompletionSource<OrderRoutingEvent> received)
    {
        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, message) =>
        {
            var routingEvent = JsonSerializer.Deserialize<OrderRoutingEvent>(
                message.Body.Span, JsonSerializerOptions.Web);
            // Acknowledge before waking the test so cleanup cannot dispose the
            // channel while the ACK is still in flight.
            await channel.BasicAckAsync(message.DeliveryTag, multiple: false);
            if (routingEvent is not null) received.TrySetResult(routingEvent);
        };
        await channel.BasicConsumeAsync(queueName, autoAck: false, consumer: consumer);
    }

    private static async Task<BasicGetResult?> GetOneAsync(IChannel channel, string queueName)
    {
        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.Elapsed < TimeSpan.FromSeconds(5))
        {
            var result = await channel.BasicGetAsync(queueName, autoAck: true);
            if (result is not null) return result;
            await Task.Delay(100);
        }

        return null;
    }

    private static async Task AssertQueueEmptyAsync(IChannel channel, string queueName)
    {
        Assert.Null(await channel.BasicGetAsync(queueName, autoAck: true));
    }

    private static void AssertReceived(BasicGetResult? result, OrderRoutingEvent expected)
    {
        var received = Assert.IsType<BasicGetResult>(result);
        Assert.Equal(expected.EventId.ToString(), received.BasicProperties.MessageId);
        var payload = JsonSerializer.Deserialize<OrderRoutingEvent>(received.Body.Span,
            JsonSerializerOptions.Web);
        Assert.NotNull(payload);
        Assert.Equal(expected.EventId, payload.EventId);
        Assert.Equal(expected.EventType, payload.EventType);
    }

    private static async Task CleanupAsync(RabbitMqSettings settings)
    {
        // Cleanup uses a fresh connection so a broken test connection cannot
        // leave topology behind, and cleanup failures cannot mask the test result.
        try
        {
            await using var connection = await settings.CreateConnectionFactory().CreateConnectionAsync();
            await using var channel = await connection.CreateChannelAsync();
            await DeleteTopologyAsync(channel, settings);
        }
        catch (Exception)
        {
            // Best effort: a leftover uniquely named test topology is harmless.
        }
    }

    private static async Task DisposeSafelyAsync(IChannel channel)
    {
        try { await channel.DisposeAsync(); }
        catch (Exception) { /* The connection may already be closed. */ }
    }

    private static async Task DeleteTopologyAsync(IChannel channel, RabbitMqSettings settings)
    {
        // Cleanup is best effort so a failed assertion still reports the original error.
        foreach (var queueName in RoutingTopology.AllQueueNames(settings)
                     .Append(RoutingTopology.RoutingDeadLetterQueueName(settings)))
        {
            try { await channel.QueueDeleteAsync(queueName); }
            catch (Exception) { /* Queue may not exist; cleanup is best effort. */ }
        }

        foreach (var exchange in new[]
                 {
                     RoutingTopology.FanoutExchangeName(settings),
                     RoutingTopology.TopicExchangeName(settings),
                     RoutingTopology.RoutingDeadLetterExchangeName(settings)
                 })
        {
            try { await channel.ExchangeDeleteAsync(exchange); }
            catch (Exception) { /* Exchange may not exist; cleanup is best effort. */ }
        }
    }
}
