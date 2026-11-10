using System.Diagnostics;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using OrderConsumer.Worker;
using RabbitMQ.Client;
using Shared.Contracts.Messaging;

namespace OrderMessagingSystem.Tests;

// Opt-in against the development RabbitMQ instance from docker-compose:
//   RABBITMQ_INTEGRATION_TESTS=1 dotnet test --filter FullyQualifiedName~RabbitMqIntegrationTests
// Without the environment variable these tests are reported as skipped.
public sealed class RabbitMqIntegrationFactAttribute : FactAttribute
{
    public RabbitMqIntegrationFactAttribute()
    {
        if (!string.Equals(Environment.GetEnvironmentVariable("RABBITMQ_INTEGRATION_TESTS"), "1",
                StringComparison.Ordinal))
            Skip = "Set RABBITMQ_INTEGRATION_TESTS=1 and start RabbitMQ with docker compose.";
    }
}

public sealed class RabbitMqIntegrationTests
{
    private const string Host = "localhost";
    private const int AmqpPort = 5673;
    private const int ManagementPort = 15673;
    private const string UserName = "orderdev";
    private const string Password = "orderdev_password";

    [RabbitMqIntegrationFact]
    public async Task Retry_queues_have_the_configured_ttls_and_dead_letter_routing()
    {
        var settings = CreateSettings();
        await using var connection = await ConnectAsync(settings);
        await using var channel = await connection.CreateChannelAsync();

        try
        {
            await RabbitMqTopology.DeclareAsync(channel, settings, CancellationToken.None);

            var expectedQueues = settings.RetryQueues
                .Select(queue => (Queue: queue, DeadLetterExchange: settings.DeadLetterExchange,
                    ReturnKey: settings.RetryReturnRoutingKey))
                .Concat(settings.StatusRetryQueues.Select(queue => (Queue: queue,
                    DeadLetterExchange: settings.StatusDeadLetterExchange,
                    ReturnKey: settings.StatusRetryReturnRoutingKey)));

            using var http = CreateManagementClient();
            foreach (var expected in expectedQueues)
            {
                var arguments = await GetQueueArgumentsAsync(http, expected.Queue.QueueName);
                Assert.Equal((int)expected.Queue.Delay.TotalMilliseconds,
                    arguments.GetProperty("x-message-ttl").GetInt32());
                Assert.Equal(expected.DeadLetterExchange,
                    arguments.GetProperty("x-dead-letter-exchange").GetString());
                Assert.Equal(expected.ReturnKey,
                    arguments.GetProperty("x-dead-letter-routing-key").GetString());
            }
        }
        finally
        {
            await BestEffortDeleteAsync(channel, settings);
        }
    }

    [RabbitMqIntegrationFact]
    public async Task First_retry_waits_about_five_seconds_and_returns_to_the_main_queue()
    {
        var settings = CreateSettings();
        await using var connection = await ConnectAsync(settings);
        await using var channel = await connection.CreateChannelAsync();
        await using var publishChannel = await connection.CreateChannelAsync(new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true));

        try
        {
            await RabbitMqTopology.DeclareAsync(channel, settings, CancellationToken.None);
            var firstRetryQueue = settings.RetryQueues[0];
            var messageId = $"integration-{Guid.NewGuid():N}";
            var correlationId = $"correlation-{Guid.NewGuid():N}";
            var source = new BasicProperties
            {
                Persistent = true,
                MessageId = messageId,
                CorrelationId = correlationId,
                ContentType = "application/json",
                Type = "OrderCreated",
                Headers = new Dictionary<string, object?> { ["custom-header"] = "custom-value" }
            };
            var retryPublisher = new RabbitMqRetryPublisher(publishChannel,
                NullLogger<RabbitMqRetryPublisher>.Instance);

            var stopwatch = Stopwatch.StartNew();
            // This exercises the real confirm-enabled retry publisher against the broker.
            await retryPublisher.PublishAsync(
                new RetryMessage("{\"orderId\":\"integration\"}"u8.ToArray(), messageId, correlationId,
                    "OrderCreated", 1, source),
                firstRetryQueue.QueueName, CancellationToken.None);

            BasicGetResult? received = null;
            while (stopwatch.Elapsed < TimeSpan.FromSeconds(20))
            {
                received = await channel.BasicGetAsync(settings.QueueName, autoAck: true);
                if (received is not null) break;
                await Task.Delay(TimeSpan.FromMilliseconds(250));
            }
            stopwatch.Stop();

            Assert.NotNull(received);
            var result = received;
            Assert.Equal(messageId, result.BasicProperties.MessageId);
            Assert.Equal(correlationId, result.BasicProperties.CorrelationId);
            // RabbitMQ.Client decodes AMQP long strings as byte[].
            var customHeader = Assert.IsType<byte[]>(result.BasicProperties.Headers!["custom-header"]);
            Assert.Equal("custom-value", Encoding.UTF8.GetString(customHeader));
            Assert.Equal(1L, result.BasicProperties.Headers![RetryHeaderInspector.RetryAttemptHeader]);
            Assert.InRange(stopwatch.Elapsed, TimeSpan.FromSeconds(4), TimeSpan.FromSeconds(19));

            // The broker accepted and preserved the retry headers, recorded the
            // retry-queue expiration, and our stage selection advances to 30s.
            Assert.Equal(1, RetryHeaderInspector.GetNextRetryIndex(
                result.BasicProperties.Headers, settings.RetryQueues));
        }
        finally
        {
            await BestEffortDeleteAsync(channel, settings);
        }
    }

    [RabbitMqIntegrationFact]
    public async Task Retry_publish_keeps_x_death_metadata_for_the_next_stage()
    {
        var settings = CreateSettings();
        await using var connection = await ConnectAsync(settings);
        await using var channel = await connection.CreateChannelAsync();
        await using var publishChannel = await connection.CreateChannelAsync(new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true));

        try
        {
            await RabbitMqTopology.DeclareAsync(channel, settings, CancellationToken.None);
            var retryPublisher = new RabbitMqRetryPublisher(publishChannel,
                NullLogger<RabbitMqRetryPublisher>.Instance);
            var source = new BasicProperties
            {
                MessageId = "retry-metadata-message",
                CorrelationId = "retry-metadata-correlation",
                ContentType = "application/json",
                Type = "OrderCreated",
                Headers = new Dictionary<string, object?>
                {
                    [RetryHeaderInspector.DeathHeader] = new List<object>
                    {
                        new Dictionary<string, object>
                        {
                            ["queue"] = settings.RetryQueues[0].QueueName,
                            ["reason"] = "expired",
                            ["count"] = 1L
                        }
                    }
                }
            };

            // Simulate republishing after the 5s stage to the 30s stage.
            await retryPublisher.PublishAsync(
                new RetryMessage("{}"u8.ToArray(), "retry-metadata-message",
                    "retry-metadata-correlation", "OrderCreated", 2, source),
                settings.RetryQueues[1].QueueName, CancellationToken.None);

            var received = await channel.BasicGetAsync(settings.RetryQueues[1].QueueName, autoAck: false);
            Assert.NotNull(received);
            var headers = received.BasicProperties.Headers!;

            // The broker accepted the client-supplied x-death table and preserved it.
            Assert.Equal(1,
                RetryHeaderInspector.GetIndexAfterLastExpiredStage(headers, settings.RetryQueues));
            Assert.Equal(2L, headers[RetryHeaderInspector.RetryAttemptHeader]);
            Assert.Equal(2, RetryHeaderInspector.GetNextRetryIndex(headers, settings.RetryQueues));

            await channel.BasicAckAsync(received.DeliveryTag, multiple: false);
        }
        finally
        {
            await BestEffortDeleteAsync(channel, settings);
        }
    }

    private static RabbitMqSettings CreateSettings() => new()
    {
        HostName = Host,
        Port = AmqpPort,
        UserName = UserName,
        Password = Password,
        VirtualHost = "/",
        QueueName = $"orders.it.{Guid.NewGuid():N}"
    };

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

    private static HttpClient CreateManagementClient()
    {
        var client = new HttpClient
        {
            BaseAddress = new Uri($"http://{Host}:{ManagementPort}/"),
            Timeout = TimeSpan.FromSeconds(10)
        };
        var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{UserName}:{Password}"));
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", credentials);
        return client;
    }

    private static async Task<JsonElement> GetQueueArgumentsAsync(HttpClient http, string queueName)
    {
        var encodedQueue = Uri.EscapeDataString(queueName);
        using var response = await http.GetAsync($"api/queues/%2F/{encodedQueue}");
        Assert.True(response.IsSuccessStatusCode,
            $"RabbitMQ management API returned {(int)response.StatusCode} for {queueName}.");

        using var document = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.True(document.RootElement.TryGetProperty("arguments", out var arguments),
            "RabbitMQ management API did not report queue arguments.");
        return arguments.Clone();
    }

    private static async Task BestEffortDeleteAsync(IChannel channel, RabbitMqSettings settings)
    {
        // Cleanup is best effort so a failed assertion still reports the original error.
        foreach (var queueName in settings.RetryQueues.Select(queue => queue.QueueName)
                     .Concat(settings.StatusRetryQueues.Select(queue => queue.QueueName))
                     .Append(settings.StatusDeadLetterQueue)
                     .Append(settings.DeadLetterQueue)
                     .Append(settings.StatusQueueName)
                     .Append(settings.QueueName))
        {
            try { await channel.QueueDeleteAsync(queueName); }
            catch (Exception) { /* Queue may not exist; cleanup is best effort. */ }
        }

        foreach (var exchange in new[] { settings.DeadLetterExchange, settings.StatusDeadLetterExchange })
        {
            try { await channel.ExchangeDeleteAsync(exchange); }
            catch (Exception) { /* Exchange may not exist; cleanup is best effort. */ }
        }
    }
}
