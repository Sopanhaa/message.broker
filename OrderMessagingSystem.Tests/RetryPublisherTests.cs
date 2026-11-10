using System.Reflection;
using Microsoft.Extensions.Logging.Abstractions;
using OrderConsumer.Worker;
using RabbitMQ.Client;
using Shared.Contracts.Messaging;

namespace OrderMessagingSystem.Tests;

public sealed class RetryPublisherTests
{
    [Fact]
    public async Task Retry_publish_preserves_identity_headers_and_payload()
    {
        var channel = DispatchProxy.Create<IChannel, PublishProxy>();
        var calls = (PublishProxy)channel;
        var publisher = new RabbitMqRetryPublisher(channel,
            NullLogger<RabbitMqRetryPublisher>.Instance);
        var body = "payload"u8.ToArray();
        var originalHeaders = new Dictionary<string, object?>
        {
            ["custom-header"] = "custom-value",
            [RetryHeaderInspector.DeathHeader] = new List<object>
            {
                new Dictionary<string, object>
                {
                    ["queue"] = "orders.retry.5s",
                    ["reason"] = "expired",
                    ["count"] = 1L
                }
            }
        };
        var source = new BasicProperties
        {
            MessageId = "message-1",
            CorrelationId = "correlation-1",
            Type = nameof(Shared.Contracts.Events.OrderCreated),
            ContentType = "application/json",
            ReplyTo = "reply-to",
            AppId = "producer",
            Headers = originalHeaders
        };

        await publisher.PublishAsync(
            new RetryMessage(body, null, null, "OrderCreated", 2, source),
            "orders.retry.30s", CancellationToken.None);

        var published = Assert.Single(calls.Published);
        Assert.Equal("", published.Exchange);
        Assert.Equal("orders.retry.30s", published.RoutingKey);
        Assert.True(published.Mandatory);
        Assert.Equal(body, published.Body.ToArray());

        var properties = published.Properties;
        Assert.True(properties.Persistent);
        Assert.Equal("message-1", properties.MessageId);
        Assert.Equal("correlation-1", properties.CorrelationId);
        Assert.Equal(nameof(Shared.Contracts.Events.OrderCreated), properties.Type);
        Assert.Equal("application/json", properties.ContentType);
        Assert.Equal("reply-to", properties.ReplyTo);
        Assert.Equal("producer", properties.AppId);
        Assert.Equal("custom-value", properties.Headers!["custom-header"]);
        Assert.Equal(2L, properties.Headers[RetryHeaderInspector.RetryAttemptHeader]);
        Assert.True(properties.Headers.ContainsKey(RetryHeaderInspector.DeathHeader));
    }

    [Fact]
    public async Task Retry_publish_uses_the_explicit_message_id_when_properties_are_missing()
    {
        var channel = DispatchProxy.Create<IChannel, PublishProxy>();
        var calls = (PublishProxy)channel;
        var publisher = new RabbitMqRetryPublisher(channel,
            NullLogger<RabbitMqRetryPublisher>.Instance);

        await publisher.PublishAsync(
            new RetryMessage("payload"u8.ToArray(), "fallback-message-id", "fallback-correlation",
                "OrderCreated", 1, null),
            "orders.retry.5s", CancellationToken.None);

        var properties = Assert.Single(calls.Published).Properties;
        Assert.Equal("fallback-message-id", properties.MessageId);
        Assert.Equal("fallback-correlation", properties.CorrelationId);
        Assert.Equal("OrderCreated", properties.Type);
        Assert.Equal(1L, properties.Headers![RetryHeaderInspector.RetryAttemptHeader]);
    }

    [Fact]
    public async Task Publish_failure_is_not_swallowed()
    {
        var channel = DispatchProxy.Create<IChannel, PublishProxy>();
        var calls = (PublishProxy)channel;
        calls.Failure = new IOException("Broker closed the channel");
        var publisher = new RabbitMqRetryPublisher(channel,
            NullLogger<RabbitMqRetryPublisher>.Instance);

        var error = await Assert.ThrowsAsync<IOException>(() => publisher.PublishAsync(
            new RetryMessage("payload"u8.ToArray(), "message-1", null, "OrderCreated", 1, null),
            "orders.retry.5s", CancellationToken.None));

        Assert.Same(calls.Failure, error);
    }

    public sealed record PublishCall(string Exchange, string RoutingKey, bool Mandatory,
        IReadOnlyBasicProperties Properties, ReadOnlyMemory<byte> Body);

    public class PublishProxy : DispatchProxy
    {
        public List<PublishCall> Published { get; } = [];
        public Exception? Failure { get; set; }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod?.Name != nameof(IChannel.BasicPublishAsync))
                throw new NotSupportedException(targetMethod?.Name);

            Published.Add(new PublishCall(
                (string)args![0]!,
                (string)args[1]!,
                (bool)args[2]!,
                (IReadOnlyBasicProperties)args[3]!,
                (ReadOnlyMemory<byte>)args[4]!));

            return Failure is null ? ValueTask.CompletedTask : ValueTask.FromException(Failure);
        }
    }
}
