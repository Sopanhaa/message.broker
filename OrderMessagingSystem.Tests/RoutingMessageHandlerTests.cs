using System.Reflection;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using OrderConsumer.Worker;
using RabbitMQ.Client;
using Shared.Contracts.Events;

namespace OrderMessagingSystem.Tests;

public sealed class RoutingMessageHandlerTests
{
    private const string QueueName = "orders.fanout.email";

    [Fact]
    public async Task Valid_message_is_acknowledged()
    {
        var (channel, calls) = CreateChannel();
        var routingEvent = new OrderRoutingEvent(Guid.NewGuid(), OrderRoutingEventTypes.Created,
            DateTimeOffset.UtcNow);

        await CreateHandler().HandleAsync(channel, Serialize(routingEvent), 42, QueueName,
            CancellationToken.None);

        Assert.Equal(("ack", 42UL, false, false), Assert.Single(calls.Settlements));
    }

    [Theory]
    [InlineData("")]
    [InlineData("{broken")]
    [InlineData("null")]
    [InlineData("{}")]
    public async Task Invalid_json_is_dead_lettered(string json)
    {
        var (channel, calls) = CreateChannel();

        await CreateHandler().HandleAsync(channel, Encoding.UTF8.GetBytes(json), 42, QueueName,
            CancellationToken.None);

        Assert.Equal(("nack", 42UL, false, false), Assert.Single(calls.Settlements));
    }

    public static IEnumerable<object[]> InvalidEvents()
    {
        yield return [new OrderRoutingEvent(Guid.Empty, OrderRoutingEventTypes.Created, DateTimeOffset.UtcNow)];
        yield return [new OrderRoutingEvent(Guid.NewGuid(), "", DateTimeOffset.UtcNow)];
        yield return [new OrderRoutingEvent(Guid.NewGuid(), "   ", DateTimeOffset.UtcNow)];
        yield return
        [
            new OrderRoutingEvent(Guid.NewGuid(), new string('x', OrderRoutingEvent.MaxEventTypeLength + 1),
                DateTimeOffset.UtcNow)
        ];
        yield return [new OrderRoutingEvent(Guid.NewGuid(), OrderRoutingEventTypes.Created, default)];
        yield return
        [
            new OrderRoutingEvent(Guid.NewGuid(), OrderRoutingEventTypes.Created, DateTimeOffset.UtcNow,
                new string('x', OrderRoutingEvent.MaxDetailLength + 1))
        ];
    }

    [Theory]
    [MemberData(nameof(InvalidEvents))]
    public async Task Invalid_event_is_dead_lettered(OrderRoutingEvent routingEvent)
    {
        var (channel, calls) = CreateChannel();

        await CreateHandler().HandleAsync(channel, Serialize(routingEvent), 42, QueueName,
            CancellationToken.None);

        Assert.Equal(("nack", 42UL, false, false), Assert.Single(calls.Settlements));
    }

    [Fact]
    public async Task Cancellation_leaves_message_unsettled()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var (channel, calls) = CreateChannel();
        var routingEvent = new OrderRoutingEvent(Guid.NewGuid(), OrderRoutingEventTypes.Created,
            DateTimeOffset.UtcNow);

        await CreateHandler().HandleAsync(channel, Serialize(routingEvent), 42, QueueName,
            cancellation.Token);

        Assert.Empty(calls.Settlements);
    }

    [Fact]
    public async Task Acknowledgment_failure_propagates_without_a_second_settlement()
    {
        var (channel, calls) = CreateChannel();
        calls.Failure = new IOException("Channel closed during settlement");
        var routingEvent = new OrderRoutingEvent(Guid.NewGuid(), OrderRoutingEventTypes.Created,
            DateTimeOffset.UtcNow);

        var error = await Assert.ThrowsAsync<IOException>(() => CreateHandler().HandleAsync(
            channel, Serialize(routingEvent), 42, QueueName, CancellationToken.None));

        Assert.Same(calls.Failure, error);
        Assert.Equal(("ack", 42UL, false, false), Assert.Single(calls.Settlements));
    }

    private static RoutingMessageHandler CreateHandler() =>
        new(NullLogger<RoutingMessageHandler>.Instance);

    private static byte[] Serialize<T>(T message) =>
        JsonSerializer.SerializeToUtf8Bytes(message, JsonSerializerOptions.Web);

    private static (IChannel Channel, ChannelProxy Calls) CreateChannel()
    {
        var channel = DispatchProxy.Create<IChannel, ChannelProxy>();
        return (channel, (ChannelProxy)channel);
    }

    // Only acknowledgments are supported so unexpected broker operations fail the test.
    public class ChannelProxy : DispatchProxy
    {
        public List<(string Operation, ulong DeliveryTag, bool Multiple, bool Requeue)> Settlements { get; } = [];
        public Exception? Failure { get; set; }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            var operation = targetMethod?.Name switch
            {
                nameof(IChannel.BasicAckAsync) => "ack",
                nameof(IChannel.BasicNackAsync) => "nack",
                _ => throw new NotSupportedException(targetMethod?.Name)
            };
            Settlements.Add((operation, (ulong)args![0]!, (bool)args[1]!,
                operation == "nack" && (bool)args[2]!));
            return Failure is null ? ValueTask.CompletedTask : ValueTask.FromException(Failure);
        }
    }
}
