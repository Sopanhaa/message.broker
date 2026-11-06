using System.Reflection;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using OrderConsumer.Worker;
using RabbitMQ.Client;
using Shared.Contracts.Events;
using Shared.Contracts.Messaging;

namespace OrderMessagingSystem.Tests;

public sealed class OrderConsumerTests
{
    private static readonly OrderCreated ValidOrder = new(Guid.NewGuid(), "Sopanha", 50m, DateTimeOffset.UtcNow);

    [Fact]
    public async Task Valid_order_is_acknowledged_only_after_processing_completes()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var processor = new FakeProcessor(async token =>
        {
            entered.SetResult();
            await completed.Task.WaitAsync(token);
        });
        var (channel, calls) = CreateChannel();
        var handling = CreateHandler(processor).HandleAsync(channel, Serialize(ValidOrder), 42, CancellationToken.None);

        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Empty(calls.Settlements);
        completed.SetResult();
        await handling.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(ValidOrder, Assert.Single(processor.Orders));
        Assert.Equal(("ack", 42UL, false, false), Assert.Single(calls.Settlements));
    }

    [Theory]
    [InlineData("")]
    [InlineData("{broken")]
    [InlineData("null")]
    [InlineData("{}")]
    [InlineData("[]")]
    public async Task Invalid_json_is_dead_lettered_without_processing(string json)
    {
        var processor = new FakeProcessor();
        var (channel, calls) = CreateChannel();

        await CreateHandler(processor).HandleAsync(channel, Encoding.UTF8.GetBytes(json), 42, CancellationToken.None);

        Assert.Empty(processor.Orders);
        Assert.Equal(("nack", 42UL, false, false), Assert.Single(calls.Settlements));
    }

    public static IEnumerable<object[]> InvalidOrders()
    {
        yield return [ValidOrder with { OrderId = Guid.Empty }];
        yield return [ValidOrder with { CustomerName = null! }];
        yield return [ValidOrder with { CustomerName = "   " }];
        yield return [ValidOrder with { CustomerName = new string('x', 101) }];
        yield return [ValidOrder with { TotalAmount = 0 }];
        yield return [ValidOrder with { TotalAmount = -1 }];
        yield return [ValidOrder with { CreatedAtUtc = default }];
    }

    [Theory]
    [MemberData(nameof(InvalidOrders))]
    public async Task Invalid_order_is_dead_lettered_without_processing(OrderCreated order)
    {
        var processor = new FakeProcessor();
        var (channel, calls) = CreateChannel();

        await CreateHandler(processor).HandleAsync(channel, Serialize(order), 42, CancellationToken.None);

        Assert.Empty(processor.Orders);
        Assert.Equal(("nack", 42UL, false, false), Assert.Single(calls.Settlements));
    }

    [Fact]
    public async Task Processing_failure_dead_letters_the_message()
    {
        var processor = new FakeProcessor(_ => throw new InvalidOperationException("Processing failed"));
        var (channel, calls) = CreateChannel();

        await CreateHandler(processor).HandleAsync(channel, Serialize(ValidOrder), 42, CancellationToken.None);

        Assert.Equal(("nack", 42UL, false, false), Assert.Single(calls.Settlements));
    }

    [Fact]
    public async Task Cancellation_before_processing_leaves_message_unsettled()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var processor = new FakeProcessor();
        var (channel, calls) = CreateChannel();

        await CreateHandler(processor).HandleAsync(channel, Serialize(ValidOrder), 42, cancellation.Token);

        Assert.Empty(processor.Orders);
        Assert.Empty(calls.Settlements);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Cancellation_during_processing_leaves_message_unsettled(bool throwProcessingError)
    {
        using var cancellation = new CancellationTokenSource();
        var processor = new FakeProcessor(_ =>
        {
            cancellation.Cancel();
            if (throwProcessingError) throw new InvalidOperationException("Processing interrupted");
            return Task.CompletedTask;
        });
        var (channel, calls) = CreateChannel();

        await CreateHandler(processor).HandleAsync(channel, Serialize(ValidOrder), 42, cancellation.Token);

        Assert.Empty(calls.Settlements);
    }

    [Fact]
    public async Task Cancellation_interrupts_in_flight_processing_without_settling_the_message()
    {
        using var cancellation = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var processor = new FakeProcessor(async token =>
        {
            entered.SetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, token);
        });
        var (channel, calls) = CreateChannel();
        var handling = CreateHandler(processor).HandleAsync(channel, Serialize(ValidOrder), 42, cancellation.Token);

        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await cancellation.CancelAsync();
        await handling.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Empty(calls.Settlements);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Settlement_failure_propagates_for_reconnection_without_a_second_settlement(bool processingFails)
    {
        var processor = new FakeProcessor(_ => processingFails
            ? Task.FromException(new InvalidOperationException("Processing failed"))
            : Task.CompletedTask);
        var (channel, calls) = CreateChannel();
        calls.Failure = new IOException("Channel closed during settlement");

        var error = await Assert.ThrowsAsync<IOException>(() =>
            CreateHandler(processor).HandleAsync(channel, Serialize(ValidOrder), 42, CancellationToken.None));

        Assert.Same(calls.Failure, error);
        Assert.Equal((processingFails ? "nack" : "ack", 42UL, false, false), Assert.Single(calls.Settlements));
    }

    private static byte[] Serialize(OrderCreated order) => JsonSerializer.SerializeToUtf8Bytes(order, JsonSerializerOptions.Web);

    private static OrderMessageHandler CreateHandler(IOrderProcessor processor) =>
        new(processor, new RabbitMqSettings(), NullLogger<OrderMessageHandler>.Instance);

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
            Settlements.Add((operation, (ulong)args![0]!, (bool)args[1]!, operation == "nack" && (bool)args[2]!));
            return Failure is null ? ValueTask.CompletedTask : ValueTask.FromException(Failure);
        }
    }

    private sealed class FakeProcessor(Func<CancellationToken, Task>? process = null) : IOrderProcessor
    {
        public List<OrderCreated> Orders { get; } = [];

        public async Task ProcessAsync(OrderCreated order, CancellationToken cancellationToken)
        {
            Orders.Add(order);
            if (process is not null) await process(cancellationToken);
        }
    }
}
