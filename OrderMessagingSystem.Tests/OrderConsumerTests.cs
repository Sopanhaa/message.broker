using System.Collections.Concurrent;
using System.Reflection;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using OrderConsumer.Worker;
using OrderPersistence;
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
        yield return [ValidOrder with { Products = [] }];
        yield return [ValidOrder with { Products = [null!] }];
        yield return [ValidOrder with { Products = [new(" ", 1, 50m)] }];
        yield return [ValidOrder with { Products = [new(new string('x', 101), 1, 50m)] }];
        yield return [ValidOrder with { Products = [new("Mouse", -1, 50m)] }];
        yield return [ValidOrder with { Products = [new("Mouse", 1, 0m)] }];
        yield return [ValidOrder with { Products = [new("Mouse", 1, 10m)] }];
        yield return [ValidOrder with { Products = [new("Mouse", 2, decimal.MaxValue)] }];
    }

    [Fact]
    public async Task Product_details_survive_message_serialization_and_reach_the_processor()
    {
        var order = ValidOrder with { Products = [new("Keyboard", 2, 20m), new("Mouse", 1, 10m)] };
        var processor = new FakeProcessor();
        var (channel, calls) = CreateChannel();

        await CreateHandler(processor).HandleAsync(channel, Serialize(order), 42, CancellationToken.None);

        var processed = Assert.Single(processor.Orders);
        Assert.Equal(order.Products, processed.Products);
        Assert.Equal(("ack", 42UL, false, false), Assert.Single(calls.Settlements));
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

    [Fact]
    public async Task Duplicate_order_message_id_is_acknowledged_without_reprocessing()
    {
        var store = new InMemoryOrderStore();
        var processor = new FakeProcessor();
        var (channel, calls) = CreateChannel();
        const string messageId = "order-message-1";
        await store.MarkProcessedAsync(messageId, ValidOrder.OrderId, nameof(OrderCreated), CancellationToken.None);

        await CreateHandler(processor, store).HandleAsync(channel, Serialize(ValidOrder), 42,
            CancellationToken.None, messageId);

        Assert.Empty(processor.Orders);
        Assert.Equal(("ack", 42UL, false, false), Assert.Single(calls.Settlements));
    }

    [Fact]
    public async Task Processed_order_message_id_is_recorded_for_deduplication()
    {
        var store = new InMemoryOrderStore();
        var processor = new FakeProcessor();
        var (channel, calls) = CreateChannel();
        const string messageId = "order-message-2";

        await CreateHandler(processor, store).HandleAsync(channel, Serialize(ValidOrder), 42,
            CancellationToken.None, messageId);

        Assert.Single(processor.Orders);
        Assert.True(await store.IsProcessedAsync(messageId, CancellationToken.None));
        Assert.Equal(("ack", 42UL, false, false), Assert.Single(calls.Settlements));
    }

    [Fact]
    public async Task Valid_status_update_is_acknowledged_and_stored()
    {
        var store = new InMemoryOrderStore();
        var orderId = Guid.NewGuid();
        store.Seed(new OrderCreated(orderId, "Sopanha", 50m, DateTimeOffset.UtcNow));
        var processor = new OrderStatusProcessor(store, NullLogger<OrderStatusProcessor>.Instance);
        var (channel, calls) = CreateChannel();
        var update = new OrderStatusChanged(orderId, OrderStatus.Completed, DateTimeOffset.UtcNow, " Delivered ");

        await CreateStatusHandler(processor).HandleAsync(channel, Serialize(update), 7, CancellationToken.None);

        Assert.Equal(("ack", 7UL, false, false), Assert.Single(calls.Settlements));
        var state = Assert.IsType<OrderSnapshot>(await store.GetAsync(update.OrderId, CancellationToken.None));
        Assert.Equal(OrderStatus.Completed, state.Status);
        Assert.Equal("Delivered", state.StatusDetail);
    }

    [Fact]
    public async Task Stale_and_duplicate_status_updates_are_ignored()
    {
        var store = new InMemoryOrderStore();
        var orderId = Guid.NewGuid();
        var now = DateTimeOffset.UtcNow;
        store.Seed(new OrderCreated(orderId, "Sopanha", 50m, now.AddMinutes(-1)));
        var completed = new OrderStatusChanged(orderId, OrderStatus.Completed, now);

        Assert.NotNull(await store.ApplyStatusAsync(completed, CancellationToken.None));
        Assert.Null(await store.ApplyStatusAsync(completed, CancellationToken.None));
        Assert.Null(await store.ApplyStatusAsync(
            new OrderStatusChanged(orderId, OrderStatus.Processing, now.AddSeconds(-1)), CancellationToken.None));

        var state = Assert.IsType<OrderSnapshot>(await store.GetAsync(orderId, CancellationToken.None));
        Assert.Equal(OrderStatus.Completed, state.Status);
    }

    [Fact]
    public async Task Status_update_for_an_unknown_order_is_ignored()
    {
        var store = new InMemoryOrderStore();

        var state = await store.ApplyStatusAsync(
            new OrderStatusChanged(Guid.NewGuid(), OrderStatus.Completed, DateTimeOffset.UtcNow),
            CancellationToken.None);

        Assert.Null(state);
    }

    [Theory]
    [InlineData("")]
    [InlineData("{broken")]
    [InlineData("null")]
    [InlineData("{}")]
    public async Task Invalid_status_json_is_dead_lettered_without_processing(string json)
    {
        var processor = new FakeStatusProcessor();
        var (channel, calls) = CreateChannel();

        await CreateStatusHandler(processor).HandleAsync(channel, Encoding.UTF8.GetBytes(json), 7,
            CancellationToken.None);

        Assert.Empty(processor.Updates);
        Assert.Equal(("nack", 7UL, false, false), Assert.Single(calls.Settlements));
    }

    public static IEnumerable<object[]> InvalidStatusUpdates()
    {
        yield return [new OrderStatusChanged(Guid.Empty, OrderStatus.Completed, DateTimeOffset.UtcNow)];
        yield return [new OrderStatusChanged(Guid.NewGuid(), (OrderStatus)99, DateTimeOffset.UtcNow)];
        yield return [new OrderStatusChanged(Guid.NewGuid(), OrderStatus.Completed, default)];
        yield return
        [
            new OrderStatusChanged(Guid.NewGuid(), OrderStatus.Completed, DateTimeOffset.UtcNow,
                new string('x', OrderStatusChanged.MaxDetailLength + 1))
        ];
    }

    [Theory]
    [MemberData(nameof(InvalidStatusUpdates))]
    public async Task Invalid_status_update_is_dead_lettered_without_processing(OrderStatusChanged update)
    {
        var processor = new FakeStatusProcessor();
        var (channel, calls) = CreateChannel();

        await CreateStatusHandler(processor).HandleAsync(channel, Serialize(update), 7, CancellationToken.None);

        Assert.Empty(processor.Updates);
        Assert.Equal(("nack", 7UL, false, false), Assert.Single(calls.Settlements));
    }

    [Fact]
    public async Task Status_processing_failure_dead_letters_the_message()
    {
        var processor = new FakeStatusProcessor(_ => throw new InvalidOperationException("Status failed"));
        var (channel, calls) = CreateChannel();

        await CreateStatusHandler(processor).HandleAsync(channel,
            Serialize(new OrderStatusChanged(Guid.NewGuid(), OrderStatus.Completed, DateTimeOffset.UtcNow)),
            7, CancellationToken.None);

        Assert.Equal(("nack", 7UL, false, false), Assert.Single(calls.Settlements));
    }

    [Fact]
    public async Task Cancellation_before_status_processing_leaves_message_unsettled()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var processor = new FakeStatusProcessor();
        var (channel, calls) = CreateChannel();

        await CreateStatusHandler(processor).HandleAsync(channel,
            Serialize(new OrderStatusChanged(Guid.NewGuid(), OrderStatus.Completed, DateTimeOffset.UtcNow)),
            7, cancellation.Token);

        Assert.Empty(processor.Updates);
        Assert.Empty(calls.Settlements);
    }

    [Fact]
    public async Task Duplicate_status_message_id_is_acknowledged_without_reprocessing()
    {
        var store = new InMemoryOrderStore();
        var orderId = Guid.NewGuid();
        store.Seed(new OrderCreated(orderId, "Sopanha", 50m, DateTimeOffset.UtcNow));
        var processor = new FakeStatusProcessor();
        var (channel, calls) = CreateChannel();
        const string messageId = "status-message-1";
        await store.MarkProcessedAsync(messageId, orderId, nameof(OrderStatusChanged), CancellationToken.None);

        await CreateStatusHandler(processor, store).HandleAsync(channel,
            Serialize(new OrderStatusChanged(orderId, OrderStatus.Completed, DateTimeOffset.UtcNow)),
            7, CancellationToken.None, messageId);

        Assert.Empty(processor.Updates);
        Assert.Equal(("ack", 7UL, false, false), Assert.Single(calls.Settlements));
    }

    [Fact]
    public async Task Status_settlement_failure_propagates_for_reconnection_without_a_second_settlement()
    {
        var store = new InMemoryOrderStore();
        var orderId = Guid.NewGuid();
        store.Seed(new OrderCreated(orderId, "Sopanha", 50m, DateTimeOffset.UtcNow));
        var processor = new OrderStatusProcessor(store, NullLogger<OrderStatusProcessor>.Instance);
        var (channel, calls) = CreateChannel();
        calls.Failure = new IOException("Channel closed during settlement");

        var error = await Assert.ThrowsAsync<IOException>(() => CreateStatusHandler(processor).HandleAsync(
            channel, Serialize(new OrderStatusChanged(orderId, OrderStatus.Completed, DateTimeOffset.UtcNow)),
            7, CancellationToken.None));

        Assert.Same(calls.Failure, error);
        Assert.Equal(("ack", 7UL, false, false), Assert.Single(calls.Settlements));
    }

    [Fact]
    public async Task Transient_order_failure_is_published_to_first_retry_queue_and_original_is_acked()
    {
        var processor = new FakeProcessor(_ => throw new IOException("Disk temporarily unavailable"));
        var publisher = new FakeRetryPublisher();
        var (channel, calls) = CreateChannel();
        var properties = Properties("order-message-1", "correlation-1");

        await CreateHandler(processor).HandleAsync(channel, Serialize(ValidOrder), 42,
            CancellationToken.None, "order-message-1", properties, publisher);

        var (message, queue) = Assert.Single(publisher.Published);
        Assert.Equal("orders.retry.5s", queue);
        Assert.Equal(1, message.Attempt);
        Assert.Equal("order-message-1", message.MessageId);
        Assert.Equal("correlation-1", message.CorrelationId);
        Assert.Equal(nameof(OrderCreated), message.MessageType);
        Assert.Same(properties, message.SourceProperties);
        Assert.Equal(("ack", 42UL, false, false), Assert.Single(calls.Settlements));
    }

    [Theory]
    [InlineData(0, "orders.retry.5s", 1)]
    [InlineData(1, "orders.retry.30s", 2)]
    [InlineData(2, "orders.retry.5m", 3)]
    public async Task Transient_failure_selects_the_next_retry_stage(
        int completedStages, string expectedQueue, int expectedAttempt)
    {
        var retryQueues = RetrySchedule.ForQueue("orders");
        var completedNames = retryQueues.Take(completedStages).Select(queue => queue.QueueName).ToArray();
        var processor = new FakeProcessor(_ => throw new TimeoutException("Database timeout"));
        var publisher = new FakeRetryPublisher();
        var (channel, calls) = CreateChannel();

        await CreateHandler(processor).HandleAsync(channel, Serialize(ValidOrder), 42,
            CancellationToken.None, "message-id",
            Properties("message-id", headers: DeathHeaders(completedNames)), publisher);

        var (message, queue) = Assert.Single(publisher.Published);
        Assert.Equal(expectedQueue, queue);
        Assert.Equal(expectedAttempt, message.Attempt);
        Assert.Equal(("ack", 42UL, false, false), Assert.Single(calls.Settlements));
    }

    [Fact]
    public async Task Exhausted_retries_are_dead_lettered_without_another_publish()
    {
        var allRetryQueues = RetrySchedule.ForQueue("orders").Select(queue => queue.QueueName).ToArray();
        var processor = new FakeProcessor(_ => throw new TimeoutException("Database timeout"));
        var publisher = new FakeRetryPublisher();
        var (channel, calls) = CreateChannel();

        await CreateHandler(processor).HandleAsync(channel, Serialize(ValidOrder), 42,
            CancellationToken.None, "message-id",
            Properties("message-id", headers: DeathHeaders(allRetryQueues)), publisher);

        Assert.Empty(publisher.Published);
        Assert.Equal(("nack", 42UL, false, false), Assert.Single(calls.Settlements));
    }

    [Fact]
    public async Task Unexpected_retry_attempt_header_stops_the_loop_at_the_dlq()
    {
        var processor = new FakeProcessor(_ => throw new TimeoutException("Database timeout"));
        var publisher = new FakeRetryPublisher();
        var (channel, calls) = CreateChannel();
        var headers = new Dictionary<string, object?>
        {
            [RetryHeaderInspector.RetryAttemptHeader] = 3L
        };

        await CreateHandler(processor).HandleAsync(channel, Serialize(ValidOrder), 42,
            CancellationToken.None, "message-id",
            Properties("message-id", headers: headers), publisher);

        Assert.Empty(publisher.Published);
        Assert.Equal(("nack", 42UL, false, false), Assert.Single(calls.Settlements));
    }

    [Fact]
    public async Task Retry_publish_failure_leaves_the_original_delivery_unsettled()
    {
        var processor = new FakeProcessor(_ => throw new IOException("Disk temporarily unavailable"));
        var failure = new IOException("Retry publish was not confirmed");
        var publisher = new FakeRetryPublisher { Failure = failure };
        var (channel, calls) = CreateChannel();

        var error = await Assert.ThrowsAsync<IOException>(() => CreateHandler(processor).HandleAsync(
            channel, Serialize(ValidOrder), 42, CancellationToken.None, "message-id",
            Properties("message-id"), publisher));

        Assert.Same(failure, error);
        Assert.Empty(calls.Settlements);
    }

    [Fact]
    public async Task Sqlite_busy_is_transient_and_routed_to_retry()
    {
        var processor = new FakeProcessor(_ => throw new SqliteException("database is locked", 5));
        var publisher = new FakeRetryPublisher();
        var (channel, calls) = CreateChannel();

        await CreateHandler(processor).HandleAsync(channel, Serialize(ValidOrder), 42,
            CancellationToken.None, "message-id", Properties("message-id"), publisher);

        Assert.Equal("orders.retry.5s", Assert.Single(publisher.Published).QueueName);
        Assert.Equal(("ack", 42UL, false, false), Assert.Single(calls.Settlements));
    }

    [Fact]
    public async Task Sqlite_constraint_violation_is_permanent_and_dead_lettered()
    {
        var processor = new FakeProcessor(_ =>
            throw new SqliteException("constraint failed", 19));
        var publisher = new FakeRetryPublisher();
        var (channel, calls) = CreateChannel();

        await CreateHandler(processor).HandleAsync(channel, Serialize(ValidOrder), 42,
            CancellationToken.None, "message-id", Properties("message-id"), publisher);

        Assert.Empty(publisher.Published);
        Assert.Equal(("nack", 42UL, false, false), Assert.Single(calls.Settlements));
    }

    [Fact]
    public async Task Transient_deduplication_failure_is_routed_to_retry()
    {
        var store = new FailingOrderStore(new SqliteException("database is locked", 6));
        var publisher = new FakeRetryPublisher();
        var (channel, calls) = CreateChannel();

        await CreateHandler(new FakeProcessor(), store).HandleAsync(channel, Serialize(ValidOrder), 42,
            CancellationToken.None, "message-id", Properties("message-id"), publisher);

        Assert.Equal("orders.retry.5s", Assert.Single(publisher.Published).QueueName);
        Assert.Equal(("ack", 42UL, false, false), Assert.Single(calls.Settlements));
    }

    [Fact]
    public async Task Duplicate_message_is_acked_even_when_it_has_retry_headers()
    {
        var store = new InMemoryOrderStore();
        var processor = new FakeProcessor();
        var publisher = new FakeRetryPublisher();
        var (channel, calls) = CreateChannel();
        const string messageId = "duplicate-with-retry-headers";
        await store.MarkProcessedAsync(messageId, ValidOrder.OrderId, nameof(OrderCreated),
            CancellationToken.None);

        await CreateHandler(processor, store).HandleAsync(channel, Serialize(ValidOrder), 42,
            CancellationToken.None, messageId,
            Properties(messageId, headers: DeathHeaders("orders.retry.5s")), publisher);

        Assert.Empty(processor.Orders);
        Assert.Empty(publisher.Published);
        Assert.Equal(("ack", 42UL, false, false), Assert.Single(calls.Settlements));
    }

    [Fact]
    public async Task Transient_status_failure_uses_the_status_retry_queue()
    {
        var processor = new FakeStatusProcessor(_ => throw new TimeoutException("Database timeout"));
        var publisher = new FakeRetryPublisher();
        var (channel, calls) = CreateChannel();
        var update = new OrderStatusChanged(Guid.NewGuid(), OrderStatus.Completed, DateTimeOffset.UtcNow);

        await CreateStatusHandler(processor).HandleAsync(channel, Serialize(update), 7,
            CancellationToken.None, "status-message-1",
            Properties("status-message-1", "status-correlation"), publisher);

        var (message, queue) = Assert.Single(publisher.Published);
        Assert.Equal("orders.status.retry.5s", queue);
        Assert.Equal("status-message-1", message.MessageId);
        Assert.Equal("status-correlation", message.CorrelationId);
        Assert.Equal(nameof(OrderStatusChanged), message.MessageType);
        Assert.Equal(("ack", 7UL, false, false), Assert.Single(calls.Settlements));
    }

    [Fact]
    public async Task Exhausted_status_retries_are_dead_lettered()
    {
        var allRetryQueues = RetrySchedule.ForQueue("orders.status")
            .Select(queue => queue.QueueName).ToArray();
        var processor = new FakeStatusProcessor(_ => throw new IOException("Disk temporarily unavailable"));
        var publisher = new FakeRetryPublisher();
        var (channel, calls) = CreateChannel();
        var update = new OrderStatusChanged(Guid.NewGuid(), OrderStatus.Completed, DateTimeOffset.UtcNow);

        await CreateStatusHandler(processor).HandleAsync(channel, Serialize(update), 7,
            CancellationToken.None, "status-message-1",
            Properties("status-message-1", headers: DeathHeaders(allRetryQueues)), publisher);

        Assert.Empty(publisher.Published);
        Assert.Equal(("nack", 7UL, false, false), Assert.Single(calls.Settlements));
    }

    private static BasicProperties Properties(string? messageId = null, string? correlationId = null,
        IDictionary<string, object?>? headers = null) =>
        new()
        {
            MessageId = messageId,
            CorrelationId = correlationId,
            Type = "OrderMessage",
            ContentType = "application/json",
            Headers = headers
        };

    private static Dictionary<string, object?> DeathHeaders(params string[] retryQueueNames)
    {
        var entries = retryQueueNames.Select(queueName => (object)new Dictionary<string, object>
        {
            ["queue"] = queueName,
            ["reason"] = "expired",
            ["count"] = 1L
        }).ToList();

        return new Dictionary<string, object?> { [RetryHeaderInspector.DeathHeader] = entries };
    }

    private static byte[] Serialize<T>(T message) =>
        JsonSerializer.SerializeToUtf8Bytes(message, JsonSerializerOptions.Web);

    private static OrderMessageHandler CreateHandler(IOrderProcessor processor, IOrderStore? store = null)
    {
        var settings = new RabbitMqSettings();
        return new(processor, store ?? new InMemoryOrderStore(), settings,
            NullLogger<OrderMessageHandler>.Instance,
            new RetryScheduler(NullLogger<RetryScheduler>.Instance));
    }

    private static OrderStatusMessageHandler CreateStatusHandler(IOrderStatusProcessor processor,
        IOrderStore? store = null)
    {
        var settings = new RabbitMqSettings();
        return new(processor, store ?? new InMemoryOrderStore(), settings,
            NullLogger<OrderStatusMessageHandler>.Instance,
            new RetryScheduler(NullLogger<RetryScheduler>.Instance));
    }

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

    private sealed class InMemoryOrderStore : IOrderStore
    {
        private readonly ConcurrentDictionary<Guid, OrderSnapshot> _orders = new();
        private readonly ConcurrentDictionary<string, byte> _processedMessages = new();

        public void Seed(OrderCreated order) => _orders[order.OrderId] = new OrderSnapshot(
            order.OrderId, order.CustomerName, order.TotalAmount, order.CreatedAtUtc,
            order.Products ?? [], OrderStatus.Received, null, order.CreatedAtUtc);

        public Task<OrderSnapshot?> GetAsync(Guid orderId, CancellationToken cancellationToken) =>
            Task.FromResult(_orders.TryGetValue(orderId, out var state) ? state : null);

        public Task<bool> TryCreateAsync(OrderCreated order, CancellationToken cancellationToken)
        {
            Seed(order);
            return Task.FromResult(true);
        }

        public Task<OrderSnapshot?> ApplyStatusAsync(OrderStatusChanged update,
            CancellationToken cancellationToken)
        {
            while (true)
            {
                if (!_orders.TryGetValue(update.OrderId, out var current))
                    return Task.FromResult<OrderSnapshot?>(null);
                if (current.StatusUpdatedAtUtc >= update.UpdatedAtUtc)
                    return Task.FromResult<OrderSnapshot?>(null);

                var next = current with
                {
                    Status = update.Status,
                    StatusDetail = string.IsNullOrWhiteSpace(update.Detail) ? null : update.Detail.Trim(),
                    StatusUpdatedAtUtc = update.UpdatedAtUtc
                };
                if (_orders.TryUpdate(update.OrderId, next, current))
                    return Task.FromResult<OrderSnapshot?>(next);
            }
        }

        public Task<bool> TryDeleteAsync(Guid orderId, CancellationToken cancellationToken) =>
            Task.FromResult(_orders.TryRemove(orderId, out _));

        public Task<bool> IsProcessedAsync(string messageId, CancellationToken cancellationToken) =>
            Task.FromResult(_processedMessages.ContainsKey(messageId));

        public Task MarkProcessedAsync(string messageId, Guid orderId, string messageType,
            CancellationToken cancellationToken)
        {
            _processedMessages.TryAdd(messageId, 0);
            return Task.CompletedTask;
        }
    }

    private sealed class FailingOrderStore(Exception failure) : IOrderStore
    {
        public Task<OrderSnapshot?> GetAsync(Guid orderId, CancellationToken cancellationToken) =>
            Task.FromException<OrderSnapshot?>(failure);

        public Task<bool> TryCreateAsync(OrderCreated order, CancellationToken cancellationToken) =>
            Task.FromException<bool>(failure);

        public Task<OrderSnapshot?> ApplyStatusAsync(OrderStatusChanged update,
            CancellationToken cancellationToken) => Task.FromException<OrderSnapshot?>(failure);

        public Task<bool> TryDeleteAsync(Guid orderId, CancellationToken cancellationToken) =>
            Task.FromException<bool>(failure);

        public Task<bool> IsProcessedAsync(string messageId, CancellationToken cancellationToken) =>
            Task.FromException<bool>(failure);

        public Task MarkProcessedAsync(string messageId, Guid orderId, string messageType,
            CancellationToken cancellationToken) => Task.FromException(failure);
    }

    private sealed class FakeRetryPublisher : IRetryPublisher
    {
        public List<(RetryMessage Message, string QueueName)> Published { get; } = [];
        public Exception? Failure { get; set; }

        public Task PublishAsync(RetryMessage message, string queueName, CancellationToken cancellationToken)
        {
            if (Failure is not null) return Task.FromException(Failure);
            Published.Add((message, queueName));
            return Task.CompletedTask;
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

    private sealed class FakeStatusProcessor(Func<CancellationToken, Task>? process = null) : IOrderStatusProcessor
    {
        public List<OrderStatusChanged> Updates { get; } = [];

        public async Task ProcessAsync(OrderStatusChanged update, CancellationToken cancellationToken)
        {
            Updates.Add(update);
            if (process is not null) await process(cancellationToken);
        }
    }
}
