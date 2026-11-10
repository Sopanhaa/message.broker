using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using OrderPersistence;
using Shared.Contracts.Events;

namespace OrderMessagingSystem.Tests;

public sealed class OrderPersistenceTests : IAsyncLifetime
{
    private readonly string _databasePath =
        Path.Combine(Path.GetTempPath(), $"order-persistence-tests-{Guid.NewGuid():N}.db");

    private OrderDbContext _db = null!;
    private EfOrderStore _store = null!;

    public async Task InitializeAsync()
    {
        var options = new DbContextOptionsBuilder<OrderDbContext>()
            .UseSqlite($"Data Source={_databasePath};Default Timeout=5")
            .Options;
        _db = new OrderDbContext(options);
        await OrderDatabase.InitializeAsync(_db);
        _store = new EfOrderStore(new TestContextFactory(options));
    }

    public Task DisposeAsync()
    {
        _db.Dispose();
        SqliteConnection.ClearAllPools();
        try { File.Delete(_databasePath); }
        catch (IOException) { }
        return Task.CompletedTask;
    }

    [Fact]
    public async Task Create_and_get_round_trips_the_order()
    {
        var order = new OrderCreated(Guid.NewGuid(), "Sopanha", 50m, DateTimeOffset.UtcNow,
            [new OrderProduct("Keyboard", 2, 20m), new OrderProduct("Mouse", 1, 10m)]);

        Assert.True(await _store.TryCreateAsync(order, CancellationToken.None));

        var snapshot = Assert.IsType<OrderSnapshot>(await _store.GetAsync(order.OrderId, CancellationToken.None));
        Assert.Equal(order.OrderId, snapshot.OrderId);
        Assert.Equal("Sopanha", snapshot.CustomerName);
        Assert.Equal(50m, snapshot.TotalAmount);
        Assert.Equal(OrderStatus.Received, snapshot.Status);
        Assert.Null(snapshot.StatusDetail);
        Assert.Equal(order.Products, snapshot.Products);
    }

    [Fact]
    public async Task Create_rejects_a_duplicate_order_id()
    {
        var order = new OrderCreated(Guid.NewGuid(), "Sopanha", 50m, DateTimeOffset.UtcNow);

        Assert.True(await _store.TryCreateAsync(order, CancellationToken.None));
        Assert.False(await _store.TryCreateAsync(order, CancellationToken.None));
    }

    [Fact]
    public async Task Apply_status_guards_against_stale_and_duplicate_updates()
    {
        var order = new OrderCreated(Guid.NewGuid(), "Sopanha", 50m, DateTimeOffset.UtcNow.AddMinutes(-1));
        Assert.True(await _store.TryCreateAsync(order, CancellationToken.None));
        var now = DateTimeOffset.UtcNow;

        var completed = await _store.ApplyStatusAsync(
            new OrderStatusChanged(order.OrderId, OrderStatus.Completed, now, " Delivered "), CancellationToken.None);
        Assert.NotNull(completed);
        Assert.Equal(OrderStatus.Completed, completed.Status);
        Assert.Equal("Delivered", completed.StatusDetail);

        Assert.Null(await _store.ApplyStatusAsync(
            new OrderStatusChanged(order.OrderId, OrderStatus.Completed, now), CancellationToken.None));
        Assert.Null(await _store.ApplyStatusAsync(
            new OrderStatusChanged(order.OrderId, OrderStatus.Processing, now.AddSeconds(-1)), CancellationToken.None));

        var snapshot = Assert.IsType<OrderSnapshot>(await _store.GetAsync(order.OrderId, CancellationToken.None));
        Assert.Equal(OrderStatus.Completed, snapshot.Status);
    }

    [Fact]
    public async Task Apply_status_for_an_unknown_order_returns_null()
    {
        var result = await _store.ApplyStatusAsync(
            new OrderStatusChanged(Guid.NewGuid(), OrderStatus.Completed, DateTimeOffset.UtcNow),
            CancellationToken.None);

        Assert.Null(result);
    }

    [Fact]
    public async Task Processed_message_ids_are_recorded_once()
    {
        var orderId = Guid.NewGuid();

        Assert.False(await _store.IsProcessedAsync("message-1", CancellationToken.None));
        await _store.MarkProcessedAsync("message-1", orderId, nameof(OrderCreated), CancellationToken.None);
        Assert.True(await _store.IsProcessedAsync("message-1", CancellationToken.None));

        // A concurrent redelivery may try to record the same ID again; it must not throw.
        await _store.MarkProcessedAsync("message-1", orderId, nameof(OrderCreated), CancellationToken.None);
        Assert.True(await _store.IsProcessedAsync("message-1", CancellationToken.None));
    }

    [Fact]
    public async Task Delete_removes_the_order()
    {
        var order = new OrderCreated(Guid.NewGuid(), "Sopanha", 50m, DateTimeOffset.UtcNow);
        Assert.True(await _store.TryCreateAsync(order, CancellationToken.None));

        Assert.True(await _store.TryDeleteAsync(order.OrderId, CancellationToken.None));
        Assert.Null(await _store.GetAsync(order.OrderId, CancellationToken.None));
        Assert.False(await _store.TryDeleteAsync(order.OrderId, CancellationToken.None));
    }

    private sealed class TestContextFactory(DbContextOptions<OrderDbContext> options)
        : IDbContextFactory<OrderDbContext>
    {
        public OrderDbContext CreateDbContext() => new(options);

        public Task<OrderDbContext> CreateDbContextAsync(CancellationToken cancellationToken = default) =>
            Task.FromResult(CreateDbContext());
    }
}
