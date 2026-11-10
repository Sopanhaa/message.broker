using System.Text.Json;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using OrderPersistence.Entities;
using Shared.Contracts.Events;

namespace OrderPersistence;

public sealed class EfOrderStore(IDbContextFactory<OrderDbContext> contextFactory) : IOrderStore
{
    public async Task<OrderSnapshot?> GetAsync(Guid orderId, CancellationToken cancellationToken)
    {
        await using var db = await contextFactory.CreateDbContextAsync(cancellationToken);
        var entity = await db.Orders.AsNoTracking()
            .FirstOrDefaultAsync(o => o.OrderId == orderId, cancellationToken);
        return entity is null ? null : ToSnapshot(entity);
    }

    public async Task<bool> TryCreateAsync(OrderCreated order, CancellationToken cancellationToken)
    {
        var entity = new OrderEntity
        {
            OrderId = order.OrderId,
            CustomerName = order.CustomerName,
            TotalAmount = order.TotalAmount,
            CreatedAtUtc = order.CreatedAtUtc,
            Status = OrderStatus.Received,
            StatusDetail = null,
            StatusUpdatedAtUtcTicks = order.CreatedAtUtc.UtcTicks,
            ProductsJson = order.Products is null
                ? null
                : JsonSerializer.Serialize(order.Products, JsonSerializerOptions.Web)
        };

        await using var db = await contextFactory.CreateDbContextAsync(cancellationToken);
        db.Orders.Add(entity);
        try
        {
            await db.SaveChangesAsync(cancellationToken);
            return true;
        }
        catch (DbUpdateException ex) when (IsUniqueConstraint(ex))
        {
            return false;
        }
    }

    public async Task<OrderSnapshot?> ApplyStatusAsync(OrderStatusChanged update, CancellationToken cancellationToken)
    {
        var detail = string.IsNullOrWhiteSpace(update.Detail) ? null : update.Detail.Trim();
        var ticks = update.UpdatedAtUtc.UtcTicks;

        await using var db = await contextFactory.CreateDbContextAsync(cancellationToken);
        // One atomic statement: stale or out-of-order updates match no rows.
        var affected = await db.Orders
            .Where(o => o.OrderId == update.OrderId && o.StatusUpdatedAtUtcTicks < ticks)
            .ExecuteUpdateAsync(setters => setters
                .SetProperty(o => o.Status, update.Status)
                .SetProperty(o => o.StatusDetail, detail)
                .SetProperty(o => o.StatusUpdatedAtUtcTicks, ticks), cancellationToken);
        if (affected == 0) return null;

        var entity = await db.Orders.AsNoTracking()
            .FirstOrDefaultAsync(o => o.OrderId == update.OrderId, cancellationToken);
        return entity is null ? null : ToSnapshot(entity);
    }

    public async Task<bool> TryDeleteAsync(Guid orderId, CancellationToken cancellationToken)
    {
        await using var db = await contextFactory.CreateDbContextAsync(cancellationToken);
        var affected = await db.Orders
            .Where(o => o.OrderId == orderId)
            .ExecuteDeleteAsync(cancellationToken);
        return affected > 0;
    }

    public async Task<bool> IsProcessedAsync(string messageId, CancellationToken cancellationToken)
    {
        await using var db = await contextFactory.CreateDbContextAsync(cancellationToken);
        return await db.ProcessedMessages.AsNoTracking()
            .AnyAsync(m => m.MessageId == messageId, cancellationToken);
    }

    public async Task MarkProcessedAsync(string messageId, Guid orderId, string messageType,
        CancellationToken cancellationToken)
    {
        await using var db = await contextFactory.CreateDbContextAsync(cancellationToken);
        db.ProcessedMessages.Add(new ProcessedMessageEntity
        {
            MessageId = messageId,
            OrderId = orderId,
            MessageType = messageType,
            ProcessedAtUtc = DateTimeOffset.UtcNow
        });
        try
        {
            await db.SaveChangesAsync(cancellationToken);
        }
        catch (DbUpdateException ex) when (IsUniqueConstraint(ex))
        {
            // Another consumer instance recorded the same message first.
        }
    }

    private static bool IsUniqueConstraint(DbUpdateException ex) =>
        ex.InnerException is SqliteException { SqliteErrorCode: 19 }; // SQLITE_CONSTRAINT

    private static OrderSnapshot ToSnapshot(OrderEntity entity) => new(
        entity.OrderId,
        entity.CustomerName,
        entity.TotalAmount,
        entity.CreatedAtUtc,
        string.IsNullOrWhiteSpace(entity.ProductsJson)
            ? []
            : JsonSerializer.Deserialize<OrderProduct[]>(entity.ProductsJson, JsonSerializerOptions.Web) ?? [],
        entity.Status,
        entity.StatusDetail,
        new DateTimeOffset(entity.StatusUpdatedAtUtcTicks, TimeSpan.Zero));
}
