using Shared.Contracts.Events;

namespace OrderPersistence;

public interface IOrderStore
{
    Task<OrderSnapshot?> GetAsync(Guid orderId, CancellationToken cancellationToken);

    // Returns false when an order with the same ID already exists.
    Task<bool> TryCreateAsync(OrderCreated order, CancellationToken cancellationToken);

    // Applies the update only when it is newer than the stored state.
    // Returns the new state, or null when the order is missing or the update is stale.
    Task<OrderSnapshot?> ApplyStatusAsync(OrderStatusChanged update, CancellationToken cancellationToken);

    Task<bool> TryDeleteAsync(Guid orderId, CancellationToken cancellationToken);

    Task<bool> IsProcessedAsync(string messageId, CancellationToken cancellationToken);

    Task MarkProcessedAsync(string messageId, Guid orderId, string messageType,
        CancellationToken cancellationToken);
}
