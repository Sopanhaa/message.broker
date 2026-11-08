using System.Collections.Concurrent;
using Shared.Contracts.Events;

namespace OrderConsumer.Worker;

public sealed record OrderState(
    Guid OrderId, OrderStatus Status, DateTimeOffset UpdatedAtUtc, string? Detail = null);

public interface IOrderStatusStore
{
    // Returns the stored state when the update is newer, or null when it is stale or a duplicate.
    OrderState? Apply(OrderStatusChanged update);

    OrderState? Get(Guid orderId);
}

// In-memory for this demo. A real consumer would persist this and make processing idempotent.
public sealed class OrderStatusStore : IOrderStatusStore
{
    private readonly ConcurrentDictionary<Guid, OrderState> _states = new();

    public OrderState? Apply(OrderStatusChanged update)
    {
        var state = new OrderState(update.OrderId, update.Status, update.UpdatedAtUtc, update.Detail);
        while (true)
        {
            if (!_states.TryGetValue(update.OrderId, out var current))
            {
                if (_states.TryAdd(update.OrderId, state)) return state;
                continue;
            }

            if (current.UpdatedAtUtc >= state.UpdatedAtUtc) return null;
            if (_states.TryUpdate(update.OrderId, state, current)) return state;
        }
    }

    public OrderState? Get(Guid orderId) =>
        _states.TryGetValue(orderId, out var state) ? state : null;
}
