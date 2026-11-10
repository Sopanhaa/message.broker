using Shared.Contracts.Events;

namespace OrderPersistence;

// Read model returned by GET /orders/{orderId}.
public sealed record OrderSnapshot(
    Guid OrderId,
    string CustomerName,
    decimal TotalAmount,
    DateTimeOffset CreatedAtUtc,
    IReadOnlyList<OrderProduct> Products,
    OrderStatus Status,
    string? StatusDetail,
    DateTimeOffset StatusUpdatedAtUtc);
