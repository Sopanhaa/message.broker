namespace Shared.Contracts.Events;

public sealed record OrderCreated(
    Guid OrderId,
    string CustomerName,
    decimal TotalAmount,
    DateTimeOffset CreatedAtUtc,
    IReadOnlyList<OrderProduct>? Products = null);
