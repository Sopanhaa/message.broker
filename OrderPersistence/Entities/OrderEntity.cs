using Shared.Contracts.Events;

namespace OrderPersistence.Entities;

public sealed class OrderEntity
{
    public Guid OrderId { get; set; }
    public string CustomerName { get; set; } = "";
    public decimal TotalAmount { get; set; }
    public DateTimeOffset CreatedAtUtc { get; set; }
    public OrderStatus Status { get; set; }
    public string? StatusDetail { get; set; }
    // Stored as ticks so the stale-update guard compares integers instead of SQLite text.
    public long StatusUpdatedAtUtcTicks { get; set; }
    public string? ProductsJson { get; set; }
}
