namespace OrderPersistence.Entities;

// Consumer inbox: one row per broker message that was fully processed.
public sealed class ProcessedMessageEntity
{
    public string MessageId { get; set; } = "";
    public Guid OrderId { get; set; }
    public string MessageType { get; set; } = "";
    public DateTimeOffset ProcessedAtUtc { get; set; }
}
