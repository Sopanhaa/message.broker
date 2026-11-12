namespace Shared.Contracts.Events;

// Demo-only payload used to exercise fanout and topic routing.
// These events are not persisted and do not affect the Orders table.
public sealed record OrderRoutingEvent(
    Guid EventId,
    string EventType,
    DateTimeOffset OccurredAtUtc,
    string? Detail = null)
{
    public const int MaxEventTypeLength = 100;
    public const int MaxDetailLength = 500;

    public static bool IsValid(OrderRoutingEvent? routingEvent) =>
        routingEvent is not null &&
        routingEvent.EventId != Guid.Empty &&
        !string.IsNullOrWhiteSpace(routingEvent.EventType) &&
        routingEvent.EventType.Trim().Length <= MaxEventTypeLength &&
        routingEvent.OccurredAtUtc != default &&
        (routingEvent.Detail is null || routingEvent.Detail.Trim().Length <= MaxDetailLength);
}

public static class OrderRoutingEventTypes
{
    public const string Created = "OrderCreated";
    public const string Updated = "OrderUpdated";
    public const string Cancelled = "OrderCancelled";
}
