using Shared.Contracts.Events;

namespace OrderProducer.Api.Models;

public sealed record PublishTopicEventRequest(string? RoutingKey, string? EventType = null)
{
    public const int MaxRoutingKeyLength = 255;

    public Dictionary<string, string[]> Validate()
    {
        var errors = new Dictionary<string, string[]>();
        if (string.IsNullOrWhiteSpace(RoutingKey) || RoutingKey.Trim().Length > MaxRoutingKeyLength)
            errors[nameof(RoutingKey)] =
                [$"Routing key must contain 1 to {MaxRoutingKeyLength} non-whitespace characters (after trimming)."];
        if (EventType is not null &&
            (string.IsNullOrWhiteSpace(EventType) || EventType.Trim().Length > OrderRoutingEvent.MaxEventTypeLength))
            errors[nameof(EventType)] =
                [$"Event type must contain 1 to {OrderRoutingEvent.MaxEventTypeLength} non-whitespace characters (after trimming)."];
        return errors;
    }
}
