using System.Text.Json.Serialization;

namespace Shared.Contracts.Events;

// Serialized as a string so status messages stay readable and new members can be added safely.
[JsonConverter(typeof(JsonStringEnumConverter<OrderStatus>))]
public enum OrderStatus
{
    Received,
    Processing,
    Completed,
    Failed
}
