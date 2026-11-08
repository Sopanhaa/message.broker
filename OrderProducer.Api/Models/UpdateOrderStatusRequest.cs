using Shared.Contracts.Events;

namespace OrderProducer.Api.Models;

public sealed record UpdateOrderStatusRequest(OrderStatus? Status, string? Detail = null)
{
    public Dictionary<string, string[]> Validate()
    {
        var errors = new Dictionary<string, string[]>();
        if (Status is null || !Enum.IsDefined(Status.Value))
            errors[nameof(Status)] = ["Status must be one of: Received, Processing, Completed, Failed."];
        if (Detail is not null && Detail.Trim().Length > OrderStatusChanged.MaxDetailLength)
            errors[nameof(Detail)] =
                [$"Detail must contain at most {OrderStatusChanged.MaxDetailLength} characters after trimming."];
        return errors;
    }
}
