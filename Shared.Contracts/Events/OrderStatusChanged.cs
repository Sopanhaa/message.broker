namespace Shared.Contracts.Events;

public sealed record OrderStatusChanged(
    Guid OrderId,
    OrderStatus Status,
    DateTimeOffset UpdatedAtUtc,
    string? Detail = null)
{
    public const int MaxDetailLength = 500;

    public static bool IsValid(OrderStatusChanged? update) =>
        update is not null &&
        update.OrderId != Guid.Empty &&
        Enum.IsDefined(update.Status) &&
        update.UpdatedAtUtc != default &&
        (update.Detail is null || update.Detail.Trim().Length <= MaxDetailLength);
}
