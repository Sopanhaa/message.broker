using OrderPersistence;
using Shared.Contracts.Events;

namespace OrderConsumer.Worker;

public sealed class OrderStatusProcessor(IOrderStore store, ILogger<OrderStatusProcessor> logger)
    : IOrderStatusProcessor
{
    public async Task ProcessAsync(OrderStatusChanged update, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var state = await store.ApplyStatusAsync(update, cancellationToken);
        if (state is null)
        {
            logger.LogInformation(
                "Ignored stale or unknown status update for order {OrderId}: {Status} at {UpdatedAtUtc}",
                update.OrderId, update.Status, update.UpdatedAtUtc);
        }
        else if (state.StatusDetail is null)
        {
            logger.LogInformation("Order {OrderId} status updated to {Status}", state.OrderId, state.Status);
        }
        else
        {
            logger.LogInformation("Order {OrderId} status updated to {Status}: {Detail}",
                state.OrderId, state.Status, state.StatusDetail);
        }
    }
}
