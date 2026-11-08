using Shared.Contracts.Events;

namespace OrderConsumer.Worker;

public sealed class OrderStatusProcessor(IOrderStatusStore store, ILogger<OrderStatusProcessor> logger)
    : IOrderStatusProcessor
{
    public Task ProcessAsync(OrderStatusChanged update, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var state = store.Apply(update);
        if (state is null)
        {
            logger.LogInformation(
                "Ignored stale status update for order {OrderId}: {Status} at {UpdatedAtUtc}",
                update.OrderId, update.Status, update.UpdatedAtUtc);
        }
        else if (state.Detail is null)
        {
            logger.LogInformation("Order {OrderId} status updated to {Status}", state.OrderId, state.Status);
        }
        else
        {
            logger.LogInformation("Order {OrderId} status updated to {Status}: {Detail}",
                state.OrderId, state.Status, state.Detail);
        }

        return Task.CompletedTask;
    }
}
