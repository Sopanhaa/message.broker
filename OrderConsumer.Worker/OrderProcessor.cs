using Shared.Contracts.Events;

namespace OrderConsumer.Worker;

public sealed class OrderProcessor(ILogger<OrderProcessor> logger) : IOrderProcessor
{
    public async Task ProcessAsync(OrderCreated order, CancellationToken cancellationToken)
    {
        logger.LogInformation("Processing order {OrderId}: customer={CustomerName}, total={TotalAmount}",
            order.OrderId, order.CustomerName, order.TotalAmount);

        // Simulate work until a business operation (for example, fulfillment) is available.
        // Real operations must tolerate redelivery of the same OrderId.
        await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
    }
}
