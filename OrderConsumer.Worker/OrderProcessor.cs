using Shared.Contracts.Events;

namespace OrderConsumer.Worker;

public sealed class OrderProcessor(ILogger<OrderProcessor> logger) : IOrderProcessor
{
    public async Task ProcessAsync(OrderCreated order, CancellationToken cancellationToken)
    {
        logger.LogInformation("Processing order {OrderId}: customer={CustomerName}, total={TotalAmount}",
            order.OrderId, order.CustomerName, order.TotalAmount);

        foreach (var product in order.Products ?? [])
            logger.LogInformation("Order product: name={ProductName}, quantity={Quantity}, unitPrice={UnitPrice}",
                product.Name, product.Quantity, product.UnitPrice);

        // Simulate work until a business operation (for example, fulfillment) is available.
        // Real operations must tolerate redelivery of the same OrderId.
        await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
    }
}
