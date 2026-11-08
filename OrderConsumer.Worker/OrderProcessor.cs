using Shared.Contracts.Events;

namespace OrderConsumer.Worker;

public sealed class OrderProcessor(IOrderStatusStore statusStore, ILogger<OrderProcessor> logger)
    : IOrderProcessor
{
    public async Task ProcessAsync(OrderCreated order, CancellationToken cancellationToken)
    {
        logger.LogInformation("Processing order {OrderId}: customer={CustomerName}, total={TotalAmount}",
            order.OrderId, order.CustomerName, order.TotalAmount);

        statusStore.Apply(new OrderStatusChanged(order.OrderId, OrderStatus.Processing,
            DateTimeOffset.UtcNow, "Order accepted for processing"));

        foreach (var product in order.Products ?? [])
            logger.LogInformation("Order product: name={ProductName}, quantity={Quantity}, unitPrice={UnitPrice}",
                product.Name, product.Quantity, product.UnitPrice);

        // Simulate work until a business operation (for example, fulfillment) is available.
        // Real operations must tolerate redelivery of the same OrderId.
        await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);

        statusStore.Apply(new OrderStatusChanged(order.OrderId, OrderStatus.Completed, DateTimeOffset.UtcNow));
        logger.LogInformation("Completed order {OrderId}", order.OrderId);
    }
}
