using OrderPersistence;
using Shared.Contracts.Events;

namespace OrderConsumer.Worker;

public sealed class OrderProcessor(IOrderStore store, ILogger<OrderProcessor> logger) : IOrderProcessor
{
    public async Task ProcessAsync(OrderCreated order, CancellationToken cancellationToken)
    {
        logger.LogInformation("Processing order {OrderId}: customer={CustomerName}, total={TotalAmount}",
            order.OrderId, order.CustomerName, order.TotalAmount);

        await ApplyAsync(new OrderStatusChanged(order.OrderId, OrderStatus.Processing,
            DateTimeOffset.UtcNow, "Order accepted for processing"), cancellationToken);

        foreach (var product in order.Products ?? [])
            logger.LogInformation("Order product: name={ProductName}, quantity={Quantity}, unitPrice={UnitPrice}",
                product.Name, product.Quantity, product.UnitPrice);

        // Simulate work until a business operation (for example, fulfillment) is available.
        // Real operations must tolerate redelivery of the same OrderId.
        await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);

        await ApplyAsync(new OrderStatusChanged(order.OrderId, OrderStatus.Completed, DateTimeOffset.UtcNow),
            cancellationToken);
        logger.LogInformation("Completed order {OrderId}", order.OrderId);
    }

    private async Task ApplyAsync(OrderStatusChanged update, CancellationToken cancellationToken)
    {
        if (await store.ApplyStatusAsync(update, cancellationToken) is null)
            logger.LogWarning("Order {OrderId} was not found; status {Status} was not recorded",
                update.OrderId, update.Status);
    }
}
