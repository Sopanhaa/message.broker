using Shared.Contracts.Events;

namespace OrderConsumer.Worker;

public interface IOrderStatusProcessor
{
    Task ProcessAsync(OrderStatusChanged update, CancellationToken cancellationToken);
}
