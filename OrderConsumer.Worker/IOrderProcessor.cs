using Shared.Contracts.Events;

namespace OrderConsumer.Worker;

public interface IOrderProcessor
{
    Task ProcessAsync(OrderCreated order, CancellationToken cancellationToken);
}
