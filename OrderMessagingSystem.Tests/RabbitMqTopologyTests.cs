using System.Reflection;
using RabbitMQ.Client;
using Shared.Contracts.Messaging;

namespace OrderMessagingSystem.Tests;

public sealed class RabbitMqTopologyTests
{
    [Fact]
    public async Task Declares_order_and_status_retry_topology()
    {
        var settings = new RabbitMqSettings();
        var channel = DispatchProxy.Create<IChannel, TopologyProxy>();
        var calls = (TopologyProxy)channel;

        await RabbitMqTopology.DeclareAsync(channel, settings, CancellationToken.None);

        AssertStream(calls, settings.QueueName, settings.DeadLetterExchange, settings.DeadLetterQueue,
            settings.RetryReturnRoutingKey, settings.RetryQueues);
        AssertStream(calls, settings.StatusQueueName, settings.StatusDeadLetterExchange,
            settings.StatusDeadLetterQueue, settings.StatusRetryReturnRoutingKey, settings.StatusRetryQueues);
    }

    [Fact]
    public void Retry_queue_names_are_derived_from_the_main_queue()
    {
        var settings = new RabbitMqSettings { QueueName = "orders" };

        Assert.Equal(["orders.retry.5s", "orders.retry.30s", "orders.retry.5m"],
            settings.RetryQueues.Select(queue => queue.QueueName));
        Assert.Equal(["orders.status.retry.5s", "orders.status.retry.30s", "orders.status.retry.5m"],
            settings.StatusRetryQueues.Select(queue => queue.QueueName));
    }

    private static void AssertStream(TopologyProxy calls, string queueName, string deadLetterExchange,
        string deadLetterQueue, string retryReturnRoutingKey, IReadOnlyList<RetryQueue> retryQueues)
    {
        Assert.Contains(calls.Exchanges,
            exchange => exchange.Name == deadLetterExchange &&
                        exchange.Type == ExchangeType.Direct &&
                        exchange.Durable);
        Assert.Contains(calls.Queues, queue => queue.Name == deadLetterQueue && queue.Durable);
        Assert.Contains(calls.Bindings,
            binding => binding.Queue == deadLetterQueue &&
                       binding.Exchange == deadLetterExchange &&
                       binding.RoutingKey == queueName);

        var mainQueue = Assert.Single(calls.Queues, queue => queue.Name == queueName);
        Assert.True(mainQueue.Durable);
        Assert.False(mainQueue.Exclusive);
        Assert.False(mainQueue.AutoDelete);
        Assert.Equal(deadLetterExchange, mainQueue.Arguments!["x-dead-letter-exchange"]);
        Assert.Equal(queueName, mainQueue.Arguments["x-dead-letter-routing-key"]);

        // Expiring retry messages come back to the processing queue, not the DLQ.
        Assert.Contains(calls.Bindings,
            binding => binding.Queue == queueName &&
                       binding.Exchange == deadLetterExchange &&
                       binding.RoutingKey == retryReturnRoutingKey);

        foreach (var retryQueue in retryQueues)
        {
            var declaration = Assert.Single(calls.Queues, queue => queue.Name == retryQueue.QueueName);
            Assert.True(declaration.Durable);
            Assert.False(declaration.Exclusive);
            Assert.False(declaration.AutoDelete);
            Assert.Equal((int)retryQueue.Delay.TotalMilliseconds,
                Assert.IsType<int>(declaration.Arguments!["x-message-ttl"]));
            Assert.Equal(deadLetterExchange, declaration.Arguments["x-dead-letter-exchange"]);
            Assert.Equal(retryReturnRoutingKey, declaration.Arguments["x-dead-letter-routing-key"]);
        }
    }

    public sealed record QueueDeclaration(string Name, bool Durable, bool Exclusive, bool AutoDelete,
        IDictionary<string, object?>? Arguments);

    public sealed record ExchangeDeclaration(string Name, string Type, bool Durable);

    public sealed record Binding(string Queue, string Exchange, string RoutingKey);

    public class TopologyProxy : DispatchProxy
    {
        public List<QueueDeclaration> Queues { get; } = [];
        public List<ExchangeDeclaration> Exchanges { get; } = [];
        public List<Binding> Bindings { get; } = [];

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            switch (targetMethod?.Name)
            {
                case nameof(IChannel.ExchangeDeclareAsync):
                    Exchanges.Add(new ExchangeDeclaration(
                        (string)args![0]!, (string)args[1]!, (bool)args[2]!));
                    return Task.CompletedTask;

                case nameof(IChannel.QueueDeclareAsync):
                    var name = (string)args![0]!;
                    Queues.Add(new QueueDeclaration(name, (bool)args[1]!, (bool)args[2]!, (bool)args[3]!,
                        args[4] as IDictionary<string, object?>));
                    return Task.FromResult(new QueueDeclareOk(name, 0, 0));

                case nameof(IChannel.QueueBindAsync):
                    Bindings.Add(new Binding((string)args![0]!, (string)args[1]!, (string)args[2]!));
                    return Task.CompletedTask;

                default:
                    throw new NotSupportedException(targetMethod?.Name);
            }
        }
    }
}
