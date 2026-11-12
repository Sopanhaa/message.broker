using System.Reflection;
using RabbitMQ.Client;
using Shared.Contracts.Messaging;

namespace OrderMessagingSystem.Tests;

public sealed class RoutingTopologyTests
{
    [Fact]
    public async Task Declares_fanout_topic_and_shared_routing_dlq()
    {
        var settings = new RabbitMqSettings { QueueName = "orders.routing.tests" };
        var channel = DispatchProxy.Create<IChannel, TopologyProxy>();
        var calls = (TopologyProxy)channel;

        await RoutingTopology.DeclareAsync(channel, settings, CancellationToken.None);

        var fanout = RoutingTopology.FanoutExchangeName(settings);
        var topic = RoutingTopology.TopicExchangeName(settings);
        var deadLetterExchange = RoutingTopology.RoutingDeadLetterExchangeName(settings);
        var deadLetterQueue = RoutingTopology.RoutingDeadLetterQueueName(settings);

        Assert.Contains(calls.Exchanges,
            exchange => exchange.Name == fanout && exchange.Type == ExchangeType.Fanout && exchange.Durable);
        Assert.Contains(calls.Exchanges,
            exchange => exchange.Name == topic && exchange.Type == ExchangeType.Topic && exchange.Durable);
        Assert.Contains(calls.Exchanges,
            exchange => exchange.Name == deadLetterExchange &&
                        exchange.Type == ExchangeType.Direct &&
                        exchange.Durable);
        Assert.Contains(calls.Queues,
            queue => queue.Name == deadLetterQueue && queue.Durable && !queue.Exclusive && !queue.AutoDelete);

        foreach (var queueName in RoutingTopology.FanoutQueueNames(settings))
        {
            AssertRoutingQueue(calls, queueName, deadLetterExchange, deadLetterQueue);
            Assert.Contains(calls.Bindings,
                binding => binding.Queue == queueName &&
                           binding.Exchange == fanout &&
                           binding.RoutingKey == "");
        }

        foreach (var binding in RoutingTopology.TopicBindings(settings))
        {
            AssertRoutingQueue(calls, binding.QueueName, deadLetterExchange, deadLetterQueue);
            Assert.Contains(calls.Bindings,
                candidate => candidate.Queue == binding.QueueName &&
                             candidate.Exchange == topic &&
                             candidate.RoutingKey == binding.Pattern);
        }
    }

    [Fact]
    public void Topic_bindings_use_star_and_hash_wildcards()
    {
        var settings = new RabbitMqSettings { QueueName = "orders" };
        var bindings = RoutingTopology.TopicBindings(settings);

        Assert.Equal(3, bindings.Count);
        Assert.Contains(bindings,
            binding => binding.QueueName == "orders.topic.order" && binding.Pattern == "order.*");
        Assert.Contains(bindings,
            binding => binding.QueueName == "orders.topic.notification" &&
                       binding.Pattern == RoutingTopology.OrderCreatedRoutingKey);
        Assert.Contains(bindings,
            binding => binding.QueueName == "orders.topic.audit" && binding.Pattern == "#");
    }

    [Fact]
    public void Names_are_derived_from_the_base_queue()
    {
        var settings = new RabbitMqSettings { QueueName = "custom" };

        Assert.Equal("custom.fanout", RoutingTopology.FanoutExchangeName(settings));
        Assert.Equal("custom.topic", RoutingTopology.TopicExchangeName(settings));
        Assert.Equal("custom.routing.dlx", RoutingTopology.RoutingDeadLetterExchangeName(settings));
        Assert.Equal("custom.routing.dead-letter", RoutingTopology.RoutingDeadLetterQueueName(settings));
        Assert.Equal(new[] { "custom.fanout.email", "custom.fanout.logging" },
            RoutingTopology.FanoutQueueNames(settings));
        Assert.Equal(new[] { "custom.topic.order", "custom.topic.notification", "custom.topic.audit" },
            RoutingTopology.TopicQueueNames(settings));
    }

    private static void AssertRoutingQueue(TopologyProxy calls, string queueName, string deadLetterExchange,
        string deadLetterQueue)
    {
        var queue = Assert.Single(calls.Queues, candidate => candidate.Name == queueName);
        Assert.True(queue.Durable);
        Assert.False(queue.Exclusive);
        Assert.False(queue.AutoDelete);
        Assert.Equal(deadLetterExchange, queue.Arguments!["x-dead-letter-exchange"]);
        Assert.Equal(queueName, queue.Arguments["x-dead-letter-routing-key"]);

        Assert.Contains(calls.Bindings,
            binding => binding.Queue == deadLetterQueue &&
                       binding.Exchange == deadLetterExchange &&
                       binding.RoutingKey == queueName);
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
                    Bindings.Add(new Binding((string)args![0]!, (string)args![1]!, (string)args![2]!));
                    return Task.CompletedTask;

                default:
                    throw new NotSupportedException(targetMethod?.Name);
            }
        }
    }
}
