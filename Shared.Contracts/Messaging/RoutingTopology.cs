using RabbitMQ.Client;

namespace Shared.Contracts.Messaging;

// A topic queue and the pattern it is bound with.
public sealed record TopicBinding(string QueueName, string Pattern);

// Demo topology for fanout and topic routing. Every name is derived from the
// configured base queue so tests can use isolated topology without touching
// the order, status, retry, or dead-letter queues.
public static class RoutingTopology
{
    public const string OrderCreatedRoutingKey = "order.created";
    public const string OrderUpdatedRoutingKey = "order.updated";
    public const string OrderCancelledRoutingKey = "order.cancelled";

    public static string FanoutExchangeName(RabbitMqSettings settings) => $"{settings.QueueName}.fanout";

    public static string TopicExchangeName(RabbitMqSettings settings) => $"{settings.QueueName}.topic";

    public static string RoutingDeadLetterExchangeName(RabbitMqSettings settings) =>
        $"{settings.QueueName}.routing.dlx";

    public static string RoutingDeadLetterQueueName(RabbitMqSettings settings) =>
        $"{settings.QueueName}.routing.dead-letter";

    public static IReadOnlyList<string> FanoutQueueNames(RabbitMqSettings settings) =>
    [
        $"{settings.QueueName}.fanout.email",
        $"{settings.QueueName}.fanout.logging"
    ];

    public static IReadOnlyList<TopicBinding> TopicBindings(RabbitMqSettings settings) =>
    [
        new($"{settings.QueueName}.topic.order", "order.*"),
        new($"{settings.QueueName}.topic.notification", OrderCreatedRoutingKey),
        new($"{settings.QueueName}.topic.audit", "#")
    ];

    public static IReadOnlyList<string> TopicQueueNames(RabbitMqSettings settings) =>
        TopicBindings(settings).Select(binding => binding.QueueName).ToArray();

    public static IReadOnlyList<string> AllQueueNames(RabbitMqSettings settings) =>
        FanoutQueueNames(settings).Concat(TopicQueueNames(settings)).ToArray();

    public static async Task DeclareAsync(IChannel channel, RabbitMqSettings settings, CancellationToken token)
    {
        var deadLetterExchange = RoutingDeadLetterExchangeName(settings);
        var deadLetterQueue = RoutingDeadLetterQueueName(settings);

        await channel.ExchangeDeclareAsync(deadLetterExchange, ExchangeType.Direct,
            durable: true, autoDelete: false, cancellationToken: token);
        await channel.QueueDeclareAsync(deadLetterQueue, durable: true,
            exclusive: false, autoDelete: false, cancellationToken: token);

        await channel.ExchangeDeclareAsync(FanoutExchangeName(settings), ExchangeType.Fanout,
            durable: true, autoDelete: false, cancellationToken: token);
        await channel.ExchangeDeclareAsync(TopicExchangeName(settings), ExchangeType.Topic,
            durable: true, autoDelete: false, cancellationToken: token);

        foreach (var queueName in FanoutQueueNames(settings))
        {
            await DeclareQueueAsync(channel, queueName, deadLetterExchange, deadLetterQueue, token);
            // A fanout exchange ignores the routing key and copies to every binding.
            await channel.QueueBindAsync(queueName, FanoutExchangeName(settings), routingKey: "",
                cancellationToken: token);
        }

        foreach (var binding in TopicBindings(settings))
        {
            await DeclareQueueAsync(channel, binding.QueueName, deadLetterExchange, deadLetterQueue, token);
            await channel.QueueBindAsync(binding.QueueName, TopicExchangeName(settings), binding.Pattern,
                cancellationToken: token);
        }
    }

    private static async Task DeclareQueueAsync(IChannel channel, string queueName, string deadLetterExchange,
        string deadLetterQueue, CancellationToken token)
    {
        await channel.QueueDeclareAsync(queueName, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?>
            {
                ["x-dead-letter-exchange"] = deadLetterExchange,
                ["x-dead-letter-routing-key"] = queueName
            }, cancellationToken: token);

        // One shared DLQ keeps every routing demo failure in a single place.
        await channel.QueueBindAsync(deadLetterQueue, deadLetterExchange, queueName, cancellationToken: token);
    }
}
