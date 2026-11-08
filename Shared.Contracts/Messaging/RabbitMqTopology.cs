using RabbitMQ.Client;

namespace Shared.Contracts.Messaging;

public static class RabbitMqTopology
{
    public static async Task DeclareAsync(IChannel channel, RabbitMqSettings settings, CancellationToken token)
    {
        await DeclareQueueAsync(channel, settings.QueueName, settings.DeadLetterExchange,
            settings.DeadLetterQueue, token);
        await DeclareQueueAsync(channel, settings.StatusQueueName, settings.StatusDeadLetterExchange,
            settings.StatusDeadLetterQueue, token);
    }

    private static async Task DeclareQueueAsync(IChannel channel, string queueName, string deadLetterExchange,
        string deadLetterQueue, CancellationToken token)
    {
        await channel.ExchangeDeclareAsync(deadLetterExchange, ExchangeType.Direct,
            durable: true, autoDelete: false, cancellationToken: token);
        await channel.QueueDeclareAsync(deadLetterQueue, durable: true,
            exclusive: false, autoDelete: false, cancellationToken: token);
        await channel.QueueBindAsync(deadLetterQueue, deadLetterExchange, queueName, cancellationToken: token);
        await channel.QueueDeclareAsync(queueName, durable: true, exclusive: false,
            autoDelete: false, arguments: new Dictionary<string, object?>
            {
                ["x-dead-letter-exchange"] = deadLetterExchange,
                ["x-dead-letter-routing-key"] = queueName
            }, cancellationToken: token);
    }
}
