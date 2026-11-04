using RabbitMQ.Client;

namespace Shared.Contracts.Messaging;

public static class RabbitMqTopology
{
    public static async Task DeclareAsync(IChannel channel, RabbitMqSettings settings, CancellationToken token)
    {
        await channel.ExchangeDeclareAsync(settings.DeadLetterExchange, ExchangeType.Direct,
            durable: true, autoDelete: false, cancellationToken: token);
        await channel.QueueDeclareAsync(settings.DeadLetterQueue, durable: true,
            exclusive: false, autoDelete: false, cancellationToken: token);
        await channel.QueueBindAsync(settings.DeadLetterQueue, settings.DeadLetterExchange,
            settings.QueueName, cancellationToken: token);
        await channel.QueueDeclareAsync(settings.QueueName, durable: true, exclusive: false,
            autoDelete: false, arguments: new Dictionary<string, object?>
            {
                ["x-dead-letter-exchange"] = settings.DeadLetterExchange,
                ["x-dead-letter-routing-key"] = settings.QueueName
            }, cancellationToken: token);
    }
}
