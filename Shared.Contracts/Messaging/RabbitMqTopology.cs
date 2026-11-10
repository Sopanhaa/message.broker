using RabbitMQ.Client;

namespace Shared.Contracts.Messaging;

public static class RabbitMqTopology
{
    public static async Task DeclareAsync(IChannel channel, RabbitMqSettings settings, CancellationToken token)
    {
        await DeclareQueueAsync(channel, settings.QueueName, settings.DeadLetterExchange,
            settings.DeadLetterQueue, settings.RetryReturnRoutingKey, settings.RetryQueues, token);
        await DeclareQueueAsync(channel, settings.StatusQueueName, settings.StatusDeadLetterExchange,
            settings.StatusDeadLetterQueue, settings.StatusRetryReturnRoutingKey, settings.StatusRetryQueues,
            token);
    }

    private static async Task DeclareQueueAsync(IChannel channel, string queueName, string deadLetterExchange,
        string deadLetterQueue, string retryReturnRoutingKey, IReadOnlyList<RetryQueue> retryQueues,
        CancellationToken token)
    {
        await channel.ExchangeDeclareAsync(deadLetterExchange, ExchangeType.Direct,
            durable: true, autoDelete: false, cancellationToken: token);
        await channel.QueueDeclareAsync(deadLetterQueue, durable: true,
            exclusive: false, autoDelete: false, cancellationToken: token);
        await channel.QueueBindAsync(deadLetterQueue, deadLetterExchange, queueName, cancellationToken: token);

        await channel.QueueDeclareAsync(queueName, durable: true, exclusive: false,
            autoDelete: false, arguments: new Dictionary<string, object?>
            {
                // A permanently rejected message keeps the existing DLQ behaviour.
                ["x-dead-letter-exchange"] = deadLetterExchange,
                ["x-dead-letter-routing-key"] = queueName
            }, cancellationToken: token);

        // Expiring retry messages are dead-lettered to the same exchange but with
        // a different routing key, so they return to this queue rather than the DLQ.
        await channel.QueueBindAsync(queueName, deadLetterExchange, retryReturnRoutingKey,
            cancellationToken: token);

        foreach (var retryQueue in retryQueues)
        {
            await channel.QueueDeclareAsync(retryQueue.QueueName, durable: true, exclusive: false,
                autoDelete: false, arguments: new Dictionary<string, object?>
                {
                    ["x-message-ttl"] = checked((int)retryQueue.Delay.TotalMilliseconds),
                    ["x-dead-letter-exchange"] = deadLetterExchange,
                    ["x-dead-letter-routing-key"] = retryReturnRoutingKey
                }, cancellationToken: token);
        }
    }
}
