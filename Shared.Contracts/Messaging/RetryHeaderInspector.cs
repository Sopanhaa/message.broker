using System.Collections;
using System.Text;

namespace Shared.Contracts.Messaging;

// Reads RabbitMQ's x-death header and our own x-retry-attempt header.
//
// RabbitMQ appends an x-death entry when it dead-letters a message. An entry
// from a retry queue has reason="expired" and queue="<retry queue>". Entries
// caused by rejecting a message from the main queue have reason="rejected"
// and are deliberately ignored here.
public static class RetryHeaderInspector
{
    public const string DeathHeader = "x-death";
    public const string RetryAttemptHeader = "x-retry-attempt";

    // Returns the zero-based index of the next retry stage, or schedule.Count
    // when every stage has already been used and the message belongs in the DLQ.
    public static int GetNextRetryIndex(IDictionary<string, object?>? headers,
        IReadOnlyList<RetryQueue> schedule)
    {
        if (schedule.Count == 0) return 0;

        var fromDeath = GetIndexAfterLastExpiredStage(headers, schedule);
        var fromAttempt = GetRetryAttempt(headers, schedule.Count);
        return Math.Clamp(Math.Max(fromDeath, fromAttempt), 0, schedule.Count);
    }

    // x-retry-attempt is written by our retry publisher. It is a second,
    // independent guard so a lost or stripped x-death header cannot create an
    // unbounded loop.
    public static int GetRetryAttempt(IDictionary<string, object?>? headers, int max)
    {
        if (headers is null || !TryGetHeader(headers, RetryAttemptHeader, out var raw))
            return 0;

        var value = AsInt64(raw);
        return value is null ? 0 : (int)Math.Clamp(value.Value, 0, max);
    }

    public static int GetIndexAfterLastExpiredStage(IDictionary<string, object?>? headers,
        IReadOnlyList<RetryQueue> schedule)
    {
        if (headers is null || !TryGetHeader(headers, DeathHeader, out var raw))
            return 0;
        if (raw is not IEnumerable entries || raw is string)
            return 0;

        var lastStage = -1;
        foreach (var entry in entries)
        {
            if (entry is not IDictionary table)
                continue;

            if (!TryGetTableValue(table, "reason", out var reason) ||
                !string.Equals(AsString(reason), "expired", StringComparison.Ordinal))
                continue;

            if (!TryGetTableValue(table, "queue", out var queue))
                continue;

            var queueName = AsString(queue);
            if (queueName is null)
                continue;

            for (var index = 0; index < schedule.Count; index++)
            {
                if (!string.Equals(schedule[index].QueueName, queueName, StringComparison.Ordinal))
                    continue;

                lastStage = Math.Max(lastStage, index);
                break;
            }
        }

        return lastStage + 1;
    }

    private static bool TryGetHeader(IDictionary<string, object?> headers, string key, out object? value)
    {
        if (headers.TryGetValue(key, out value))
            return true;

        // AMQP field-table keys are case-sensitive, but tolerate alternate casing
        // so a producer using a different client cannot silently reset the counter.
        foreach (var pair in headers)
        {
            if (!string.Equals(pair.Key, key, StringComparison.OrdinalIgnoreCase))
                continue;

            value = pair.Value;
            return true;
        }

        value = null;
        return false;
    }

    private static bool TryGetTableValue(IDictionary table, string key, out object? value)
    {
        if (table.Contains(key))
        {
            value = table[key];
            return true;
        }

        foreach (DictionaryEntry entry in table)
        {
            if (entry.Key is not string name ||
                !string.Equals(name, key, StringComparison.OrdinalIgnoreCase))
                continue;

            value = entry.Value;
            return true;
        }

        value = null;
        return false;
    }

    private static string? AsString(object? value) => value switch
    {
        string text => text,
        byte[] bytes => Encoding.UTF8.GetString(bytes),
        ReadOnlyMemory<byte> memory => Encoding.UTF8.GetString(memory.Span),
        _ => null
    };

    private static long? AsInt64(object? value) => value switch
    {
        long number => number,
        int number => number,
        short number => number,
        sbyte number => number,
        byte number => number,
        ushort number => number,
        uint number => number,
        ulong number => number <= long.MaxValue ? (long)number : long.MaxValue,
        _ => null
    };
}
