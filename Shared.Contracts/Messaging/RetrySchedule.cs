namespace Shared.Contracts.Messaging;

// A single delayed retry stage. The queue holds the message for Delay and then
// dead-letters it back to the original processing queue.
public sealed record RetryQueue(string QueueName, TimeSpan Delay);

public static class RetrySchedule
{
    // Attempt 1 waits 5 seconds, attempt 2 waits 30 seconds, attempt 3 waits 5 minutes.
    public static readonly IReadOnlyList<TimeSpan> Delays =
    [
        TimeSpan.FromSeconds(5),
        TimeSpan.FromSeconds(30),
        TimeSpan.FromMinutes(5)
    ];

    public static IReadOnlyList<RetryQueue> ForQueue(string queueName) =>
        Delays.Select(delay => new RetryQueue(
            $"{queueName}.retry.{FormatDelay(delay)}", delay)).ToArray();

    private static string FormatDelay(TimeSpan delay) =>
        delay.TotalSeconds < 60
            ? $"{delay.TotalSeconds:0}s"
            : $"{delay.TotalMinutes:0}m";
}
