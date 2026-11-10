using System.Text;
using Shared.Contracts.Messaging;

namespace OrderMessagingSystem.Tests;

public sealed class RetryHeaderInspectorTests
{
    private static readonly IReadOnlyList<RetryQueue> OrderSchedule = RetrySchedule.ForQueue("orders");
    private static readonly IReadOnlyList<RetryQueue> StatusSchedule = RetrySchedule.ForQueue("orders.status");

    [Fact]
    public void Schedule_matches_the_documented_delays_and_names()
    {
        Assert.Collection(OrderSchedule,
            stage => AssertStage(stage, "orders.retry.5s", TimeSpan.FromSeconds(5)),
            stage => AssertStage(stage, "orders.retry.30s", TimeSpan.FromSeconds(30)),
            stage => AssertStage(stage, "orders.retry.5m", TimeSpan.FromMinutes(5)));

        Assert.Collection(StatusSchedule,
            stage => AssertStage(stage, "orders.status.retry.5s", TimeSpan.FromSeconds(5)),
            stage => AssertStage(stage, "orders.status.retry.30s", TimeSpan.FromSeconds(30)),
            stage => AssertStage(stage, "orders.status.retry.5m", TimeSpan.FromMinutes(5)));
    }

    [Fact]
    public void No_headers_selects_the_first_stage()
    {
        Assert.Equal(0, RetryHeaderInspector.GetNextRetryIndex(null, OrderSchedule));
        Assert.Equal(0, RetryHeaderInspector.GetNextRetryIndex(
            new Dictionary<string, object?>(), OrderSchedule));
    }

    [Fact]
    public void Expired_5s_selects_the_30s_stage()
    {
        var headers = Death(
            Entry("orders.retry.5s", "expired", 1L));

        Assert.Equal(1, RetryHeaderInspector.GetNextRetryIndex(headers, OrderSchedule));
    }

    [Fact]
    public void Expired_5s_and_30s_select_the_5m_stage()
    {
        var headers = Death(
            Entry("orders.retry.5s", "expired", 1L),
            Entry("orders.retry.30s", "expired", 1L));

        Assert.Equal(2, RetryHeaderInspector.GetNextRetryIndex(headers, OrderSchedule));
    }

    [Fact]
    public void All_retry_stages_expired_selects_the_dlq()
    {
        var headers = Death(
            Entry("orders.retry.5s", "expired", 1L),
            Entry("orders.retry.30s", "expired", 1L),
            Entry("orders.retry.5m", "expired", 1L));

        Assert.Equal(OrderSchedule.Count,
            RetryHeaderInspector.GetNextRetryIndex(headers, OrderSchedule));
    }

    [Fact]
    public void Rejected_main_queue_event_does_not_advance_the_retry_stage()
    {
        var headers = Death(
            Entry("orders", "rejected", 4L),
            Entry("orders.retry.5s", "rejected", 1L));

        Assert.Equal(0, RetryHeaderInspector.GetNextRetryIndex(headers, OrderSchedule));
    }

    [Fact]
    public void Unrelated_expired_queue_does_not_advance_the_retry_stage()
    {
        var headers = Death(
            Entry("some.other.queue", "expired", 10L),
            Entry("orders.status.retry.5s", "expired", 1L));

        Assert.Equal(0, RetryHeaderInspector.GetNextRetryIndex(headers, OrderSchedule));
    }

    [Fact]
    public void Rabbit_mq_header_types_are_parsed()
    {
        // Real x-death values use long/int counts and string or byte[] text.
        // This also omits the count field to prove it is not required.
        var headers = new Dictionary<string, object?>
        {
            [RetryHeaderInspector.DeathHeader] = new List<object>
            {
                new Dictionary<string, object>
                {
                    ["queue"] = Encoding.UTF8.GetBytes("orders.retry.5s"),
                    ["reason"] = Encoding.UTF8.GetBytes("expired")
                },
                new Dictionary<string, object?>
                {
                    ["queue"] = "orders.retry.30s",
                    ["reason"] = "expired",
                    ["count"] = 1
                }
            }
        };

        Assert.Equal(2, RetryHeaderInspector.GetNextRetryIndex(headers, OrderSchedule));
    }

    [Fact]
    public void Higher_death_stage_wins_over_a_duplicate_count()
    {
        // A duplicate retry copy can make count > 1 for the same stage. Stage
        // progression is based on which stages completed, not the event count.
        var headers = Death(
            Entry("orders.retry.5s", "expired", 2L));

        Assert.Equal(1, RetryHeaderInspector.GetNextRetryIndex(headers, OrderSchedule));
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(1, 1)]
    [InlineData(2, 2)]
    [InlineData(3, 3)]
    [InlineData(99, 3)]
    public void Retry_attempt_header_is_a_bounded_second_guard(int attempt, int expectedIndex)
    {
        var headers = new Dictionary<string, object?>
        {
            [RetryHeaderInspector.RetryAttemptHeader] = (long)attempt
        };

        Assert.Equal(expectedIndex,
            RetryHeaderInspector.GetNextRetryIndex(headers, OrderSchedule));
    }

    [Fact]
    public void The_furthest_of_x_death_and_retry_attempt_is_used()
    {
        var headers = Death(Entry("orders.retry.5s", "expired", 1L));
        headers[RetryHeaderInspector.RetryAttemptHeader] = 2L;

        Assert.Equal(2, RetryHeaderInspector.GetNextRetryIndex(headers, OrderSchedule));
    }

    [Fact]
    public void Malformed_x_death_values_do_not_advance_the_stage()
    {
        var headers = new Dictionary<string, object?>
        {
            [RetryHeaderInspector.DeathHeader] = "not-an-array"
        };

        Assert.Equal(0, RetryHeaderInspector.GetNextRetryIndex(headers, OrderSchedule));

        headers[RetryHeaderInspector.DeathHeader] = new List<object>
        {
            "not-a-table",
            new Dictionary<string, object> { ["queue"] = 123, ["reason"] = "expired" },
            new Dictionary<string, object> { ["queue"] = "orders.retry.5s" }
        };

        Assert.Equal(0, RetryHeaderInspector.GetNextRetryIndex(headers, OrderSchedule));
    }

    private static void AssertStage(RetryQueue queue, string name, TimeSpan delay)
    {
        Assert.Equal(name, queue.QueueName);
        Assert.Equal(delay, queue.Delay);
    }

    private static Dictionary<string, object?> Death(params object[] entries) =>
        new() { [RetryHeaderInspector.DeathHeader] = entries.ToList() };

    private static Dictionary<string, object> Entry(string queue, string reason, long count) =>
        new()
        {
            ["queue"] = queue,
            ["reason"] = reason,
            ["count"] = count
        };
}
