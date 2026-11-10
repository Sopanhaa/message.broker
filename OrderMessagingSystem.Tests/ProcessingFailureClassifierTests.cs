using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using OrderConsumer.Worker;

namespace OrderMessagingSystem.Tests;

public sealed class ProcessingFailureClassifierTests
{
    [Theory]
    [InlineData(5)]  // SQLITE_BUSY
    [InlineData(6)]  // SQLITE_LOCKED
    public void Sqlite_busy_and_locked_are_transient(int errorCode)
    {
        Assert.True(ProcessingFailureClassifier.IsTransient(
            new SqliteException("database is locked", errorCode)));
    }

    [Theory]
    [InlineData(1)]   // SQLITE_ERROR
    [InlineData(10)]  // SQLITE_IOERR
    [InlineData(14)]  // SQLITE_CANTOPEN
    [InlineData(19)]  // SQLITE_CONSTRAINT
    [InlineData(26)]  // SQLITE_NOTADB
    public void Other_sqlite_errors_are_not_treated_as_transient(int errorCode)
    {
        Assert.False(ProcessingFailureClassifier.IsTransient(
            new SqliteException("sqlite failure", errorCode)));
    }

    [Fact]
    public void Io_and_timeout_failures_are_transient()
    {
        Assert.True(ProcessingFailureClassifier.IsTransient(new IOException("disk unavailable")));
        Assert.True(ProcessingFailureClassifier.IsTransient(new TimeoutException("database timeout")));
    }

    [Fact]
    public void Wrapped_transient_sqlite_failure_is_detected()
    {
        var wrapped = new DbUpdateException("save failed",
            new SqliteException("database is locked", 5));

        Assert.True(ProcessingFailureClassifier.IsTransient(wrapped));
    }

    [Fact]
    public void Wrapped_permanent_sqlite_failure_is_not_transient()
    {
        var wrapped = new DbUpdateException("save failed",
            new SqliteException("constraint failed", 19));

        Assert.False(ProcessingFailureClassifier.IsTransient(wrapped));
    }

    [Fact]
    public void Aggregate_with_a_transient_failure_is_transient()
    {
        var aggregate = new AggregateException(
            new InvalidOperationException("business failure"),
            new TimeoutException("database timeout"));

        Assert.True(ProcessingFailureClassifier.IsTransient(aggregate));
    }

    [Fact]
    public void Unexpected_and_null_exceptions_are_not_transient()
    {
        Assert.False(ProcessingFailureClassifier.IsTransient(null));
        Assert.False(ProcessingFailureClassifier.IsTransient(
            new InvalidOperationException("unexpected")));
    }
}
