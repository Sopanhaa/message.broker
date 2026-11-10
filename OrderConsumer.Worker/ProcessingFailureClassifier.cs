using Microsoft.Data.Sqlite;

namespace OrderConsumer.Worker;

// Classifies exceptions without treating every database error as retryable.
// Only explicitly recoverable infrastructure failures are retried. Everything
// else is routed to the DLQ by the handler so it remains visible for inspection.
public static class ProcessingFailureClassifier
{
    // SQLITE_BUSY and SQLITE_LOCKED. Their extended codes (for example
    // SQLITE_BUSY_SNAPSHOT) share the same primary code.
    private const int SqliteBusy = 5;
    private const int SqliteLocked = 6;

    public static bool IsTransient(Exception? exception)
    {
        if (exception is null) return false;

        // Walk the exception chain because EF Core wraps SQLite errors in
        // DbUpdateException (and aggregate exceptions can wrap several errors).
        var seen = new HashSet<Exception>(ReferenceEqualityComparer.Instance);
        for (var current = exception; current is not null && seen.Add(current);
             current = current.InnerException)
        {
            switch (current)
            {
                case TimeoutException:
                case IOException:
                    return true;
                case SqliteException sqlite when IsRecoverable(sqlite):
                    return true;
                case AggregateException aggregate when aggregate.InnerExceptions.Any(IsTransient):
                    return true;
            }
        }

        return false;
    }

    private static bool IsRecoverable(SqliteException exception) =>
        exception.SqliteErrorCode is SqliteBusy or SqliteLocked;
}
