using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

namespace OrderPersistence;

public static class OrderDatabase
{
    public static async Task InitializeAsync(OrderDbContext db, CancellationToken cancellationToken = default)
    {
        // The API and the worker can start at the same time and both call EnsureCreated.
        // Retry briefly if the other process creates the tables first.
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await db.Database.EnsureCreatedAsync(cancellationToken);
                // WAL lets one process read while the other writes the shared file.
                await db.Database.ExecuteSqlRawAsync("PRAGMA journal_mode=WAL;", cancellationToken);
                return;
            }
            catch (SqliteException ex) when (attempt < 5 && ex.SqliteErrorCode is 1 or 5 or 19)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(200 * attempt), cancellationToken);
            }
        }
    }
}
