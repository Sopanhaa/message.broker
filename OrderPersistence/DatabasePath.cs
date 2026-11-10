namespace OrderPersistence;

// The API and the worker are separate processes, so both resolve the same default
// location for the shared SQLite file. Set Database:Path to override it.
public static class DatabasePath
{
    public static string Resolve(string? configuredPath)
    {
        var path = string.IsNullOrWhiteSpace(configuredPath) ? "orders.db" : configuredPath.Trim();

        if (path.StartsWith("~/", StringComparison.Ordinal))
            path = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), path[2..]);

        if (!Path.IsPathRooted(path))
            path = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                "OrderMessagingSystem", path);

        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory)) Directory.CreateDirectory(directory);

        return path;
    }
}
