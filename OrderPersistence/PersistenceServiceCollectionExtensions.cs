using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace OrderPersistence;

public static class PersistenceServiceCollectionExtensions
{
    public static IServiceCollection AddOrderPersistence(this IServiceCollection services,
        IConfiguration configuration)
    {
        services.AddDbContextFactory<OrderDbContext>((provider, options) =>
        {
            var config = provider.GetRequiredService<IConfiguration>();
            var dataSource = DatabasePath.Resolve(config["Database:Path"]);
            options.UseSqlite(new SqliteConnectionStringBuilder
            {
                DataSource = dataSource,
                DefaultTimeout = 5,
                Pooling = true
            }.ToString());
        });
        services.AddSingleton<IOrderStore, EfOrderStore>();
        return services;
    }

    public static async Task InitializeOrderDatabaseAsync(this IServiceProvider services,
        CancellationToken cancellationToken = default)
    {
        var factory = services.GetRequiredService<IDbContextFactory<OrderDbContext>>();
        await using var db = await factory.CreateDbContextAsync(cancellationToken);
        await OrderDatabase.InitializeAsync(db, cancellationToken);
    }
}
