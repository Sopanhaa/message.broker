using Microsoft.EntityFrameworkCore;
using OrderPersistence.Entities;
using Shared.Contracts.Events;

namespace OrderPersistence;

public sealed class OrderDbContext(DbContextOptions<OrderDbContext> options) : DbContext(options)
{
    public DbSet<OrderEntity> Orders => Set<OrderEntity>();
    public DbSet<ProcessedMessageEntity> ProcessedMessages => Set<ProcessedMessageEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        var order = modelBuilder.Entity<OrderEntity>();
        order.ToTable("Orders");
        order.HasKey(o => o.OrderId);
        order.Property(o => o.CustomerName).IsRequired().HasMaxLength(100);
        // SQLite has no decimal type. Store the exact value as text; it is never compared.
        order.Property(o => o.TotalAmount).HasColumnType("TEXT");
        order.Property(o => o.Status).HasConversion<string>().HasMaxLength(20);
        order.Property(o => o.StatusDetail).HasMaxLength(OrderStatusChanged.MaxDetailLength);
        order.Property(o => o.ProductsJson).HasColumnType("TEXT");

        var processed = modelBuilder.Entity<ProcessedMessageEntity>();
        processed.ToTable("ProcessedMessages");
        processed.HasKey(m => m.MessageId);
        processed.Property(m => m.MessageId).HasMaxLength(100);
        processed.Property(m => m.MessageType).IsRequired().HasMaxLength(100);
        processed.Property(m => m.ProcessedAtUtc).HasColumnType("TEXT");
        processed.HasIndex(m => m.OrderId);
    }
}
