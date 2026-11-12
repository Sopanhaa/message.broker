using OrderPersistence;
using OrderProducer.Api.Models;
using OrderProducer.Api.Services;
using Shared.Contracts.Events;
using Shared.Contracts.Messaging;

var builder = WebApplication.CreateBuilder(args);
var rabbitMq = builder.Configuration.GetSection("RabbitMq").Get<RabbitMqSettings>()
    ?? throw new InvalidOperationException("RabbitMq configuration is missing.");
rabbitMq.Validate();
builder.Services.AddSingleton(rabbitMq);
builder.Services.AddSingleton<IMessagePublisher, RabbitMqPublisher>();
builder.Services.AddOrderPersistence(builder.Configuration);

var app = builder.Build();
await app.Services.InitializeOrderDatabaseAsync();

app.MapGet("/", () => Results.Ok(new { service = "OrderProducer.Api", status = "running" }));
// Liveness only: this intentionally does not claim that RabbitMQ is reachable.
app.MapGet("/health", () => Results.Ok(new { status = "healthy", check = "liveness" }));
app.MapGet("/orders/{orderId:guid}",
    async (Guid orderId, IOrderStore store, CancellationToken cancellationToken) =>
    {
        var order = await store.GetAsync(orderId, cancellationToken);
        return order is null ? Results.NotFound() : Results.Ok(order);
    });
app.MapPost("/orders", async (CreateOrderRequest request, IMessagePublisher publisher,
    IOrderStore store, ILogger<Program> logger, CancellationToken cancellationToken) =>
{
    var errors = request.Validate();
    if (errors.Count > 0) return Results.ValidationProblem(errors);

    var order = new OrderCreated(Guid.NewGuid(), request.CustomerName!.Trim(),
        request.TotalAmount, DateTimeOffset.UtcNow,
        request.Products?.Select(product => product with { Name = product.Name.Trim() }).ToArray());
    if (!await store.TryCreateAsync(order, cancellationToken))
        return Results.Conflict(new { message = "An order with this ID already exists." });

    try
    {
        await publisher.PublishAsync(order, cancellationToken);
        return Results.Accepted(value: new { order.OrderId, status = "accepted", order.CreatedAtUtc });
    }
    catch (MessagePublishException ex)
    {
        logger.LogError(ex, "Publishing order {OrderId} failed", order.OrderId);
        // The client is told the order was not accepted, so do not leave it behind.
        // CancellationToken.None keeps the rollback running if the request was aborted.
        if (!await store.TryDeleteAsync(order.OrderId, CancellationToken.None))
            logger.LogWarning("Could not roll back order {OrderId} after a publish failure", order.OrderId);
        return Results.Problem(statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "Order could not be confirmed by RabbitMQ",
            detail: "Check RabbitMQ and try again. A timeout can mean the outcome is unknown; retrying can create duplicates.");
    }
});
app.MapPatch("/orders/{orderId:guid}/status",
    async (Guid orderId, UpdateOrderStatusRequest request, IMessagePublisher publisher,
        IOrderStore store, ILogger<Program> logger, CancellationToken cancellationToken) =>
    {
        var errors = request.Validate();
        if (orderId == Guid.Empty)
            errors[nameof(orderId)] = ["Order ID must not be empty."];
        if (errors.Count > 0) return Results.ValidationProblem(errors);

        if (await store.GetAsync(orderId, cancellationToken) is null)
            return Results.NotFound(new { message = $"Order {orderId} was not found." });

        var detail = string.IsNullOrWhiteSpace(request.Detail) ? null : request.Detail.Trim();
        var status = new OrderStatusChanged(orderId, request.Status!.Value, DateTimeOffset.UtcNow, detail);
        try
        {
            await publisher.PublishStatusAsync(status, cancellationToken);
            return Results.Accepted(value: new { status.OrderId, status.Status, status.UpdatedAtUtc });
        }
        catch (MessagePublishException ex)
        {
            logger.LogError(ex, "Publishing status update for order {OrderId} failed", orderId);
            return Results.Problem(statusCode: StatusCodes.Status503ServiceUnavailable,
                title: "Order status could not be confirmed by RabbitMQ",
                detail: "Check RabbitMQ and try again. A timeout can mean the outcome is unknown; retrying can create duplicates.");
        }
    });
app.MapPost("/routing/fanout", async (IMessagePublisher publisher, ILogger<Program> logger,
    CancellationToken cancellationToken) =>
{
    var routingEvent = new OrderRoutingEvent(Guid.NewGuid(), OrderRoutingEventTypes.Created,
        DateTimeOffset.UtcNow, "Fanout routing demo");
    try
    {
        await publisher.PublishFanoutAsync(routingEvent, cancellationToken);
        return Results.Accepted(value: new
        {
            routingEvent.EventId,
            exchange = RoutingTopology.FanoutExchangeName(rabbitMq),
            routingEvent.EventType,
            routingEvent.OccurredAtUtc
        });
    }
    catch (MessagePublishException ex)
    {
        logger.LogError(ex, "Publishing fanout event {EventId} failed", routingEvent.EventId);
        return Results.Problem(statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "Fanout event could not be confirmed by RabbitMQ",
            detail: "Check RabbitMQ and try again.");
    }
});
app.MapPost("/routing/topic", async (PublishTopicEventRequest request, IMessagePublisher publisher,
    ILogger<Program> logger, CancellationToken cancellationToken) =>
{
    var errors = request.Validate();
    if (errors.Count > 0) return Results.ValidationProblem(errors);

    var routingKey = request.RoutingKey!.Trim();
    var eventType = request.EventType?.Trim() ?? OrderRoutingEventTypes.Created;
    var routingEvent = new OrderRoutingEvent(Guid.NewGuid(), eventType, DateTimeOffset.UtcNow);
    try
    {
        await publisher.PublishTopicAsync(routingEvent, routingKey, cancellationToken);
        return Results.Accepted(value: new
        {
            routingEvent.EventId,
            exchange = RoutingTopology.TopicExchangeName(rabbitMq),
            routingEvent.EventType,
            routingKey,
            routingEvent.OccurredAtUtc
        });
    }
    catch (MessagePublishException ex)
    {
        logger.LogError(ex, "Publishing topic event {EventId} with routing key {RoutingKey} failed",
            routingEvent.EventId, routingKey);
        return Results.Problem(statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "Topic event could not be confirmed by RabbitMQ",
            detail: "Check RabbitMQ and try again.");
    }
});
app.Run();

// Allows WebApplicationFactory to start this API in automated tests.
public partial class Program;
