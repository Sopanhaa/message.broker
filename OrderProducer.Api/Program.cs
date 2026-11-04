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

var app = builder.Build();
app.MapGet("/", () => Results.Ok(new { service = "OrderProducer.Api", status = "running" }));
// Liveness only: this intentionally does not claim that RabbitMQ is reachable.
app.MapGet("/health", () => Results.Ok(new { status = "healthy", check = "liveness" }));
app.MapPost("/orders", async (CreateOrderRequest request, IMessagePublisher publisher,
    ILogger<Program> logger, CancellationToken cancellationToken) =>
{
    var errors = request.Validate();
    if (errors.Count > 0) return Results.ValidationProblem(errors);

    var order = new OrderCreated(Guid.NewGuid(), request.CustomerName!.Trim(),
        request.TotalAmount, DateTimeOffset.UtcNow);
    try
    {
        await publisher.PublishAsync(order, cancellationToken);
        return Results.Accepted(value: new { order.OrderId, status = "accepted", order.CreatedAtUtc });
    }
    catch (MessagePublishException ex)
    {
        logger.LogError(ex, "Publishing order {OrderId} failed", order.OrderId);
        return Results.Problem(statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "Order could not be confirmed by RabbitMQ",
            detail: "Check RabbitMQ and try again. A timeout can mean the outcome is unknown; retrying can create duplicates.");
    }
});
app.Run();

// Allows WebApplicationFactory to start this API in automated tests.
public partial class Program;
