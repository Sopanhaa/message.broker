using OrderConsumer.Worker;
using OrderPersistence;
using Shared.Contracts.Messaging;

var builder = Host.CreateApplicationBuilder(args);
var rabbitMq = builder.Configuration.GetSection("RabbitMq").Get<RabbitMqSettings>()
    ?? throw new InvalidOperationException("RabbitMq configuration is missing.");
rabbitMq.Validate();
builder.Services.AddSingleton<RabbitMqSettings>(rabbitMq);
builder.Services.AddOrderPersistence(builder.Configuration);
builder.Services.AddSingleton<IOrderProcessor, OrderProcessor>();
builder.Services.AddSingleton<IOrderStatusProcessor, OrderStatusProcessor>();
builder.Services.AddSingleton<RetryScheduler>();
builder.Services.AddSingleton<OrderMessageHandler>();
builder.Services.AddSingleton<OrderStatusMessageHandler>();
builder.Services.AddSingleton<RoutingMessageHandler>();
builder.Services.Configure<HostOptions>(options => options.ShutdownTimeout = TimeSpan.FromSeconds(15));
builder.Services.AddHostedService<Worker>();
builder.Services.AddHostedService<RoutingDemoWorker>();
var host = builder.Build();
await host.Services.InitializeOrderDatabaseAsync();
await host.RunAsync();
