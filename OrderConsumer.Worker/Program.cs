using OrderConsumer.Worker;
using Shared.Contracts.Messaging;

var builder = Host.CreateApplicationBuilder(args);
var rabbitMq = builder.Configuration.GetSection("RabbitMq").Get<RabbitMqSettings>()
    ?? throw new InvalidOperationException("RabbitMq configuration is missing.");
rabbitMq.Validate();
builder.Services.AddSingleton(rabbitMq);
builder.Services.Configure<HostOptions>(options => options.ShutdownTimeout = TimeSpan.FromSeconds(15));
builder.Services.AddHostedService<Worker>();
await builder.Build().RunAsync();
